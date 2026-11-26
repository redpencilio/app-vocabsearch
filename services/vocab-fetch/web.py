import os
from datetime import datetime
from string import Template
import threading

import requests
from escape_helpers import sparql_escape, sparql_escape_uri
from flask import request
from helpers import generate_uuid, logger, query
from sudo_query import update_sudo

from rdflib import Graph, URIRef, Literal
from void import generateVoID, deleteVoID

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from file import file_to_shared_uri, shared_uri_to_path
from file import construct_get_file_query, construct_insert_file_query
from task import find_actionable_task_of_type, run_task, find_actionable_task, create_download_task
from dataset import get_dataset, update_dataset_download, get_dataset_by_uuid
from ldes_dump import query_all_ldes_datasets, query_outdated_dump_ldes_datasets
from sparql_util import binding_results, serialize_graph_to_sparql, graph_to_file, MU_VIRTUOSO_ENDPOINT
from format_to_mime import FORMAT_TO_MIME_EXT

def drop_graph_virtuoso(graph_uri):
    """Drop graph using direct virtuoso endpoint"""
    import requests
    query = f"DROP SILENT GRAPH {sparql_escape_uri(graph_uri)}"
    logger.debug(f"Dropping graph via virtuoso: {query}")
    response = requests.post(MU_VIRTUOSO_ENDPOINT, data={'update': query})
    response.raise_for_status()
    return response

# Maybe make these configurable
TASKS_GRAPH = "http://mu.semte.ch/graphs/public"
FILES_GRAPH = "http://mu.semte.ch/graphs/public"
VOID_DATASET_GRAPH = "http://mu.semte.ch/graphs/public"

FILE_RESOURCE_BASE = "http://example-resource.com/"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
VOID_DATASET_RESOURCE_BASE = "http://example-resource.com/void-dataset/"

VOCAB_DOWNLOAD_JOB = "http://lblod.data.gift/id/jobs/concept/JobOperation/vocab-download"
LDES_TYPE = "http://vocabsearch.data.gift/dataset-types/LDES"

UPDATE_DATASET_DUMP_CRON_PATTERN = os.environ.get("UPDATE_DATASET_DUMP_CRON_PATTERN")


def load_dataset_file(uri: str, graph: str = MU_APPLICATION_GRAPH):
    query_string = construct_get_file_query(uri, graph)
    file_result = query(query_string, sudo=True)["results"]["bindings"][0]

    g = Graph()
    g.parse(shared_uri_to_path(file_result["physicalFile"]["value"]))

    return g


def download_dataset_file(url: str, format: str, graph: str = MU_APPLICATION_GRAPH):
    mime_type, file_extension = FORMAT_TO_MIME_EXT[format]
    accept_string = ", ".join(
        value[0] + (";q=1.0" if key == format else ";q=0.1")
        for key, value in FORMAT_TO_MIME_EXT.items()
    )
    upload_resource_uuid = generate_uuid()
    upload_resource_uri = f"{FILE_RESOURCE_BASE}{upload_resource_uuid}"
    file_resource_uuid = generate_uuid()
    file_resource_name = f"{file_resource_uuid}.{file_extension}"

    file_resource_uri = file_to_shared_uri(file_resource_name)

    headers = {"Accept": accept_string}

    with requests.get(url, headers=headers, stream=True) as res:
        if res.url != url:
            logger.info("You've been redirected. Probably want to replace url in db.")

        assert res.ok

        # TODO: better handling + negociating
        logger.info(f'Content-Type: {res.headers["Content-Type"]}')
        logger.info(f"MIME-Type: {mime_type}")

        with open(shared_uri_to_path(file_resource_uri), "wb") as f:
            for chunk in res.iter_content(chunk_size=None):
                f.write(chunk)

            f.seek(0, 2)
            file_size = f.tell()

    file = {
        "uri": upload_resource_uri,
        "uuid": upload_resource_uuid,
        "name": file_resource_name,
        "mimetype": "text/plain",
        "created": datetime.now(),
        "size": file_size,
        "extension": file_extension,
    }
    physical_file = {
        "uri": file_resource_uri,
        "uuid": file_resource_uuid,
        "name": file_resource_name,
    }

    query_string = construct_insert_file_query(file, physical_file, graph)

    # TODO Check query result before writing file to disk
    update_sudo(query_string)

    return upload_resource_uri


def escape(binding):
    if binding["type"] == "uri":
        return URIRef(binding["value"])
    elif binding["type"] == "typed-literal":
        return Literal(binding["value"], datatype=binding["datatype"])
    else:
        return Literal(binding["value"])

def get_task_uri(task_uuid: str, graph: str = MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT DISTINCT ?task_uri WHERE {
    GRAPH $graph {
        ?task_uri task:operation <http://mu.semte.ch/vocabularies/ext/dataset-download-task>;
             mu:uuid $task_uuid .
    }
}
""")

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        task_uuid=sparql_escape(task_uuid),
    )
    query_res = query(query_string)
    return query_res["results"]["bindings"][0]["task_uri"]["value"]

def redownload_dataset(dataset_uri):
    dataset_result = query(get_dataset(dataset_uri, VOID_DATASET_GRAPH), sudo=True)['results']['bindings'][0]
    _type = dataset_result['type']['value']
    if _type == LDES_TYPE:
        # graph = dataset_result['dataset_graph']['value']
        # file_uri = graph_to_file(graph, FILES_GRAPH)
        # for LDES the data is automatically reloaded. Dumping to file takes too long. Instead use the LDES-data graph.
        # This means the download job for LDES will automatically succeed.
        # TODO: This should maybe fire up a recheck in the LDES-consumer or check if it is still running?
        # For now, do a check if the LDES-graph is present. If not, fail the job (as that means a problem with the consumer).
        dataset_graph = dataset_result['dataset_graph']['value']
        if not dataset_graph:
            raise ValueError(f"LDES-graph not found for dataset {dataset_uri}")
    else:
        download_link = dataset_result['download_url']['value']
        file_format = dataset_result['format']['value']
        file_uri = download_dataset_file(download_link, file_format, FILES_GRAPH)
        update_sudo(update_dataset_download(dataset_uri, file_uri, VOID_DATASET_GRAPH))
    return dataset_uri


@app.route("/dataset-download-task/<task_uuid>/run", methods=["POST"])
def run_dataset_download_route(task_uuid: str):
    try:
        task_uri = get_task_uri(task_uuid)
    except Exception:
        logger.info(f"No job found by uuid ${task_uuid}")
        return

    run_task(
        task_uri,
        TASKS_GRAPH,
        lambda sources: [redownload_dataset(sources[0])],
        lambda query_string: query(query_string, sudo=True),
        update_sudo,
    )

    return ""



def generate_dataset_structural_metadata(dataset_uri, should_return_vocab):
    try:
        logger.info(f"Starting metadata generation for dataset: {dataset_uri}")
        dataset_res = query(get_dataset(dataset_uri, VOID_DATASET_GRAPH), sudo=True)["results"][
            "bindings"
        ][0]

        dataset_type = dataset_res.get("type", {}).get("value")
        dataset_graph = dataset_res.get("dataset_graph", {}).get("value")
        data_dump = dataset_res.get("data_dump", {}).get("value")

        logger.info(f"dataset <{dataset_uri}> with type: {dataset_type} | Dataset graph: {dataset_graph} | Data dump: {data_dump}")

        # We update dataset metadata by purging old and re-loadind new data (instead of applying the diff)
        # This has some unexpected consequences, since ldes consumer-manager triggers on addition and
        # removal of void:Datasets. Since our actual intention isn't to remove the whole object (including type), but
        # rather to update more detailed properties, we exclude the `rdf:type` predicate from the removal process.
        logger.info(f"Deleting existing VoID metadata for <{dataset_uri}>")
        deleteVoID(dataset_uri, VOID_DATASET_GRAPH)
        if dataset_graph:
            # Graph-based dataset (from LDES-consumer)
            logger.info(f"Processing graph-based dataset <{dataset_uri}> with graph: {dataset_graph}")
            generateVoID(dataset_graph, dataset_uri, VOID_DATASET_GRAPH)

        elif data_dump:
            # File-based dataset: load file into temporary graph for analysis
            temp_graph_uri = f"http://temp.vocabsearch.com/graph/{generate_uuid()}"
            logger.info(f"Processing file-based dataset <{dataset_uri}> with data dump: {data_dump} | Loading into temporary graph: {temp_graph_uri}")

            try:
                # Load file content into temporary graph in triplestore
                file_graph = load_dataset_file(data_dump, FILES_GRAPH)
                logger.info(f"dataset <{dataset_uri}>: Loaded {len(file_graph)} triples from file to local graph")

                # Serialize to temporary graph in triplestore
                for query_string in serialize_graph_to_sparql(file_graph, temp_graph_uri):
                    update_sudo(query_string)

                logger.info(f"dataset <{dataset_uri}>: File data loaded into temporary graph <{temp_graph_uri}> in triplestore, generating VoID metadata")
                # Generate VoID from temporary graph
                generateVoID(temp_graph_uri, dataset_uri, VOID_DATASET_GRAPH)

            finally:
                # Clean up temporary graph (unification will reload from file when needed)
                try:
                    drop_graph_virtuoso(temp_graph_uri)
                    logger.info(f"dataset <{dataset_uri}>: Cleaned up temporary graph: {temp_graph_uri}")
                except Exception as cleanup_error:
                    logger.warning(f"dataset <{dataset_uri}>: Failed to cleanup temporary graph {temp_graph_uri}: {cleanup_error}")
        else:
            # Neither graph nor file dump available
            raise ValueError(f"Dataset {dataset_uri} has neither dataset_graph nor data_dump. Cannot generate metadata.")

        if should_return_vocab:
            vocab_uri = dataset_res.get("vocab", {}).get("value")
            logger.info(f"Returning vocab URI: {vocab_uri}")
            return vocab_uri
        else:
            logger.info(f"Returning dataset URI: {dataset_uri}")
            return dataset_uri
    except Exception as e:
        logger.error(f"Error in generate_dataset_structural_metadata for {dataset_uri}: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        raise


VOCAB_DOWNLOAD_OPERATION = "http://mu.semte.ch/vocabularies/ext/VocabDownloadJob"
METADATA_EXTRACTION_OPERATION = (
    "http://mu.semte.ch/vocabularies/ext/MetadataExtractionJob"
)

running_tasks_lock = threading.Lock()


def run_tasks():
    # run this function only once at a time to avoid overloading the service
    acquired = running_tasks_lock.acquire(blocking=False)

    if not acquired:
        logger.debug("Already running `run_tasks`")
        return

    try:
        while True:
            task_q = find_actionable_task_of_type(
                [VOCAB_DOWNLOAD_OPERATION, METADATA_EXTRACTION_OPERATION], TASKS_GRAPH
            )
            task_res = query(task_q, sudo=True)
            if task_res["results"]["bindings"]:
                (task_uri, task_operation, job_operation) = binding_results(
                    task_res, ("uri", "operation", "job_operation")
                )[0]
            else:
                logger.debug("No more tasks found")
                return
            try:
                if task_operation == VOCAB_DOWNLOAD_OPERATION:
                    logger.debug(f"Running task {task_uri}, operation {task_operation}")
                    run_task(
                        task_uri,
                        TASKS_GRAPH,
                        lambda sources: [redownload_dataset(sources[0])],
                        lambda query_string: query(query_string, sudo=True),
                        update_sudo,
                    )

                elif task_operation == METADATA_EXTRACTION_OPERATION:
                    logger.debug(f"Running task {task_uri}, operation {task_operation}")
                    # this task is part of a job that will run unification afterwards
                    # which needs vocab uri as the output of previous job
                    # Maybe better to create an extra task "metadata_extraction_before_unification"
                    # which can be used in the VOCAB_DOWNLOAD job
                    should_return_vocab = (job_operation == VOCAB_DOWNLOAD_JOB)
                    run_task(
                        task_uri,
                        TASKS_GRAPH,
                        lambda sources: [
                            generate_dataset_structural_metadata(sources[0], should_return_vocab)
                        ],
                        lambda query_string: query(query_string, sudo=True),
                        update_sudo,
                    )
            except:
                logger.warn(
                    f"Problem while running task {task_uri}, operation {task_operation}"
                )
    finally:
        running_tasks_lock.release()

def update_ldes_dataset_dump():
    logger.info("querying for LDES datasets with updates (requiring an update of their file-dump)")
    # TODO: LDES have no dumps anymore. This should maybe check the last unification job instead.
    # for now just create download task for every LDES dataset
    # datasets = query_outdated_dump_ldes_datasets()
    datasets = query_all_ldes_datasets()
    if datasets:
        logger.info("LDES datasets needing dump update: " + str(datasets))
    else:
        logger.info("No LDES datasets requiring a dump update found.")
    for dataset in datasets:
        logger.info(f"Creating dump task for dataset {dataset}")
        qs = create_download_task(dataset, TASKS_GRAPH)
        update_sudo(qs)

@app.route("/run-update-ldes-dataset-check", methods=["POST"])
def run_update_ldes_dataset_check():
    update_ldes_dataset_dump()
    return "", 200

if UPDATE_DATASET_DUMP_CRON_PATTERN:
    scheduler = BackgroundScheduler()
    logger.info(f"Configuring dataset dump update cron interval {UPDATE_DATASET_DUMP_CRON_PATTERN}")
    scheduler.add_job(update_ldes_dataset_dump, CronTrigger.from_crontab(UPDATE_DATASET_DUMP_CRON_PATTERN))
    logger.info(f"Starting Cron scheduler")
    scheduler.start()

@app.route("/delta", methods=["POST"])
def process_delta():
    inserts = request.json[0]["inserts"]
    task_triples = [
        t
        for t in inserts
        if t["predicate"]["value"] == "http://www.w3.org/ns/adms#status"
        and t["object"]["value"]
        == "http://redpencil.data.gift/id/concept/JobStatus/scheduled"
    ]
    if not task_triples:
        return "Can't do anything with this delta. Skipping.", 500

    thread = threading.Thread(target=run_tasks)
    thread.start()

    return "", 200
