import os
from string import Template
import threading

from rdflib import Graph, URIRef
import requests
from more_itertools import batched

from flask import request
from app import app

from escape_helpers import sparql_escape, sparql_escape_uri
from helpers import generate_uuid, logger
from helpers import query as sparql_query
from helpers import update as sparql_update
from sudo_query import query_sudo, auth_update_sudo as update_sudo

from sparql_util import (
    binding_results,
    serialize_graph_to_sparql,
    sparql_construct_res_to_graph,
    load_file_to_db,
    drop_graph,
    diff_graphs,
    copy_graph_to_temp,
)

from task import find_actionable_task_of_type, find_same_scheduled_tasks, get_input_contents_task, run_task, find_actionable_task, run_tasks, create_vocab_deletion_task, VOCAB_DELETION_OPERATION
from vocabulary import get_vocabulary
from dataset import get_dataset

from unification import (
    get_property_paths,
    get_ununified_batch,
    delete_dataset_subjects_from_graph,
)
from remove_vocab import (
    remove_files,
    select_vocab_concepts_batch,
    remove_vocab_data_dumps,
    remove_vocab_source_datasets,
    remove_vocab_meta,
    remove_vocab_vocab_fetch_jobs,
    remove_vocab_vocab_unification_jobs,
    remove_vocab_partitions,
    remove_vocab_mapping_shape,
    remove_vocab_ldes_containers,
    get_vocab_ldes_containers,
)

# Maybe make these configurable
FILE_RESOURCE_BASE = "http://example-resource.com/"
TASKS_GRAPH = "http://mu.semte.ch/graphs/public"
TEMP_GRAPH_BASE = "http://example-resource.com/graph/"
VOCAB_GRAPH = "http://mu.semte.ch/graphs/public"
UNIFICATION_TARGET_GRAPH = "http://mu.semte.ch/graphs/public"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
TEMP_GRAPH_BASE = "http://example-resource.com/graph/"

CONT_UN_OPERATION = "http://mu.semte.ch/vocabularies/ext/ContentUnificationJob"


def run_vocab_unification(vocab_uri):
    vocab_sources = query_sudo(get_vocabulary(vocab_uri, VOCAB_GRAPH))["results"][
        "bindings"
    ]

    if not vocab_sources:
        raise Exception(f"Vocab {vocab_uri} does not have a mapping. Unification cancelled.")

    temp_named_graph = TEMP_GRAPH_BASE + generate_uuid()
    for vocab_source in vocab_sources:
        dataset_versions = query_sudo(
            get_dataset(vocab_source["sourceDataset"]["value"], VOCAB_GRAPH)
        )["results"]["bindings"]
        print(dataset_versions)
        # TODO: LDES check
        if "data_dump" in dataset_versions[0].keys():
            temp_named_graph = load_file_to_db(
                dataset_versions[0]["data_dump"]["value"], VOCAB_GRAPH, temp_named_graph
            )
            if len(dataset_versions) > 1:  # previous dumps exist
                old_temp_named_graph = load_file_to_db(
                    dataset_versions[1]["data_dump"]["value"], VOCAB_GRAPH
                )
                # diffing now happens in triplestore. If we make sure everything gets stored
                # as sorted ntriples files, this can be done on file basis. Would improve perf
                # and avoid having to load everything to triplestore with python rdflib store
                # as an intermediary (!)
                diff_subjects = diff_graphs(old_temp_named_graph, temp_named_graph)
                for diff_subjects_batch in batched(diff_subjects, 10):
                    query_sudo(delete_dataset_subjects_from_graph(diff_subjects_batch, VOCAB_GRAPH))
                drop_graph(old_temp_named_graph)
        else:
            # since we now also save ldes datasets to files, ldes datasets can also get
            # cleanups that are made possible by diffing above.
            # This should become a dead code path
            # keep until we can assure that an ldes dataset always has a dump (still needs cron to trigger the dump download)
            copy_graph_to_temp(
                dataset_versions[0]["dataset_graph"]["value"], temp_named_graph
            )
    prop_paths_qs = get_property_paths(
        vocab_sources[0]["mappingShape"]["value"], VOCAB_GRAPH
    )
    prop_paths_res = query_sudo(prop_paths_qs)

    for path_props in prop_paths_res["results"]["bindings"]:
        while True:
            get_batch_qs = get_ununified_batch(
                path_props["destClass"]["value"],
                path_props["destPath"]["value"],
                [
                    vocab_source["sourceDataset"]["value"]
                    for vocab_source in vocab_sources
                ],
                path_props["sourceClass"]["value"],
                path_props["sourcePathString"]["value"],  # !
                temp_named_graph,
                VOCAB_GRAPH,
                10,
            )
            # We might want to dump intermediary unified content to file before committing to store
            batch_res = query_sudo(get_batch_qs)
            if not batch_res["results"]["bindings"]:
                logger.info("Finished unification")
                break
            else:
                logger.info("Running unification batch")
            g = sparql_construct_res_to_graph(batch_res)
            for query_string in serialize_graph_to_sparql(g, VOCAB_GRAPH):
                update_sudo(query_string)

    drop_graph(temp_named_graph)
    return vocab_uri


@app.route("/delete-vocabulary/<vocab_uuid>", methods=("DELETE",))
def delete_vocabulary(vocab_uuid: str):
    try:
        task_query, task_uri = create_vocab_deletion_task(vocab_uuid, VOCAB_GRAPH)
        update_sudo(task_query)
        
        # Start deletion process in background
        thread = threading.Thread(target=run_scheduled_tasks)
        thread.start()
        
        # Return task URI for status tracking
        task_id = task_uri.split("/")[-1]
        return {"message": "Vocabulary deletion started", "task_id": task_id, "status": "scheduled"}, 202
    except Exception as e:
        logger.error(f"Failed to create deletion task for vocab {vocab_uuid}: {e}")
        return {"error": "Failed to start deletion process"}, 500


@app.route("/deletion-status/<task_id>", methods=("GET",))
def get_deletion_status(task_id: str):
    """Get the status of a vocabulary deletion task"""
    try:
        task_uri = f"http://redpencil.data.gift/id/task/{task_id}"
        
        query_template = Template("""
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT ?status ?created ?modified ?vocab_uuid WHERE {
    GRAPH $graph {
        $task_uri a task:Task ;
            adms:status ?status ;
            dct:created ?created ;
            task:inputContainer/ext:content ?vocab_uri .
        OPTIONAL { $task_uri dct:modified ?modified }
    }
    BIND(STRAFTER(STR(?vocab_uri), "/") AS ?vocab_uuid)
}""")
        
        query_string = query_template.substitute(
            graph=sparql_escape_uri(VOCAB_GRAPH),
            task_uri=sparql_escape_uri(task_uri)
        )
        
        result = query_sudo(query_string)
        bindings = result["results"]["bindings"]
        
        if not bindings:
            return {"error": "Task not found"}, 404
        
        binding = bindings[0]
        status_uri = binding["status"]["value"]
        status = status_uri.split("/")[-1]  # Extract status name from URI
        
        response = {
            "task_id": task_id,
            "status": status,
            "created": binding["created"]["value"],
            "vocab_uuid": binding.get("vocab_uuid", {}).get("value", "unknown")
        }
        
        if "modified" in binding:
            response["modified"] = binding["modified"]["value"]
        
        return response, 200
        
    except Exception as e:
        logger.error(f"Failed to get deletion status for task {task_id}: {e}")
        return {"error": "Failed to get task status"}, 500


def execute_vocabulary_deletion(vocab_uuid: str):
    """Execute the actual vocabulary deletion steps"""
    try:
        logger.info(f"Starting deletion of vocabulary {vocab_uuid}")
        
        # Step 1: Remove physical files
        logger.info(f"Removing files for vocabulary {vocab_uuid}")
        remove_files(vocab_uuid, VOCAB_GRAPH)
        
        # Step 2: Remove data dumps
        logger.info(f"Removing data dumps for vocabulary {vocab_uuid}")
        update_sudo(remove_vocab_data_dumps(vocab_uuid, VOCAB_GRAPH))

        # Step 3: Remove concepts in batches
        logger.info(f"Removing concepts for vocabulary {vocab_uuid}")
        while True:
            batch = query_sudo(select_vocab_concepts_batch(vocab_uuid, VOCAB_GRAPH))
            bindings = batch["results"]["bindings"]
            if bindings:
                g = sparql_construct_res_to_graph(batch)
                for query_string in serialize_graph_to_sparql(g, VOCAB_GRAPH, "DELETE"):
                    update_sudo(query_string)
            else:
                break
        
        # Step 4: Check and remove LDES containers
        logger.info(f"Checking for LDES containers for vocabulary {vocab_uuid}")
        ldes_containers_res = query_sudo(get_vocab_ldes_containers(vocab_uuid, VOCAB_GRAPH))
        ldes_containers = [binding["datasetGraph"]["value"] for binding in ldes_containers_res["results"]["bindings"]]
        
        if ldes_containers:
            logger.info(f"Found {len(ldes_containers)} LDES containers to remove for vocabulary {vocab_uuid}")
            
            # The LDES consumer manager will automatically clean up Docker containers
            # when it receives delta notifications about dataset deletions.
            # Our deletion of source datasets will trigger this via the delta system.
            
            # Remove LDES container data from triplestore
            # This should trigger delta notifications that the consumer manager will pick up
            update_sudo(remove_vocab_ldes_containers(vocab_uuid, VOCAB_GRAPH))
            
            logger.info(f"LDES container cleanup will be handled automatically by consumer manager via deltas")
            
            # Log the containers that were processed
            for container in ldes_containers:
                logger.info(f"LDES container removed from triplestore: {container}")
        else:
            logger.info(f"No LDES containers found for vocabulary {vocab_uuid}")
        
        # Step 5: Remove jobs and metadata
        logger.info(f"Removing jobs and metadata for vocabulary {vocab_uuid}")
        update_sudo(remove_vocab_vocab_fetch_jobs(vocab_uuid, VOCAB_GRAPH))
        update_sudo(remove_vocab_vocab_unification_jobs(vocab_uuid, VOCAB_GRAPH))
        update_sudo(remove_vocab_partitions(vocab_uuid, VOCAB_GRAPH))
        update_sudo(remove_vocab_source_datasets(vocab_uuid, VOCAB_GRAPH))
        update_sudo(remove_vocab_mapping_shape(vocab_uuid, VOCAB_GRAPH))
        update_sudo(remove_vocab_meta(vocab_uuid, VOCAB_GRAPH))
        
        logger.info(f"Successfully deleted vocabulary {vocab_uuid}")
        return vocab_uuid
        
    except Exception as e:
        logger.error(f"Error during vocabulary deletion {vocab_uuid}: {e}")
        raise


def verify_search_index_cleanup(vocab_uuid: str, max_retries=5, retry_delay=2):
    """Verify that vocabulary concepts are removed from search index"""
    import time
    
    for attempt in range(max_retries):
        try:
            # Query search service for concepts from this vocabulary
            search_url = "http://search/concepts"
            params = {
                "filter[vocabulary]": f"http://example-resource.com/vocab/{vocab_uuid}",
                "page[size]": "1"
            }
            
            response = requests.get(search_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                concept_count = data.get("meta", {}).get("count", 0)
                
                if concept_count == 0:
                    logger.info(f"Search index cleanup verified for vocabulary {vocab_uuid}")
                    return True
                else:
                    logger.info(f"Search still contains {concept_count} concepts for vocabulary {vocab_uuid}, attempt {attempt + 1}/{max_retries}")
            else:
                logger.warning(f"Search query failed with status {response.status_code}, attempt {attempt + 1}/{max_retries}")
                
        except Exception as e:
            logger.warning(f"Search verification error: {e}, attempt {attempt + 1}/{max_retries}")
        
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
            retry_delay *= 1.5  # Exponential backoff
    
    logger.warning(f"Search index cleanup could not be verified for vocabulary {vocab_uuid} after {max_retries} attempts")
    return False


def execute_vocabulary_deletion_with_verification(vocab_uuid: str):
    """Execute vocabulary deletion and verify search index cleanup"""
    # Extract vocab_uuid from URI if needed
    if vocab_uuid.startswith("http://"):
        vocab_uuid = vocab_uuid.split("/")[-1]
    
    try:
        # Execute the deletion
        result = execute_vocabulary_deletion(vocab_uuid)
        
        # Verify search index cleanup
        logger.info(f"Verifying search index cleanup for vocabulary {vocab_uuid}")
        search_verified = verify_search_index_cleanup(vocab_uuid)
        
        if not search_verified:
            logger.warning(f"Vocabulary {vocab_uuid} deleted from database but may still be in search index")
        
        return result
        
    except Exception as e:
        logger.error(f"Vocabulary deletion failed for {vocab_uuid}: {e}")
        raise


running_tasks_lock = threading.Lock()


def run_scheduled_tasks():
    # run this function only once at a time to avoid overloading the service
    acquired = running_tasks_lock.acquire(blocking=False)

    if not acquired:
        logger.debug("Already running `run_tasks`")
        return

    try:
        while True:
            task_q = find_actionable_task_of_type([CONT_UN_OPERATION, VOCAB_DELETION_OPERATION], TASKS_GRAPH)
            task_res = query_sudo(task_q)
            if task_res["results"]["bindings"]:
                (task_uri, task_operation) = binding_results(
                    task_res, ("uri", "operation")
                )[0]
            else:
                logger.debug("No more tasks found")
                return
            try:
                inputs_res =  query_sudo(get_input_contents_task(task_uri, TASKS_GRAPH))
                inputs = binding_results(inputs_res, "content")
                similar_tasks_res = query_sudo(find_same_scheduled_tasks(task_operation, inputs,  TASKS_GRAPH))
                similar_tasks = binding_results(similar_tasks_res, "uri")
                if task_operation == CONT_UN_OPERATION:
                    logger.debug(f"Running task {task_uri}, operation {task_operation}")
                    logger.debug(f"Updating at the same time: {' | '.join(similar_tasks)}")
                    run_tasks(
                        similar_tasks,
                        TASKS_GRAPH,
                        lambda sources: [run_vocab_unification(sources[0])],
                        query_sudo,
                        update_sudo,
                    )
                elif task_operation == VOCAB_DELETION_OPERATION:
                    logger.debug(f"Running deletion task {task_uri}, operation {task_operation}")
                    logger.debug(f"Deleting at the same time: {' | '.join(similar_tasks)}")
                    run_tasks(
                        similar_tasks,
                        TASKS_GRAPH,
                        lambda sources: [execute_vocabulary_deletion_with_verification(sources[0])],
                        query_sudo,
                        update_sudo,
                    )

            except Exception as e:
                logger.error(
                    f"Problem while running task {task_uri}, operation {task_operation}: {e}"
                )
    finally:
        running_tasks_lock.release()


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

    thread = threading.Thread(target=run_scheduled_tasks)
    thread.start()

    return "", 200
