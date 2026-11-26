import os
from datetime import datetime
from string import Template
import threading

import requests
from escape_helpers import sparql_escape, sparql_escape_uri
from flask import request
from helpers import generate_uuid, logger
from helpers import query
from sudo_query import update_sudo

from vocabularies import (
    get_vocabs_config_triples,
    matching_aliasses_in_graph,
    remove_aliases_vocabs,
    copy_vocabs_configs_from_graph,
    replace_with_uniq_uuid,
    matching_uris_in_graph,
    datasets_of_vocab,
    vocab_uris_from_graph,
)

from rdflib import Graph, URIRef, Literal
from rdflib.void import generateVoID

from file import file_to_shared_uri, shared_uri_to_path
from file import construct_get_file_query, construct_insert_file_query
from task import (
    find_actionable_task_of_type,
    run_task,
    start_download_task,
)
from sparql_util import serialize_graph_to_sparql

from sparql_util import sparql_construct_res_to_graph, drop_graph, binding_results

# Maybe make these configurable
TASKS_GRAPH = "http://mu.semte.ch/graphs/public"
FILES_GRAPH = "http://mu.semte.ch/graphs/public"
BASE_IMPORT_GRAPH = "http://example-resource.com/graph/VocabImport/"

FILE_RESOURCE_BASE = "http://example-resource.com/files/"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")

VOCAB_EXPORT_OPERATION = "http://mu.semte.ch/vocabularies/ext/VocabsExportJob"
VOCAB_IMPORT_OPERATION = "http://mu.semte.ch/vocabularies/ext/VocabsImportJob"

VOCABULARY_TYPE = "http://mu.semte.ch/vocabularies/ext/VocabularyMeta"
DATA_DUMP_TYPE = "http://vocabsearch.data.gift/dataset-types/FileDump"
LDES_TYPE = "http://vocabsearch.data.gift/dataset-types/LDES"


def do_vocab_export(vocab_uris):
    construct_json = query(get_vocabs_config_triples(vocab_uris), sudo=True)
    g = sparql_construct_res_to_graph(construct_json)
    phys_uuid = generate_uuid()
    phys_filename = "export_" + phys_uuid + ".ttl"
    share_uri = file_to_shared_uri(phys_filename)
    share_path = shared_uri_to_path(share_uri)
    # ensure the share folder exists
    os.makedirs(os.path.split(share_path)[0], exist_ok=True)
    g.serialize(destination=share_path, format="turtle")

    upload_resource_uuid = generate_uuid()
    upload_resource_uri = f"{FILE_RESOURCE_BASE}{upload_resource_uuid}"
    file_resource_uuid = generate_uuid()
    file_resource_name = f"{file_resource_uuid}.ttl"

    file = {
        "uri": upload_resource_uri,
        "uuid": upload_resource_uuid,
        "name": file_resource_name,
        "mimetype": "text/plain",
        "created": datetime.now(),
        "size": os.path.getsize(share_path),
        "extension": "ttl",
    }
    physical_file = {
        "uri": share_uri,
        "uuid": phys_uuid,
        "name": phys_filename,
    }

    query_string = construct_insert_file_query(file, physical_file, FILES_GRAPH)
    update_sudo(query_string)

    return [upload_resource_uri]


def vocab_config_file_to_graph(file_uri: str, files_graph: str):
    query_string = construct_get_file_query(file_uri, files_graph)
    file_result = query(query_string, sudo=True)["results"]["bindings"][0]

    g = Graph()
    g.parse(shared_uri_to_path(file_result["physicalFile"]["value"]))

    return g


def import_vocab_configs(file_uri):
    graph_uri = BASE_IMPORT_GRAPH + generate_uuid()

    configs_g = vocab_config_file_to_graph(file_uri, FILES_GRAPH)
    for query_string in serialize_graph_to_sparql(configs_g, graph_uri):
        update_sudo(query_string)

    # check if alias is already in use
    matching_aliases_results = query(matching_aliasses_in_graph(graph_uri), sudo=True)
    matching_aliases = binding_results(matching_aliases_results, ("vocab", "alias"))
    # remove those from the import graph
    if matching_aliases:
        update_sudo(remove_aliases_vocabs(matching_aliases, graph_uri))

    # remove overlap for UUID of vocab and sourceDatasets UUIDs and mapping UUIDs
    matching_uris_results = query(matching_uris_in_graph(graph_uri), sudo=True)

    for uri in binding_results(matching_uris_results, "uri"):
        base = uri.rsplit("/", 1)[0] + "/"
        new_uuid = generate_uuid()
        new_uri = base + new_uuid
        update_sudo(replace_with_uniq_uuid(uri, new_uri, new_uuid, graph_uri))

    # import the desired triples from the temp_graph
    update_sudo(copy_vocabs_configs_from_graph(graph_uri))

    # get the vocab URIs that were imported
    new_vocab_uris_results = query(vocab_uris_from_graph(graph_uri), sudo=True)
    new_vocab_uris = binding_results(new_vocab_uris_results, "uri")
    # remove import graph
    drop_graph(graph_uri)
    # start a download task for the new vocabs
    for vocab_uri in new_vocab_uris:
        datasets_results = query(datasets_of_vocab(vocab_uri), sudo=True)
        for dataset_uri, data_type in binding_results(
            datasets_results, ("uri", "data_type")
        ):
            if data_type == DATA_DUMP_TYPE:
                update_sudo(start_download_task(dataset_uri, TASKS_GRAPH))

    return new_vocab_uris


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
                [VOCAB_EXPORT_OPERATION, VOCAB_IMPORT_OPERATION], TASKS_GRAPH
            )
            task_res = query(task_q, sudo=True)
            if task_res["results"]["bindings"]:
                (task_uri, task_operation) = binding_results(
                    task_res, ("uri", "operation")
                )[0]
            else:
                logger.debug("No more tasks found")
                return
            try:
                if task_operation == VOCAB_EXPORT_OPERATION:
                    logger.debug(f"Running task {task_uri}, operation {task_operation}")
                    run_task(
                        task_uri,
                        TASKS_GRAPH,
                        lambda sources: do_vocab_export(sources),
                        query,
                        update_sudo,
                    )
                elif task_operation == VOCAB_IMPORT_OPERATION:
                    logger.debug(f"Running task {task_uri}, operation {task_operation}")
                    run_task(
                        task_uri,
                        TASKS_GRAPH,
                        lambda fileUris: import_vocab_configs(fileUris[0]),
                        query,
                        update_sudo,
                    )
            finally:
                logger.warn(
                    f"Problem while running task {task_uri}, operation {task_operation}"
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

    thread = threading.Thread(target=run_tasks)
    thread.start()

    return "", 200
