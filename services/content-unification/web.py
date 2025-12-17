import os
from string import Template
import threading

from rdflib import Graph, URIRef
import requests
from more_itertools import batched

from flask import request

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

from task import find_actionable_task_of_type, find_same_scheduled_tasks, get_input_contents_task, run_task, find_actionable_task, run_tasks
from vocabulary import get_vocabulary, vocabulary_uri
from dataset import get_dataset

from unification import (
    get_property_paths,
    get_ununified_batch,
    delete_dataset_subjects_from_graph,
)
from remove_vocab import (
    VOCAB_DELETE_OPERATION,
    VOCAB_DELETE_WAIT_OPERATION,
    start_vocab_delete_task,
    run_vocab_delete_operation,
    run_vocab_delete_wait_operation
)

# Maybe make these configurable
FILE_RESOURCE_BASE = "http://example-resource.com/"
TASKS_GRAPH = "http://mu.semte.ch/graphs/public"
TEMP_GRAPH_BASE = "http://example-resource.com/graph/"
VOCAB_GRAPH = "http://mu.semte.ch/graphs/public"
UNIFICATION_TARGET_GRAPH = "http://mu.semte.ch/graphs/public"
MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
DATA_GRAPH = "http://mu.semte.ch/graphs/public"
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
    task_uuid = generate_uuid()
    logger.info(f"Deleting vocab {vocab_uuid}, task id: {task_uuid}")
    vocab_iri = query_sudo(vocabulary_uri(vocab_uuid, DATA_GRAPH))["results"]["bindings"][0]["vocabulary"]["value"]
    update_sudo(start_vocab_delete_task(vocab_iri, task_uuid, TASKS_GRAPH))

    return {
        'meta': { 'task_id': task_uuid }
    }


running_tasks_lock = threading.Lock()


def run_scheduled_tasks():
    # run this function only once at a time to avoid overloading the service
    acquired = running_tasks_lock.acquire(blocking=False)

    if not acquired:
        logger.debug("Already running `run_tasks`")
        return

    try:
        while True:
            task_q = find_actionable_task_of_type(
                [CONT_UN_OPERATION, VOCAB_DELETE_OPERATION, VOCAB_DELETE_WAIT_OPERATION],
                TASKS_GRAPH)
            task_res = query_sudo(task_q)
            if task_res["results"]["bindings"]:
                (task_uri, task_operation) = binding_results(
                    task_res, ("uri", "operation")
                )[0]
            else:
                logger.debug("No more tasks found")
                return
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
            elif task_operation == VOCAB_DELETE_OPERATION:
                logger.debug(f"Running task {task_uri}, operation {task_operation}")
                logger.debug(f"Updating at the same time: {' | '.join(similar_tasks)}")
                run_tasks(
                    similar_tasks,
                    TASKS_GRAPH,
                    lambda sources: [run_vocab_delete_operation(sources[0])],
                    query_sudo,
                    update_sudo,
                )
            elif task_operation == VOCAB_DELETE_WAIT_OPERATION:
                logger.debug(f"Running task {task_uri}, operation {task_operation}")
                logger.debug(f"Updating at the same time: {' | '.join(similar_tasks)}")
                run_tasks(
                    similar_tasks,
                    TASKS_GRAPH,
                    lambda sources: [run_vocab_delete_wait_operation(sources[0])],
                    query_sudo,
                    update_sudo,
                    thread=True
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
