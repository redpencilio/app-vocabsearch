import os
from string import Template
import threading
import time

from rdflib import Graph, URIRef
import requests
from more_itertools import batched

from flask import request

from escape_helpers import sparql_escape, sparql_escape_uri
from helpers import generate_uuid, logger, query
from sudo_query import update_sudo

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
    count_ununified,
    start_filter_count_task,
    get_filter_count_input,
    remove_filter_count_input,
    write_filter_count_output,
    get_delete_subjects_batch,
    delete_subjects,
    delete_dataset_subjects_from_graph,
)
from remove_vocab import (
    start_vocab_delete_task,
    run_vocab_delete_operation,
    run_vocab_delete_wait_operation
)
from constants import (
    FILE_RESOURCE_BASE,
    SLEEP_MS_BETWEEN_QUERIES_UNIFY,
    TASKS_GRAPH,
    VOCAB_GRAPH,
    UNIFICATION_TARGET_GRAPH,
    MU_APPLICATION_GRAPH,
    DATA_GRAPH,
    CONT_UN_OPERATION,
    FILTER_COUNT_OPERATION,
    VOCAB_DELETE_OPERATION,
    VOCAB_DELETE_WAIT_OPERATION,
    UNIFICATION_BATCH_SIZE,
)

TEMP_GRAPH_BASE = "http://example-resource.com/graph/"

def build_temp_graphs(datasets):
    # for every dataset:
    # if file-based, load into temporary graph
    # if graph-based, return graph directly
    graphs = set()
    temp_named_graph = TEMP_GRAPH_BASE + generate_uuid()

    for dataset in datasets:
        dataset_versions = query(
            get_dataset(dataset, VOCAB_GRAPH),
            sudo=True)["results"]["bindings"]

        if "dataset_graph" in dataset_versions[0].keys():
            graphs.add(dataset_versions[0]["dataset_graph"]["value"])

        elif "data_dump" in dataset_versions[0].keys():
            graphs.add(temp_named_graph)
            temp_named_graph = load_file_to_db(
                dataset_versions[0]["data_dump"]["value"], VOCAB_GRAPH, temp_named_graph
            )
            # this should only load in the data, diffing is done by unification itself.
        else:
            pass
            # TODO: return error?

    return list(graphs), temp_named_graph

def run_vocab_unification(vocab_uri):
    vocab_sources = query(get_vocabulary(vocab_uri, VOCAB_GRAPH), sudo=True)["results"][
        "bindings"
    ]

    if not vocab_sources:
        raise Exception(f"Vocab {vocab_uri} does not have a mapping. Unification cancelled.")

    datasets = [vocab_source["sourceDataset"]["value"] for vocab_source in vocab_sources]

    dataset_graphs, temp_named_graph = build_temp_graphs(datasets)
    try:
      prop_paths_qs = get_property_paths(
          vocab_sources[0]["mappingShape"]["value"], VOCAB_GRAPH
      )
      prop_paths_res = query(prop_paths_qs, sudo=True)

      for path_props in prop_paths_res["results"]["bindings"]:
        # Delete obsolete entities first
        while True:
            delete_subjects_batch_qs = get_delete_subjects_batch(
                dest_class=path_props["destClass"]["value"],
                source_datasets=[
                    vocab_source["sourceDataset"]["value"]
                    for vocab_source in vocab_sources
                ],
                source_class=path_props["sourceClass"]["value"],
                source_path_string=path_props["sourcePathString"]["value"],  # !
                source_filter=path_props.get("sourceFilter", {}).get("value") or '',
                source_graphs=dataset_graphs,
                target_graph=VOCAB_GRAPH,
                batch_size=UNIFICATION_BATCH_SIZE,
             )
            batch_res = query(delete_subjects_batch_qs, sudo=True)
            if not batch_res["results"]["bindings"]:
                logger.info("Finished deleting obsolete unifications")
                break
            else:
                logger.info("Deleting obsolete batch")

            delete_subjects_qs = delete_subjects(
                target_subjects=[b["targetSubject"]["value"] for b in batch_res["results"]["bindings"]],
                target_graph=VOCAB_GRAPH
            )
            update_sudo(delete_subjects_qs, max_retries=0)
            if(SLEEP_MS_BETWEEN_QUERIES_UNIFY and SLEEP_MS_BETWEEN_QUERIES_UNIFY >0 ):
              logger.info(f"Waiting for {SLEEP_MS_BETWEEN_QUERIES_UNIFY} ms")
              time.sleep(SLEEP_MS_BETWEEN_QUERIES_UNIFY / 1000)

        # Then unify new ones
        while True:
            get_batch_qs = get_ununified_batch(
                dest_class=path_props["destClass"]["value"],
                dest_predicate=path_props["destPath"]["value"],
                source_datasets=[
                    vocab_source["sourceDataset"]["value"]
                    for vocab_source in vocab_sources
                ],
                source_class=path_props["sourceClass"]["value"],
                source_path_string=path_props["sourcePathString"]["value"],  # !
                source_filter=path_props.get("sourceFilter", {}).get("value") or '',
                source_graphs=dataset_graphs,
                target_graph=VOCAB_GRAPH,
                batch_size=UNIFICATION_BATCH_SIZE,
            )
            # We might want to dump intermediary unified content to file before committing to store
            batch_res = query(get_batch_qs, sudo=True)
            if not batch_res["results"]["bindings"]:
                logger.info("Finished unification")
                break
            else:
                logger.info("Running unification batch")
            g = sparql_construct_res_to_graph(batch_res)
            for query_string in serialize_graph_to_sparql(g, VOCAB_GRAPH):
                update_sudo(query_string, max_retries=0)
            if(SLEEP_MS_BETWEEN_QUERIES_UNIFY and SLEEP_MS_BETWEEN_QUERIES_UNIFY > 0 ):
              logger.info(f"Waiting for {SLEEP_MS_BETWEEN_QUERIES_UNIFY} ms")
              time.sleep(SLEEP_MS_BETWEEN_QUERIES_UNIFY / 1000)
    except Exception as e:
        logger.error(f"Error during vocab {vocab_uri} unification: {e}")
        raise e
    finally:
        drop_graph(temp_named_graph)
    return vocab_uri

@app.route("/filter-count/", methods=('POST',))
def filter_count():
    dataset_uri = request.form['dataset_uri']
    source_class = request.form['class']
    source_path_string= request.form['source_path_string']
    source_filter: str = request.form['filter']

    task_uuid, qs = start_filter_count_task(
        dataset_uri=dataset_uri,
        source_class=source_class,
        source_path_string=source_path_string,
        source_filter=source_filter,
        graph=TASKS_GRAPH
    )
    query(qs, sudo=True)
    return {
        'meta': {
            'task_uuid': task_uuid
        }
    }

def run_filter_count_task(input):
    input_qs = get_filter_count_input(input)
    input_res = query(input_qs, sudo=True)
    input_bindings= input_res["results"]["bindings"][0]

    dataset_uri = input_bindings["dataset"]["value"]
    source_class = input_bindings["sourceClass"]["value"]
    source_path_string = input_bindings["sourcePathString"]["value"]
    source_filter = input_bindings["sourceFilter"]["value"]

    from unification import UNUNIFIED_BATCH_TEMPLATE_VARS
    shadowed_vars = [var for var in UNUNIFIED_BATCH_TEMPLATE_VARS if source_filter.find(var) != -1]

    warning = None
    if not shadowed_vars == []:
        warning = f"Found vars in your filter that overlap with internally used vars. This could lead to unexpected behaviour: {', '.join(shadowed_vars)}"

    dataset_graphs, temp_named_graph = build_temp_graphs([dataset_uri])

    from SPARQLWrapper.SPARQLExceptions import EndPointInternalError

    count_query = count_ununified(
            source_class, source_path_string, source_filter, dataset_graphs
        )
    try:
        filter_res = query(count_query, sudo=True)
    except (EndPointInternalError) as e:
        output_uri, qs = write_filter_count_output(
                graph=DATA_GRAPH,
                dataset_uri=dataset_uri,
                query=count_query.strip(),
                valid=False,
                error=str(e),
        )
        update_sudo(qs, max_retries=0)
        return output_uri
    finally:
        drop_graph(temp_named_graph)
        update_sudo(remove_filter_count_input(input_uri=input, graph=DATA_GRAPH), max_retries=0)

    count = filter_res["results"]["bindings"][0]["count"]["value"]
    count = int(count)

    output_uri, qs = write_filter_count_output(
        graph=DATA_GRAPH,
        dataset_uri=dataset_uri,
        query=count_query.strip(),
        valid=True,
        count=count,
        warning=warning
    )
    update_sudo(qs, max_retries=0)
    return output_uri


@app.route("/delete-vocabulary/<vocab_uuid>", methods=("DELETE",))
def delete_vocabulary(vocab_uuid: str):
    task_uuid = generate_uuid()
    logger.info(f"Deleting vocab {vocab_uuid}, task id: {task_uuid}")
    vocab_iri = query(vocabulary_uri(vocab_uuid, DATA_GRAPH), sudo=True)["results"]["bindings"][0]["vocabulary"]["value"]
    update_sudo(start_vocab_delete_task(vocab_iri, task_uuid, TASKS_GRAPH), max_retries=0)

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
                [CONT_UN_OPERATION, FILTER_COUNT_OPERATION, VOCAB_DELETE_OPERATION, VOCAB_DELETE_WAIT_OPERATION],
                TASKS_GRAPH)
            task_res = query(task_q, sudo=True)
            if task_res["results"]["bindings"]:
                (task_uri, task_operation) = binding_results(
                    task_res, ("uri", "operation")
                )[0]
            else:
                logger.debug("No more tasks found")
                return
            inputs_res = query(get_input_contents_task(task_uri, TASKS_GRAPH), sudo=True)
            inputs = binding_results(inputs_res, "content")
            similar_tasks_res = query(find_same_scheduled_tasks(task_operation, inputs,  TASKS_GRAPH), sudo=True)
            similar_tasks = binding_results(similar_tasks_res, "uri")
            if task_operation == CONT_UN_OPERATION:
                logger.debug(f"Running task {task_uri}, operation {task_operation}")
                logger.debug(f"Updating at the same time: {' | '.join(similar_tasks)}")
                run_tasks(
                    similar_tasks,
                    TASKS_GRAPH,
                    lambda sources: [run_vocab_unification(sources[0])],
                    lambda query_string: query(query_string, sudo=True),
                    lambda update_string: update_sudo(update_string, max_retries=0),
                )
            elif task_operation == FILTER_COUNT_OPERATION:
                logger.debug(f"Running task {task_uri}, operation {task_operation}")
                logger.debug(f"Updating at the same time: {' | '.join(similar_tasks)}")
                run_tasks(
                    similar_tasks,
                    TASKS_GRAPH,
                    lambda sources: [run_filter_count_task(sources[0])],
                    lambda query_string: query(query_string, sudo=True),
                    lambda update_string: update_sudo(update_string, max_retries=0),
                )
            elif task_operation == VOCAB_DELETE_OPERATION:
                logger.debug(f"Running task {task_uri}, operation {task_operation}")
                logger.debug(f"Updating at the same time: {' | '.join(similar_tasks)}")
                run_tasks(
                    similar_tasks,
                    TASKS_GRAPH,
                    lambda sources: [run_vocab_delete_operation(sources[0])],
                    lambda query_string: query(query_string, sudo=True),
                    lambda update_string: update_sudo(update_string, max_retries=0),
                )
            elif task_operation == VOCAB_DELETE_WAIT_OPERATION:
                logger.debug(f"Running task {task_uri}, operation {task_operation}")
                logger.debug(f"Updating at the same time: {' | '.join(similar_tasks)}")
                run_tasks(
                    similar_tasks,
                    TASKS_GRAPH,
                    lambda sources: [run_vocab_delete_wait_operation(sources[0])],
                    lambda query_string: query(query_string, sudo=True),
                    lambda update_string: update_sudo(update_string, max_retries=0),
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
