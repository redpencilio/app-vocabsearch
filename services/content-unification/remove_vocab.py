import os
import datetime
from string import Template
from helpers import query, logger, generate_uuid
from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_datetime
from file import shared_uri_to_path
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

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
DATA_GRAPH = "http://mu.semte.ch/graphs/public"

CONTAINER_URI_PREFIX = "http://redpencil.data.gift/id/container/"
JOB_URI_PREFIX = "http://redpencil.data.gift/id/job/"
TASK_URI_PREFIX = "http://redpencil.data.gift/id/task/"
TASKS_GRAPH = "http://mu.semte.ch/graphs/public"

VOCAB_DELETE_OPERATION = "http://mu.semte.ch/vocabularies/ext/VocabDeleteJob"
VOCAB_DELETE_WAIT_OPERATION = "http://mu.semte.ch/vocabularies/ext/VocabDeleteWaitJob"
VOCAB_GRAPH = "http://mu.semte.ch/graphs/public"

def start_vocab_delete_task(vocab_iri, task_uuid, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    INSERT DATA {
      GRAPH $graph {
        $container_uri a nfo:DataContainer;
          mu:uuid $container_uuid;
          ext:content $vocab_iri .
        $job_uri a cogs:Job ;
          mu:uuid $job_uuid;
          dct:created $created;
          dct:modified $created;
          dct:creator "empty";
          task:operation <http://lblod.data.gift/id/jobs/concept/JobOperation/vocab-delete> ;
          adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> .
        $task_uri a task:Task ;
            mu:uuid $task_uuid ;
            dct:created $created ;
            dct:modified $created ;
            task:index "0";
            dct:isPartOf $job_uri;
            task:inputContainer $container_uri;
            task:operation <http://mu.semte.ch/vocabularies/ext/VocabDeleteJob> ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> .
      }
    }
                              """)

    container_uuid = generate_uuid()
    container_uri = CONTAINER_URI_PREFIX + container_uuid
    job_uuid = generate_uuid()
    job_uri = JOB_URI_PREFIX + job_uuid
    task_uri = TASK_URI_PREFIX + task_uuid
    created = datetime.datetime.now()
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        container_uri=sparql_escape_uri(container_uri),
        job_uri=sparql_escape_uri(job_uri),
        task_uri=sparql_escape_uri(task_uri),
        container_uuid=sparql_escape_string(container_uuid),
        job_uuid=sparql_escape_string(job_uuid),
        task_uuid=sparql_escape_string(task_uuid),
        created=sparql_escape_datetime(created),
        vocab_iri=sparql_escape_uri(vocab_iri)
    )

    return query_string


def run_vocab_delete_operation(vocab_uri):
    remove_files(vocab_uri, VOCAB_GRAPH)
    update_sudo(remove_vocab_data_dumps(vocab_uri, VOCAB_GRAPH))

    # concepts may return too many results in mu-auth internal construct. Batch it here.
    while True:
        batch = query_sudo(select_vocab_concepts_batch(vocab_uri, VOCAB_GRAPH))
        bindings = batch["results"]["bindings"]
        if bindings:
            g = sparql_construct_res_to_graph(batch)
            for query_string in serialize_graph_to_sparql(g, VOCAB_GRAPH, "DELETE"):
                update_sudo(query_string)
        else:
            break
    # todo: these job deletions are not yet adjusted to the new Jobs structure (which use data containers)
    update_sudo(remove_vocab_vocab_fetch_jobs(vocab_uri, VOCAB_GRAPH))
    update_sudo(remove_vocab_vocab_unification_jobs(vocab_uri, VOCAB_GRAPH))
    update_sudo(remove_vocab_partitions(vocab_uri, VOCAB_GRAPH))
    update_sudo(remove_vocab_source_datasets(vocab_uri, VOCAB_GRAPH))
    update_sudo(remove_vocab_mapping_shape(vocab_uri, VOCAB_GRAPH))

    update_sudo(start_vocab_delete_wait_task(vocab_uri, TASKS_GRAPH))
    return vocab_uri

def remove_files(vocab_uri: str, graph: str):
    response = query_sudo(find_file_paths(vocab_uri, graph))
    for binding in response['results']['bindings']:
        uri = binding['dataSource']['value']
        path = shared_uri_to_path(uri)
        os.remove(path)


def find_file_paths(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX void: <http://rdfs.org/ns/void#>

SELECT ?dataSource WHERE {
    $vocab a ext:VocabularyMeta ;
        ext:sourceDataset ?sourceDataset .
    ?sourceDataset void:dataDump ?fileResource .
    ?fileResource ^nie:dataSource ?dataSource .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string


def select_vocab_concepts_batch(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

SELECT DISTINCT (?concept AS ?s) (?conceptPred AS ?p) (?conceptObj AS ?o)
WHERE {
    GRAPH $graph {
        { SELECT ?concept {
            $vocab a ext:VocabularyMeta ;
                ext:sourceDataset ?sourceDataset .
            ?concept dct:source ?sourceDataset .
        } LIMIT 10 }
        ?concept ?conceptPred ?conceptObj .
    }
}
""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string


def remove_vocab_data_dumps(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX void: <http://rdfs.org/ns/void#>

WITH $graph
DELETE {
    ?dataDump ?dataDumpPred ?dataDumpObj .
    ?dataSource ?dataSourcePred ?dataSourceObj .
    ?sourceDataset void:dataDump ?dataDump .
}
WHERE {
    $vocab a ext:VocabularyMeta ;
        ext:sourceDataset ?sourceDataset .
    ?sourceDataset void:dataDump ?dataDump .
    ?dataDump ?dataDumpPred ?dataDumpObj .
    ?dataSource nie:dataSource ?dataDump .
    ?dataSource ?dataSourcePred ?dataSourceObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string


def remove_vocab_source_datasets(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

WITH $graph
DELETE {
    ?vocab ext:sourceDataset ?sourceDataset .
    ?sourceDataset ?sourceDatasetPred ?sourceDatasetObj .
}
WHERE {
    $vocab a ext:VocabularyMeta ;
        ext:sourceDataset ?sourceDataset .
    ?sourceDataset ?sourceDatasetPred ?sourceDatasetObj.
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string


def remove_vocab_vocab_fetch_jobs(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX prov: <http://www.w3.org/ns/prov#>

WITH $graph

DELETE {
    ?job ?jobPred ?jobObj .
}
WHERE {
    $vocab a ext:VocabularyMeta ;
        ext:sourceDataset ?sourceDataset .
    ?job prov:used ?sourceDataset .
	?job a cogs:Job .
    ?job ?jobPred ?jobObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string


def remove_vocab_vocab_unification_jobs(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX prov: <http://www.w3.org/ns/prov#>

WITH $graph

DELETE {
    ?job ?jobPred ?jobObj .
}
WHERE {
    $vocab a ext:VocabularyMeta .
    ?job prov:used ?vocab .
	?job a cogs:Job .
    ?job ?jobPred ?jobObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string


def remove_vocab_meta(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

WITH $graph
DELETE {
    $vocab ?vocabMetaPred ?vocabMetaObj .
}
WHERE {
    $vocab a ext:VocabularyMeta ;
        ?vocabMetaPred ?vocabMetaObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string


def remove_vocab_partitions(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX void: <http://rdfs.org/ns/void#>

WITH $graph

DELETE {
    ?partition ?partitionPred ?partitionObj .
}
WHERE {
    $vocab a ext:VocabularyMeta ;
        ext:sourceDataset ?sourceDataset .
    ?sourceDataset ?classPropPart ?partition .
    ?partition ?partitionPred ?partitionObj .
    VALUES ?classPropPart { void:classPartition void:propertyPartition }
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string


def remove_vocab_mapping_shape(vocab_uri: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX shacl: <http://www.w3.org/ns/shacl#>

WITH $graph

DELETE {
    ?mappingShape ?mappingShapePred ?mappingShapeObj .
    ?propertyShape ?propertyShapePred ?propertyShapeObj .
}
WHERE {
    VALUES ?vocab { $vocab }
    ?vocab a ext:VocabularyMeta .
    ?vocab ext:mappingShape ?mappingShape .
    ?mappingShape ?mappingShapePred ?mappingShapeObj .
    OPTIONAL {
        ?mappingShape shacl:property ?propertyShape .
        ?propertyShape ?propertyShapePred ?propertyShapeObj.
    }
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab=sparql_escape_uri(vocab_uri),
    )
    return query_string

def start_vocab_delete_wait_task(vocab_iri, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    INSERT {
      GRAPH $graph {
        $task_uri a task:Task ;
            mu:uuid $task_uuid ;
            dct:created $created ;
            dct:modified $created ;
            task:index "1";
            dct:isPartOf ?job ;
            task:inputContainer ?container ;
            task:operation <http://mu.semte.ch/vocabularies/ext/VocabDeleteWaitJob> ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> .
      }
    } WHERE {
        ?container a nfo:DataContainer ;
            ext:content $vocab_iri .

        ?prev_task a task:Task ;
            task:inputContainer ?container ;
            task:operation <http://mu.semte.ch/vocabularies/ext/VocabDeleteJob> ;
            dct:isPartOf ?job .

        ?job a cogs:Job ;
          task:operation <http://lblod.data.gift/id/jobs/concept/JobOperation/vocab-delete> .
    }
                              """)

    task_uuid = generate_uuid()
    task_uri = TASK_URI_PREFIX + task_uuid
    created = datetime.datetime.now()
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        task_uri=sparql_escape_uri(task_uri),
        task_uuid=sparql_escape_string(task_uuid),
        created=sparql_escape_datetime(created),
        vocab_iri=sparql_escape_uri(vocab_iri)
    )

    return query_string

MAX_NO_PROGRESS = 5

def run_vocab_delete_wait_operation(vocab_uri):
    import time

    tries = 0

    previous_count = None
    no_progress_count = 0

    while True:

        sleep_time = 10 if tries <= 3 else 60
        time.sleep(sleep_time)

        tries += 1
        count = count_concepts_in_search(vocab_uri)

        if previous_count is not None and count >= previous_count:
            no_progress_count += 1
            if no_progress_count >= MAX_NO_PROGRESS:
                raise RuntimeError(
                    f"Vocabulary deletion stuck at {count} concepts after {no_progress_count} checks with no progress: {vocab_uri}"
                )
        else:
            no_progress_count = 0

        if count > 0:
            logger.info(f"Vocab still has {count} concepts in search: {vocab_uri}")
            previous_count = count
            continue
        else:
            break

    logger.info(f"Vocab removed from search in {tries} tries: {vocab_uri}")
    update_sudo(remove_vocab_meta(vocab_uri, VOCAB_GRAPH))

def count_concepts_in_search(vocab_uri) -> int:
    import requests

    headers = {"Accept": "application/vnd.api+json"}
    url = "http://search/concepts/search"
    params = {
        'filter[vocabulary]': vocab_uri,
        'page[size]': 1
    }

    with requests.get(url, headers=headers, params=params) as res:
        assert res.ok
        json = res.json()
        count = json['count']
        return count
