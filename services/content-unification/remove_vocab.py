import os
from string import Template
from helpers import query, logger
from escape_helpers import sparql_escape_uri, sparql_escape_string
from file import shared_uri_to_path


def remove_files(vocab_uuid: str, graph: str):
    response = query(find_file_paths(vocab_uuid, graph))
    for binding in response['results']['bindings']:
        uri = binding['dataSource']['value']
        path = shared_uri_to_path(uri)
        os.remove(path)


def find_file_paths(vocab_uuid: str, graph: str) -> str:
    query_template = Template("""
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX void: <http://rdfs.org/ns/void#>

SELECT ?dataSource WHERE {
    ?vocabMeta a ext:VocabularyMeta ;
                 mu:uuid $vocab_uuid .
    ?vocabMeta ext:sourceDataset ?sourceDataset .
    ?sourceDataset void:dataDump ?fileResource .
    ?fileResource ^nie:dataSource ?dataSource .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def select_vocab_concepts_batch(vocab_uuid: str, graph: str) -> str:
    query_template = Template("""
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

SELECT DISTINCT (?concept AS ?s) (?conceptPred AS ?p) (?conceptObj AS ?o)
WHERE {
    GRAPH $graph {
        ?vocabMeta a ext:VocabularyMeta ;
                    mu:uuid $vocab_uuid .
        ?vocabMeta ext:sourceDataset ?sourceDataset .
        ?concept dct:source ?sourceDataset .
        ?concept ?conceptPred ?conceptObj .
    }
}
LIMIT 10
""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def remove_vocab_data_dumps(vocab_uuid: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX void: <http://rdfs.org/ns/void#>

WITH $graph
DELETE {
    ?dataDump ?dataDumpPred ?dataDumpObj .
    ?dataSource ?dataSourcePred ?dataSourceObj .
    ?sourceDataset void:dataDump ?dataDump .
}
WHERE {
    ?vocab a ext:VocabularyMeta ;
           mu:uuid $vocab_uuid .
    ?vocab ext:sourceDataset ?sourceDataset .
    ?sourceDataset void:dataDump ?dataDump .
    ?dataDump ?dataDumpPred ?dataDumpObj .
    ?dataSource nie:dataSource ?dataDump .
    ?dataSource ?dataSourcePred ?dataSourceObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def remove_vocab_source_datasets(vocab_uuid: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

WITH $graph
DELETE {
    ?vocab ext:sourceDataset ?sourceDataset .
    ?sourceDataset ?sourceDatasetPred ?sourceDatasetObj .
}
WHERE {
    ?vocab a ext:VocabularyMeta ;
           mu:uuid $vocab_uuid .
    ?vocab ext:sourceDataset ?sourceDataset .
    ?sourceDataset ?sourceDatasetPred ?sourceDatasetObj.
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def remove_vocab_vocab_fetch_jobs(vocab_uuid: str, graph: str) -> str:
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
    ?vocab a ext:VocabularyMeta ;
           mu:uuid $vocab_uuid .
    ?vocab ext:sourceDataset ?sourceDataset .
    ?job prov:used ?sourceDataset .
	?job a cogs:Job .
    ?job ?jobPred ?jobObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def remove_vocab_vocab_unification_jobs(vocab_uuid: str, graph: str) -> str:
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
    ?vocab a ext:VocabularyMeta ;
           mu:uuid $vocab_uuid .
    ?job prov:used ?vocab .
	?job a cogs:Job .
    ?job ?jobPred ?jobObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def remove_vocab_meta(vocab_uuid: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

WITH $graph
DELETE {
    ?vocabMeta ?vocabMetaPred ?vocabMetaObj .
}
WHERE {
    ?vocabMeta a ext:VocabularyMeta ;
                 mu:uuid $vocab_uuid .
    ?vocabMeta ?vocabMetaPred ?vocabMetaObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def remove_vocab_partitions(vocab_uuid: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX void: <http://rdfs.org/ns/void#>

WITH $graph

DELETE {
    ?partition ?partitionPred ?partitionObj .
}
WHERE {
    ?vocab a ext:VocabularyMeta ;
           mu:uuid $vocab_uuid .
    ?vocab ext:sourceDataset ?sourceDataset .
    ?sourceDataset ?classPropPart ?partition .
    ?partition ?partitionPred ?partitionObj .
    VALUES ?classPropPart { void:classPartition void:propertyPartition }
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def remove_vocab_mapping_shape(vocab_uuid: str, graph: str) -> str:
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
    ?vocab a ext:VocabularyMeta ;
           mu:uuid $vocab_uuid .
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
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def remove_vocab_ldes_containers(vocab_uuid: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX dct: <http://purl.org/dc/terms/>

WITH $graph
DELETE {
    ?datasetGraph ?datasetGraphPred ?datasetGraphObj .
}
WHERE {
    ?vocab a ext:VocabularyMeta ;
           mu:uuid $vocab_uuid .
    ?vocab ext:sourceDataset ?sourceDataset .
    ?sourceDataset dct:type <http://vocabsearch.data.gift/dataset-types/LDES> ;
                   ext:datasetGraph ?datasetGraph .
    ?datasetGraph ?datasetGraphPred ?datasetGraphObj .
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string


def get_vocab_ldes_containers(vocab_uuid: str, graph: str) -> str:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT ?datasetGraph WHERE {
    GRAPH $graph {
        ?vocab a ext:VocabularyMeta ;
               mu:uuid $vocab_uuid .
        ?vocab ext:sourceDataset ?sourceDataset .
        ?sourceDataset dct:type <http://vocabsearch.data.gift/dataset-types/LDES> ;
                       ext:datasetGraph ?datasetGraph .
    }
}
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uuid=sparql_escape_string(vocab_uuid),
    )
    return query_string
