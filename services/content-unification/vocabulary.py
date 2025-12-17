import os
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")

def get_vocabulary(vocabulary, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT ?sourceDataset ?mappingShape
WHERE {
    GRAPH $graph {
        $vocabulary
            a ext:VocabularyMeta ;
            ext:sourceDataset ?sourceDataset ;
            ext:mappingShape ?mappingShape .
    }
}""")
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        vocabulary=sparql_escape_uri(vocabulary),
    )
    return query_string

def vocabulary_uri(uuid, graph=MU_APPLICATION_GRAPH):
    query_template = Template("""
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

        SELECT ?vocabulary
        WHERE {
            GRAPH $graph {
                ?vocabulary
                    a ext:VocabularyMeta ;
                    mu:uuid $uuid .
            }
        }
    """)

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        uuid=sparql_escape_string(uuid)
    )

    return query_string

