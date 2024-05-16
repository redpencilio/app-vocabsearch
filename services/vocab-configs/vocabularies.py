import os
from string import Template
from escape_helpers import (
    sparql_escape_uri,
    sparql_escape_datetime,
    sparql_escape_string,
)
from helpers import generate_uuid, logger

DATA_GRAPH = "http://mu.semte.ch/graphs/public"


# only call where graph_orig has no overlap in URIs of graph_target
def copy_vocabs_configs_from_graph(graph_orig, graph_target=DATA_GRAPH):
    query_template = Template("""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX void: <http://rdfs.org/ns/void#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX shacl: <http://www.w3.org/ns/shacl#>

    INSERT {
      GRAPH $graph_target {
    ?vocab a ext:VocabularyMeta;
            rdfs:label ?label;
            mu:uuid ?vocabUuid;
            ext:sourceDataset ?sourceDataset;
            ext:mappingShape ?mappingShape ;
            dbo:alias ?alias.
    ?sourceDataset a void:Dataset;
            dcterms:type ?type;
            foaf:page ?page;
            mu:uuid ?sourceUuid;
            void:feature ?format;
            ext:maxRequests ?maxRequests;
            ext:dereferenceMembers ?derefMembers.
    ?mappingShape a shacl:NodeShape ;
                  mu:uuid ?msUuid ;
                  shacl:targetClass ?msTargetClass ;
                  shacl:property ?msPropertyShape .
    ?msPropertyShape a shacl:PropertyShape;
                   mu:uuid ?psUuid ;
                   shacl:path ?psPath;
                   shacl:description ?psDescription .
            }
    } WHERE {
      GRAPH $graph_source {
        ?vocab a ext:VocabularyMeta;
            rdfs:label ?label;
            mu:uuid ?vocabUuid .    
            
        {
            ?vocab dbo:alias ?alias .
        } UNION {
            ?vocab ext:sourceDataset ?sourceDataset . 
            ?sourceDataset a void:Dataset;
                dcterms:type ?type;
                foaf:page ?page;
                mu:uuid ?sourceUuid;
                void:feature ?format;
                ext:maxRequests ?maxRequests;
                ext:dereferenceMembers ?derefMembers.
        } UNION {
            ?vocab ext:mappingShape ?mappingShape.
            {
                ?mappingShape a shacl:NodeShape ;
                    mu:uuid ?msUuid ;
                    shacl:targetClass ?msTargetClass .
            } UNION {      
                ?mappingShape shacl:property ?msPropertyShape .
                ?msPropertyShape a shacl:PropertyShape;
                    mu:uuid ?psUuid ;
                    shacl:path ?psPath;
                    shacl:description ?psDescription .    
            }
        }                       
      }
    } 
    """)
    query = query_template.substitute(
        graph_target=sparql_escape_uri(graph_target),
        graph_source=sparql_escape_uri(graph_orig),
    )
    return query


def get_vocabs_config_triples(vocab_uris, graph=DATA_GRAPH):
    query_template = Template("""
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX void: <http://rdfs.org/ns/void#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX shacl: <http://www.w3.org/ns/shacl#>

CONSTRUCT {
 ?vocab a ext:VocabularyMeta;
         mu:uuid ?vocabUuid;
         rdfs:label ?label;
         ext:sourceDataset ?sourceDataset;
         ext:mappingShape ?mappingShape.
  ?mappingShape a shacl:NodeShape ;
                  mu:uuid ?msUuid;
                  shacl:targetClass ?msTargetClass ;
                  shacl:property ?msPropertyShape .
  ?msPropertyShape a shacl:PropertyShape;
                   mu:uuid ?psUuid;
                   shacl:path ?psPath;
                   shacl:description ?psDescription .        
  ?vocab dbo:alias ?alias.
 ?sourceDataset a void:Dataset;
         mu:uuid ?sdUuid;
         dcterms:type ?type;
         foaf:page ?page;
         void:feature ?format;
         ext:maxRequests ?maxRequests;
         ext:dereferenceMembers ?derefMembers.
} WHERE {
  GRAPH $graph {
    VALUES ?vocab { $vocab_uris }
   ?vocab a ext:VocabularyMeta;
            mu:uuid ?vocabUuid;
            rdfs:label ?label .        
        {
            ?vocab dbo:alias ?alias .
        } UNION {
            ?vocab ext:sourceDataset ?sourceDataset .
            ?sourceDataset a void:Dataset;
                mu:uuid ?sdUuid;
                dcterms:type ?type;
                foaf:page ?page;
                ext:maxRequests ?maxRequests;
                ext:dereferenceMembers ?derefMembers.
            OPTIONAL { 
              ?sourceDataset void:feature ?format .
            }
        } UNION {
            ?vocab ext:mappingShape ?mappingShape.
            {
                ?mappingShape a shacl:NodeShape ;
                    mu:uuid ?msUuid;
                    shacl:targetClass ?msTargetClass .
            } UNION {      
                ?mappingShape shacl:property ?msPropertyShape .
                ?msPropertyShape a shacl:PropertyShape;
                    mu:uuid ?psUuid;
                    shacl:path ?psPath;
                    shacl:description ?psDescription .    
            }
        }                       
  }
} 
                        """)
    the_query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        vocab_uris=" ".join([sparql_escape_uri(uri) for uri in vocab_uris]),
    )
    return the_query_string

def vocab_uris_from_graph(graph):
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
SELECT DISTINCT ?uri
WHERE {
    GRAPH $graph {
        ?uri a ext:VocabularyMeta .
    }
}
""")
    query = query_template.substitute(
        graph=sparql_escape_uri(graph),
    )
    return query

def matching_aliasses_in_graph(graph_matching, graph_orig=DATA_GRAPH):
    query_template = Template("""
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
SELECT DISTINCT (?vocab_matching as ?vocab) ?alias
WHERE {
    GRAPH $graph_matching {
        ?vocab_matching a ext:VocabularyMeta;
                    dbo:alias ?alias .
    }
    FILTER EXISTS {
        GRAPH $graph_orig {
            ?vocab_orig a ext:VocabularyMeta;
                    dbo:alias ?alias .
        }
    }
}
""")
    query = query_template.substitute(
        graph_matching=sparql_escape_uri(graph_matching),
        graph_orig=sparql_escape_uri(graph_orig),
    )
    return query

def matching_uris_in_graph(graph_matching, graph_orig=DATA_GRAPH):
    query_template = Template("""
PREFIX  ext:  <http://mu.semte.ch/vocabularies/ext/>
SELECT DISTINCT  ?uri ?type
WHERE { 
  GRAPH $graph_matching { 
    ?uri  a  ?type }
      FILTER EXISTS { GRAPH $graph_orig
            { ?uri a ?type }
        }
}
""")
    query = query_template.substitute(
        graph_matching=sparql_escape_uri(graph_matching),
        graph_orig=sparql_escape_uri(graph_orig),
    )
    return query

def matching_vocab_uris_in_graph(graph_matching, graph_orig=DATA_GRAPH):
    query_template = Template("""
PREFIX  ext:  <http://mu.semte.ch/vocabularies/ext/>
SELECT DISTINCT  ?uri
WHERE { 
  GRAPH $graph_matching { 
    ?uri  a  ext:VocabularyMeta }
      FILTER EXISTS { GRAPH $graph_orig
            { ?uri a ext:VocabularyMeta }
        }
}
""")
    query = query_template.substitute(
        graph_matching=sparql_escape_uri(graph_matching),
        graph_orig=sparql_escape_uri(graph_orig),
    )
    return query


def matching_dataset_uris_in_graph(graph_matching, graph_orig=DATA_GRAPH):
    query_template = Template("""
PREFIX  ext:  <http://mu.semte.ch/vocabularies/ext/>
SELECT DISTINCT ?uri
WHERE { 
  GRAPH $graph_matching { 
    ?vocab a   ext:VocabularyMeta .
    ?vocab ext:sourceDataset ?uri. }
  FILTER EXISTS { 
    GRAPH $graph_orig { 
      ?vocab a   ext:VocabularyMeta .
      ?vocab ext:sourceDataset ?uri. }
  }
}
""")
    query = query_template.substitute(
        graph_matching=sparql_escape_uri(graph_matching),
        graph_orig=sparql_escape_uri(graph_orig),
    )
    return query

def datasets_of_vocab(vocab_uri, graph=DATA_GRAPH):
    query_template = Template("""
PREFIX  ext:  <http://mu.semte.ch/vocabularies/ext/>
PREFIX dcterms: <http://purl.org/dc/terms/>
SELECT DISTINCT ?uri ?data_type
WHERE { 
  GRAPH $graph { 
    $vocab_uri a   ext:VocabularyMeta .
    $vocab_uri ext:sourceDataset ?uri. 
    ?uri dcterms:type ?data_type .
  }
}
""")
    query = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_uri=sparql_escape_uri(vocab_uri)
    )
    return query

def remove_aliases_vocabs(vocab_aliases, graph):
    query_template = Template("""
        PREFIX dbo: <http://dbpedia.org/ontology/>
        DELETE {
          GRAPH $graph {
            ?vocab dbo:alias ?alias .
          }  
        } WHERE {
          GRAPH $graph {
            VALUES (?vocab ?alias) {$vocab_and_alias_list}
            ?vocab dbo:alias ?alias .
          }
        }
        """)
    delete_query = query_template.substitute(
        graph=sparql_escape_uri(graph),
        vocab_and_alias_list=" ".join(
            ["(" + sparql_escape_uri(vocab) + " " + sparql_escape_string(alias) + ")" for (vocab, alias) in vocab_aliases]
        ),
    )
    return delete_query


def replace_with_uniq_uuid(uri, new_uri, new_uuid, graph):
    query_template = Template("""
        PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
        DELETE {
          GRAPH $graph {
            ?uri ?p ?s .
            ?o ?p2 ?uri .
            ?uri mu:uuid ?uuid .
          }
        }
        INSERT {
          GRAPH $graph {
            ?newUri ?p ?s .
            ?o ?p2 ?newUri .
            ?newUri mu:uuid ?newUuid .
          }
        }
        WHERE {
          GRAPH $graph {
            VALUES (?uri ?newUri ?newUuid) {($uri $new_uri $new_uuid)}
            { ?uri ?p ?s . 
              FILTER NOT EXISTS { ?uri mu:uuid ?s . }
            }
            UNION
            {?o ?p2 ?uri .}
            UNION 
            { ?uri mu:uuid ?uuid .}
          }
        }
        """)
    # in the above query `FILTER NOT EXISTS { ?uri mu:uuid ?s . }` is done so the uuid
    # is not part of the `?uri ?p ?s` solution (which are all added, as the uuid has to be overwritten.
    
    replace_query = query_template.substitute(
        graph=sparql_escape_uri(graph),
        uri=sparql_escape_uri(uri),
        new_uri=sparql_escape_uri(new_uri),
        new_uuid=sparql_escape_string(new_uuid),
    )
    return replace_query
