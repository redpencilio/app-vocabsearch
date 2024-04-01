import os
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH")
NEW_SUBJECT_BASE = "http://example-resource.com/dataset-subject/"

def get_property_paths(node_shape, metadata_graph):
    query_template = Template("""
PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?sourceClass ?sourcePathString ?destClass ?destPath
WHERE {
    GRAPH $metadata_graph {
        $node_shape
            a sh:NodeShape ;
            sh:targetClass ?sourceClass ;
            sh:property ?propertyShape .
        ?propertyShape
            a sh:PropertyShape ;
            sh:description ?destPath ;
            sh:path ?sourcePathString .
        BIND(skos:Concept as ?destClass)
    }
}
""")
    query_string = query_template.substitute(
        metadata_graph=sparql_escape_uri(metadata_graph),
        node_shape=sparql_escape_uri(node_shape),
    )
    return query_string


# Unification works as follows:
# the Subject from the source is taken based on the provided class (Pivot Type)
# and predicate path (Label/Tag path) given by the user
# this source is added to every provided dataset with a constructed URI (?internalSubject)
# that is unique per vocab (?vocabUri) and dataset subject (?sourceSubject)
# prov:wasDerivedFrom connects the original dataset's URI
# Note that this constructed URI is only needed for internal use:
# this avoids reusing URIs for the same dataset subjects over multiple vocabs (which might use the same dataset source)
# however, the user is only interested in the actual dataset uri (via prov:wasDerivedFrom)

# A unified entity is connected to a vocab via one (or more) datasets, but in reality a unified entity
# is part of a vocabulary (a vocab has one "unified dataset"), not part of "multiple" datasets. 
# As long as a concept is connected to one dataset of the vocab, the search will find it back.
def get_ununified_batch(dest_class,
                        dest_predicate,
                        source_datasets,
                        source_class,
                        source_path_string,
                        source_graph,
                        target_graph,
                        batch_size):
    query_template = Template("""
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

CONSTRUCT {
      ?internalSubject
          prov:wasDerivedFrom ?sourceSubject ;
          a $dest_class ;
          $dest_predicate ?sourceValue ;
          dct:source ?sourceDataset .
}
WHERE {
    VALUES ?sourceDataset {
        $source_datasets
    }
    ?vocabUri ext:sourceDataset ?sourceDataset .
    
    FILTER NOT EXISTS {
        GRAPH $target_graph {
            ?targetSubject 
                prov:wasDerivedFrom ?sourceSubject ;
                a $dest_class ;
                $dest_predicate ?sourceValue .
        }
    }
    GRAPH $source_graph {
        ?sourceSubject
            a $source_class ;
            $source_path_string ?sourceValue .
    }
    BIND(IRI(CONCAT($new_subject_uri_base, MD5(CONCAT(str(?vocabUri), str(?sourceSubject))))) as ?internalSubject)
}
LIMIT $batch_size
""")
    query_string = query_template.substitute(
        dest_class=sparql_escape_uri(dest_class),
        dest_predicate=sparql_escape_uri(dest_predicate),
        source_datasets="\n         ".join([sparql_escape_uri(source_dataset) for source_dataset in source_datasets]),
        source_class=sparql_escape_uri(source_class),
        source_path_string=source_path_string,  # !this is already formatted as a sparql predicate path by the frontend. 
        source_graph=sparql_escape_uri(source_graph),
        target_graph=sparql_escape_uri(target_graph),
        batch_size=batch_size,
        new_subject_uri_base=sparql_escape_string(NEW_SUBJECT_BASE)
    )
    return query_string


# delete the subjects provided that are part of a dataset
# note that these are related to the subject uris in our internal app via prov:wasDerivedFrom,
# see `get_ununified_batch` function for details
def delete_dataset_subjects_from_graph(subjects, graph):
    query_template = Template("""
PREFIX prov: <http://www.w3.org/ns/prov#>
WITH $graph
DELETE {
    ?internalSubject prov:wasDerivedFrom ?datasetSubject .
    ?internalSubject ?p ?o .
}
WHERE {
    ?internalSubject prov:wasDerivedFrom ?datasetSubject .
    ?internalSubject ?p ?o .
    VALUES ?datasetSubject {
        $subjects
    }
}
""")
    query_string = query_template.substitute(
        subjects="\n        ".join([sparql_escape_uri(s) for s in subjects]),
        graph=sparql_escape_uri(graph),
    )
    return query_string
