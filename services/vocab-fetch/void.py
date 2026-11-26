from string import Template
from helpers import query, update
from helpers import generate_uuid, logger
from sudo_query import update_sudo
from escape_helpers import sparql_escape, sparql_escape_uri, sparql_escape_string

VOID_GRAPH_URI_BASE = "http://example.com/graph/"

# Queries adapted from https://github.com/cygri/make-void by Richard Cygniak

def generate_insert_property_partitions(dataset_contents_g, dataset, graph):
    property_partition_base = str(dataset) + "_property"
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>

INSERT {
    GRAPH $void_graph {
        $dataset
            void:propertyPartition ?pp .
        ?pp a void:Dataset;
            void:property ?p;
            void:triples ?triples;
            void:distinctSubjects ?subjects;
            void:distinctObjects ?objects .
    }
}
WHERE {
  {
    SELECT (COUNT(*) AS ?triples) (COUNT(DISTINCT ?s) AS ?subjects) (COUNT(DISTINCT ?o) AS ?objects) ?p
    WHERE {
        GRAPH $dataset_contents_graph {
            ?s ?p ?o .
        }
    }
    GROUP BY ?p
  }
  BIND(STRUUID() AS ?uuid)
  BIND(URI(CONCAT($property_partition_base, ?uuid)) AS ?pp)
}
""")
    query_string = query_template.substitute(
        dataset_contents_graph=sparql_escape_uri(dataset_contents_g),
        dataset=sparql_escape_uri(dataset),
        property_partition_base=sparql_escape_string(property_partition_base),
        void_graph=sparql_escape_uri(graph)
    )
    update_sudo(query_string)
    return graph, dataset


def generate_insert_class_partitions(dataset_contents_g, dataset, graph):
    class_partition_base = str(dataset) + "_class"
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>

INSERT {
    GRAPH $void_graph {
        $dataset
            void:classPartition ?cp .
        ?cp a void:Dataset;
            void:class ?type;
            void:entities ?instances .
    }
}
WHERE {
    {
        SELECT (COUNT(DISTINCT ?instance) AS ?instances) ?type
        WHERE {
            GRAPH $dataset_contents_graph {
                ?instance a ?type .
                FILTER (isURI(?type))
            }
        }
        GROUP BY ?type
    }
    BIND(STRUUID() AS ?uuid)
    BIND(URI(CONCAT($class_partition_base, ?uuid)) AS ?cp)
}
""")
    query_string = query_template.substitute(
        dataset_contents_graph=sparql_escape_uri(dataset_contents_g),
        dataset=sparql_escape_uri(dataset),
        class_partition_base=sparql_escape_string(class_partition_base),
        void_graph=sparql_escape_uri(graph)
    )
    update_sudo(query_string)
    return graph, dataset

def generate_insert_summary(dataset_contents_g, dataset, graph):
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>

INSERT {
    GRAPH $void_graph {
        $dataset
            void:triples ?triples;
            void:entities ?entities;
            void:classes ?classes;
            void:properties ?properties.
    }
}
WHERE {
    {
        SELECT (COUNT(*) AS ?triples) (COUNT(DISTINCT ?p) AS ?properties)
        WHERE {
            GRAPH $dataset_contents_graph {
                ?s ?p ?o .
            }
        }
    }
    {
        SELECT (COUNT(DISTINCT ?type) AS ?classes)
        WHERE {
            GRAPH $dataset_contents_graph {
                [] a ?type .
                FILTER (isURI(?type))
            }
        }
    }
    {
        SELECT (COUNT(DISTINCT ?s) AS ?entities)
        WHERE {
            GRAPH $dataset_contents_graph {
                ?s ?p ?o .
            }
        }
    }
}
""")
    query_string = query_template.substitute(
        dataset_contents_graph=sparql_escape_uri(dataset_contents_g),
        dataset=sparql_escape_uri(dataset),
        void_graph=sparql_escape_uri(graph)
    )
    update_sudo(query_string)
    return graph, dataset

def generateVoID(dataset_contents_g, dataset, graph):
    generate_insert_class_partitions(dataset_contents_g, dataset, graph)
    generate_insert_property_partitions(dataset_contents_g, dataset, graph)
    generate_insert_summary(dataset_contents_g, dataset, graph)
    return graph, dataset

def generate_delete_class_partitions(dataset, graph):
    # Note the missing delete of dataset rdf type triple
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>

DELETE {
    GRAPH $void_graph {
        $dataset void:classPartition ?cp .
        ?cp ?cpp ?cpo .
    }
}
WHERE {
    GRAPH $void_graph {
        $dataset a void:Dataset;
            void:classPartition ?cp .
        ?cp ?cpp ?cpo .
    }
}
""")
    query_string = query_template.substitute(
        dataset=sparql_escape_uri(dataset),
        void_graph=sparql_escape_uri(graph)
    )
    update_sudo(query_string)
    return graph, dataset

def generate_delete_property_partitions(dataset, graph):
    # Note the missing delete of dataset rdf type triple
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>

DELETE {
    GRAPH $void_graph {
        $dataset void:propertyPartition ?pp .
        ?pp ?ppp ?ppo .
    }
}
WHERE {
    GRAPH $void_graph {
        $dataset a void:Dataset;
            void:propertyPartition ?pp .
        ?pp ?ppp ?ppo .
    }
}
""")
    query_string = query_template.substitute(
        dataset=sparql_escape_uri(dataset),
        void_graph=sparql_escape_uri(graph)
    )
    update_sudo(query_string)
    return graph, dataset


def generate_delete_summary(dataset, graph):
    # Note the missing delete of dataset rdf type triple
    query_template = Template("""
PREFIX void: <http://rdfs.org/ns/void#>

INSERT {
    GRAPH $void_graph {
        $dataset
            void:triples ?triples;
            void:entities ?entities;
            void:classes ?classes;
            void:properties ?properties.
    }
}
WHERE {
    GRAPH $void_graph {
        $dataset a void:Dataset;
            void:triples ?triples;
            void:entities ?entities;
            void:classes ?classes;
            void:properties ?properties.
    }
}
""")
    query_string = query_template.substitute(
        dataset=sparql_escape_uri(dataset),
        void_graph=sparql_escape_uri(graph)
    )
    update_sudo(query_string)
    return graph, dataset

def deleteVoID(dataset, graph):
    generate_delete_class_partitions(dataset, graph)
    generate_delete_property_partitions(dataset, graph)
    generate_delete_summary(dataset, graph)
    return graph, dataset
