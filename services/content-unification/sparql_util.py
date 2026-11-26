from escape_helpers import sparql_escape_uri
from helpers import logger, generate_uuid, query

from sudo_query import direct_update_triplestore
from file import construct_get_file_query, shared_uri_to_path

import os
from string import Template
from more_itertools import batched
from rdflib.graph import Graph
from rdflib.term import URIRef, Literal
from constants import MU_APPLICATION_GRAPH

TEMP_GRAPH_BASE = 'http://example-resource.com/graph/'
BATCH_SIZE = 100

# adapted from https://github.com/RDFLib/rdflib/issues/1704
def serialize_graph_to_sparql(g, graph_name: str, operation="INSERT"):
    # Note that the Graph triples method yields triples in random order
    # Triples aren't grouped by subject. Current mu-search delta handling
    # isn't affected by this.
    for triples_batch in batched(g.triples((None, None, None)), BATCH_SIZE):
        # updatequery = "\n".join([f"PREFIX {prefix}: {ns.n3()}" for prefix, ns in g.namespaces()])
        # Dropping prefixes boosts performance. Since the '.n3()'-method produces ntriples compliant triples,
        # dropping the prefixes yields an ntriples only query, which is significantly faster
        updatequery = f"\n{operation} DATA {{\n\tGRAPH {sparql_escape_uri(graph_name)} {{\n"
        updatequery += " .\n".join([f"\t\t{s.n3()} {p.n3()} {o.n3()}" for (s, p, o) in triples_batch])
        updatequery += f" . \n\t }}\n}}\n"
        yield updatequery

def json_to_term(json_term):
    if json_term['type'] == 'uri':
        return URIRef(json_term['value'])
    else:
        if 'xml:lang' in json_term:  # SELECT
            lang = json_term['xml:lang']
            return Literal(json_term['value'], lang=lang)
        elif 'lang' in json_term:  # CONSTRUCT
            lang = json_term['lang']
            return Literal(json_term['value'], lang=lang)
        else:
            if 'datatype' in json_term:
                return Literal(json_term['value'], datatype=json_term['datatype'])
            else:
                return Literal(json_term['value'])
        

def sparql_construct_res_to_graph(res):
    """ Turn results of a sparql construct query into an rdflib graph """
    g = Graph()
    for binding in res['results']['bindings']:
        s = URIRef(binding['s']['value'])
        p = URIRef(binding['p']['value'])
        o = json_to_term(binding['o'])
        g.add((s, p, o))
    return g

def binding_results(json_result, binded_values):
  bindings = []
  for binding in json_result["results"]["bindings"]:
      if isinstance(binded_values, tuple):
        values = tuple(binding[key]["value"] for key in binded_values)
      else:
        values = binding[binded_values]["value"]  
      bindings.append(values)
  return bindings


def copy_graph_to_temp(graph, temp_named_graph=None):
    query_string = Template("""
INSERT {
    GRAPH $new_graph {?s ?p ?o .}
}
WHERE {
    GRAPH $old_graph {?s ?p ?o .}
}
    """).substitute(
        old_graph=sparql_escape_uri(graph),
        new_graph=sparql_escape_uri(temp_named_graph),
    )
    direct_update_triplestore(query_string)
    return temp_named_graph

def upload_file_to_graph(file, graph):
    g = Graph()
    g.parse(file)
    for query_string in serialize_graph_to_sparql(g, graph):
        direct_update_triplestore(query_string)

def load_file_to_db(uri: str, metadata_graph: str = MU_APPLICATION_GRAPH, temp_named_graph=None):
    if not temp_named_graph:
        temp_named_graph = TEMP_GRAPH_BASE + generate_uuid()
    query_string = construct_get_file_query(uri, metadata_graph)
    file_result = query(query_string, sudo=True)['results']['bindings'][0]
    upload_file_to_graph(shared_uri_to_path(file_result['physicalFile']['value']), temp_named_graph)
    return temp_named_graph

def drop_graph(graph):
    direct_update_triplestore("DROP SILENT GRAPH {}".format(sparql_escape_uri(graph)))

def diff_graphs(graph_old, graph_new):
    query_res = query(Template("""
SELECT DISTINCT ?s
WHERE {
    GRAPH $new_graph {
        ?s ?p ?o .
    }
    FILTER NOT EXISTS {
        GRAPH $old_graph {
            ?s ?p ?o .
        }
    }
}
""").substitute(
    old_graph=sparql_escape_uri(graph_old),
    new_graph=sparql_escape_uri(graph_new),
), sudo=True)
    s = [b['s']['value'] for b in query_res['results']['bindings']]
    return s
