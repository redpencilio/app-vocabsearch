import os
from escape_helpers import sparql_escape_uri
from string import Template
from datetime import datetime
from file import file_to_shared_uri, shared_uri_to_path
from file import construct_get_file_query, construct_insert_file_query
from more_itertools import batched
from format_to_mime import FORMAT_TO_MIME_EXT
from helpers import generate_uuid, logger
from helpers import query, update
from sudo_query import update_sudo
import requests
BATCH_SIZE = 100

FILE_RESOURCE_BASE = "http://example-resource.com/"
MU_VIRTUOSO_ENDPOINT = os.environ.get("MU_VIRTUOSO_ENDPOINT")

# adapted from https://github.com/RDFLib/rdflib/issues/1704
def serialize_graph_to_sparql(g, graph_name: str):
    """RDFlib graph to sparql"""
    for triples_batch in batched(g.triples((None, None, None)), BATCH_SIZE):
        updatequery = "\n".join(
            [f"PREFIX {prefix}: {ns.n3()}" for prefix, ns in g.namespaces()]
        )
        updatequery += f"\nINSERT DATA {{\n\tGRAPH {sparql_escape_uri(graph_name)} {{\n"
        updatequery += " .\n".join(
            [f"\t\t{s.n3()} {p.n3()} {o.n3()}" for (s, p, o) in triples_batch]
        )
        updatequery += f" . \n\t }}\n}}\n"
        yield updatequery

dump_graph_query_template = Template("""
CONSTRUCT {
    ?s ?p ?o .
}
WHERE
{
    {
        SELECT DISTINCT ?s ?p ?o
        FROM $graph
        WHERE
        {
            ?s ?p ?o .
        }
        ORDER BY ASC(?s)
    }
}
OFFSET $offset
LIMIT $limit
""")

def graph_to_file(graph_name, graph):
    """triplestore graph to file"""
    mime_type = "text/plain"
    file_extension = "nt"
    upload_resource_uuid = generate_uuid()
    upload_resource_uri = f"{FILE_RESOURCE_BASE}{upload_resource_uuid}"
    file_resource_uuid = generate_uuid()
    file_resource_name = f"{file_resource_uuid}.{file_extension}"

    file_resource_uri = file_to_shared_uri(file_resource_name)

    headers = {"Accept": mime_type}

    i = 0
    logger.info(f"Starting graph {graph_name} dump to {shared_uri_to_path(file_resource_uri)}.")
    with open(shared_uri_to_path(file_resource_uri), "wb") as f:
        while True:
            logger.debug(f"Writing triplestore graph to file. Batch {i+1}")
            query_string = dump_graph_query_template.substitute(
                graph=sparql_escape_uri(graph_name),
                offset=i*BATCH_SIZE,
                limit=BATCH_SIZE
            )
            url = MU_VIRTUOSO_ENDPOINT + '?query=' + query_string
            with requests.get(url, headers=headers, stream=True) as res:
                assert res.ok
                if res.content == b'# Empty NT\n':
                    break
                for chunk in res.iter_content(chunk_size=None):
                    f.write(chunk)
                i += 1

        f.seek(0, 2)
        file_size = f.tell()

    file = {
        "uri": upload_resource_uri,
        "uuid": upload_resource_uuid,
        "name": file_resource_name,
        "mimetype": "text/plain",
        "created": datetime.now(),
        "size": file_size,
        "extension": file_extension,
    }
    physical_file = {
        "uri": file_resource_uri,
        "uuid": file_resource_uuid,
        "name": file_resource_name,
    }

    query_string = construct_insert_file_query(file, physical_file, graph)

    # TODO Check query result before writing file to disk
    update_sudo(query_string)

    return upload_resource_uri

def binding_results(json_result, binded_values):
    bindings = []
    for binding in json_result["results"]["bindings"]:
        if isinstance(binded_values, tuple):
            values = tuple(
                (binding[key]["value"] if key in binding else None)
                for key in binded_values
            )
        else:
            values = binding[binded_values]["value"]
        bindings.append(values)
    return bindings
