import datetime
import os
import time

from SPARQLWrapper import SPARQLWrapper, JSON
from helpers import logger, update

sparqlQueryDirectUpdate = SPARQLWrapper(os.environ.get("MU_SPARQL_DIRECT_UPDATEPOINT"), returnFormat= JSON)
sparqlQueryDirectUpdate.method = "POST"

def direct_update_triplestore(the_query, attempt=0, max_retries=5):
    """Execute the given update SPARQL query directly on the triple store,
    bypassing sparql-parser. If the given query is no update query, nothing happens.
    Retries the update with an exponential backoff in the case of failure."""
    sparqlQueryDirectUpdate.setQuery(the_query)
    if sparqlQueryDirectUpdate.isSparqlUpdateRequest():
        try:
            start = time.time()
            logger.debug(f"started direct update at {datetime.datetime.now()}")
            logger.debug("execute direct update: \n" + the_query)

            sparqlQueryDirectUpdate.query()

            logger.debug(f"direct update took {time.time() - start} seconds")
        except Exception as e:
            logger.error("Direct update query failed: \n" + the_query)
            logger.error(f"Direct update query error: {str(e)}")
            if attempt <= max_retries:
                wait_time = 0.6 * attempt + 30
                logger.warn(f"Retrying after {wait_time} seconds [{attempt}/{max_retries}]")
                time.sleep(wait_time)

                direct_update_triplestore(the_query, attempt + 1, max_retries)
            else:
                logger.error("Max attempts reached for direct update. Skipping.")
                raise e

def update_sudo(the_query, attempt=0, max_retries=5):
    """Execute the given update SPARQL query on the triple store,
    if the given query is no update query, nothing happens.
    Retries the update with an exponential backoff in the case of failure."""
    try:
        start = time.time()
        logger.debug(f"started update at {datetime.datetime.now()}")
        logger.debug("execute update: \n" + the_query)

        update(the_query, sudo=True)

        logger.debug(f"query took {time.time() - start} seconds")
    except Exception as e:
        logger.error("Update query failed: \n" + the_query)
        logger.error(f"Update query error: {str(e)}")
        if attempt <= max_retries:
            wait_time = 0.6 * attempt + 30
            logger.warn(f"Retrying after {wait_time} seconds [{attempt}/{max_retries}]")
            time.sleep(wait_time)

            update_sudo(the_query, attempt + 1, max_retries)
        else:
            logger.error("Max attempts reached for update. Skipping.")
            raise e
