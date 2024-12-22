import json
import logging
import os
import time
from typing import Dict

import boto3
import neo4j

from cartography.intel.jit_analysis.graph_analysis_queries import QUERIES
from cartography.intel.jit_analysis.util import datetime_serializer

logger = logging.getLogger(__name__)


def extract(new4j_session: neo4j.Session) -> Dict:
    """
    Extract the graph analysis data from the graph
    """
    logger.info("Extracting Graph Analysis data from Neo4J")
    graph_analysis = {}
    for key, value in QUERIES.items():
        logger.info(f"Executing query: {value['name']}")
        query = value["query"]

        for i in range(3):  # Retry logic
            try:
                result = new4j_session.run(query)
                records = result.data()
                if len(records) == 1:
                    records = records[0]
                    keys = list(records.keys())
                    if len(keys) == 1:
                        records = records[keys[0]]

                logger.info(f"Query {value['name']} executed successfully")
                graph_analysis[key] = records
                if records is not None:
                    break
            except Exception:
                logger.error(f"Error executing query {value['name']}. Retrying...")
                time.sleep(5)

    return graph_analysis


def transform(graph_analysis: Dict) -> Dict:
    """
    Transform the graph analysis data, Will be implemented in future
    """
    return graph_analysis


def load(graph_analysis: Dict) -> None:
    logger.info("Uploading Graph Analysis to S3")
    env_name = os.environ['JIT_ENV_NAME']
    tenant_id = os.environ['TENANT_ID']
    bucket_name = f"jit-kg-{env_name}"
    key = f"{tenant_id}/internal_analysis/graph_analysis.json"
    logger.info(f"Uploading to S3 bucket: {bucket_name}, key: {key}")
    body = json.dumps(graph_analysis, indent=4, default=datetime_serializer)
    boto3.client("s3").put_object(Bucket=bucket_name, Key=key, Body=body)
    logger.info("Uploaded Graph Analysis to S3")


def sync(neo4j_session: neo4j.Session) -> None:
    """
    Performs the sequential tasks to extract data from the graph and upload it to S3
    :param neo4j_session: Neo4J session for database interface
    :return: Nothing
    """
    logger.info("Syncing Graph Analysis to S3")
    graph_analysis = extract(neo4j_session)
    transformed_graph_analysis = transform(graph_analysis)
    load(transformed_graph_analysis)
    logger.info("Synced Graph Analysis to S3")
