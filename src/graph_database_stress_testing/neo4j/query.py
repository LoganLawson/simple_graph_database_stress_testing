from neo4j import GraphDatabase, AsyncGraphDatabase
from src.graph_database_stress_testing.utilities.configuration import get_config
from multiprocessing import Pool


def query(uri, user, password, query_string):
    with GraphDatabase.driver(uri, auth=(user, password)) as driver:
        with driver.session(database='neo4j') as session:
            result = session.run(query_string)
            records = [record.data() for record in result]
    return records


async def concurrent_query(uri, user, password, query_string):
    async with AsyncGraphDatabase.driver(uri, auth=(user, password)) as driver:
        async with driver.session(database='neo4j') as session:
            result = await session.run(query_string)
            records = [record.data() async for record in result]
    return records
