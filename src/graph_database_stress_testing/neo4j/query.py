from neo4j import GraphDatabase, AsyncGraphDatabase
from src.graph_database_stress_testing.utilities.configuration import get_config
from multiprocessing import Pool


def query(uri, user, password, query_string):
    with GraphDatabase.driver(uri, auth=(user, password)).session(database='neo4j') as session:
        result = session.run(query_string)
        records = [record.data() for record in result]
    return records


def concurrent_queries(uri, user, password, query_string, n_processes=4):
    with Pool(n_processes) as pool:
        response = pool.starmap(
            query, [(uri, user, password, query_string)] * n_processes)
    return response
