from neo4j import GraphDatabase
from src.graph_database_stress_testing.utilities.configuration import get_config


def format_load_nodes_query_string(nodetype, config):

    properties = config['properties']
    properties.update({'id': 'id'})

    properties_string = ', '.join([f'{k}: row.{k}' for k in properties])
    node_label_string = nodetype.capitalize()

    return f"""load csv with headers from 'file:///{nodetype}_nodes.csv' as row
    create (p:{node_label_string} {{{properties_string}}})"""


def format_load_edges_query_string(edge_type, config):

    properties = config['properties']
    source_label = config['source'].capitalize()
    target_label = config['target'].capitalize()

    properties_string = ', '.join([f'{k}: row.{k}' for k in properties])
    edge_type_string = edge_type.upper()

    return f"""load csv with headers from 'file:///{edge_type}_edges.csv' as row
    match (s:{source_label} {{id: row.source}}), (t:{target_label} {{id: row.target}})
    create (s)-[:{edge_type_string} {{{properties_string}}}]->(t)"""


def load_dataset(config_path):

    config = get_config(config_path)
    faker_config = get_config(config['data']['faker_conf'])

    uri = config['neo4j']['uri']
    user = config['neo4j']['user']
    password = config['neo4j']['password']

    with GraphDatabase.driver(uri, auth=(user, password)).session() as session:
        for nodetype, node_config in faker_config['nodes'].items():
            query_string = \
                format_load_nodes_query_string(nodetype, node_config)
            session.run(query_string)

        for edge_type, edge_config in faker_config['edges'].items():
            query_string = \
                format_load_edges_query_string(edge_type, edge_config)
            session.run(query_string)
