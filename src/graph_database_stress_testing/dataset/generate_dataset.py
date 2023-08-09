from faker import Faker
import pandas as pd
import yaml


def get_config(config_path):
    with open(config_path) as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config


def _generate_properties(properties: dict, n: int, faker: Faker):
    return [{k: getattr(faker, v)() for k, v in properties.items()}
            for _ in range(n)]


def _generate_nodes_edges(faker_config: dict, seed=0):

    fake = Faker()
    Faker.seed(seed)

    nodes = {}
    for node_type, node_config in faker_config['nodes'].items():

        properties = _generate_properties(
            node_config['properties'], node_config['n'], fake)

        df = pd.DataFrame(properties)
        df['label'] = node_type
        df['id'] = df['label'] + df.index.astype(str)

        nodes[node_type] = df

    edges = {}
    for edge_type, edge_config in faker_config['edges'].items():

        source = nodes[edge_config['source']]
        target = nodes[edge_config['target']]

        source_id = source['id'].sample(
            edge_config['n'], replace=True).reset_index(drop=True)
        target_id = target['id'].sample(
            edge_config['n'], replace=True).reset_index(drop=True)

        properties = _generate_properties(
            edge_config['properties'], node_config['n'], fake)
        
        df = pd.DataFrame(properties)
        df['source'] = source_id
        df['target'] = target_id
        df['type'] = edge_type


        edges[edge_type] = df

    return nodes, edges


def generate_dataset(config_path, seed=0):
    config = get_config(config_path)
    faker_config = get_config(config['data']['faker_conf'])
    data_path = config['data']['path']
    nodes, edges = _generate_nodes_edges(faker_config, seed)

    [df.to_csv(f'{data_path}{node_type}_nodes.csv', index=False)
     for node_type, df in nodes.items()]
    [df.to_csv(f'{data_path}{edge_type}_edges.csv', index=False)
     for edge_type, df in edges.items()]
