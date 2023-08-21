from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd
import os
from multiprocessing import Process
import json
import docker
client = docker.from_env()


def log_stats(container, log_path):
    with open(log_path + 'stats.jsonl', 'w') as f:
        for line in client.api.stats(container, stream=True, decode=True):
            f.write(json.dumps(line) + '\n')


def cpu_percentage_system(d):
    user_delta = \
        d['cpu_stats']['cpu_usage']['total_usage'] - \
        d['precpu_stats']['cpu_usage']['total_usage']
    system_delta = \
        d['cpu_stats']['system_cpu_usage'] - \
        d['precpu_stats']['system_cpu_usage']

    cpu_percentage = user_delta / system_delta * 100
    return cpu_percentage


def cpu_percentage_allocated(d, allocated_nano_cpus=4 * 10**9):
    return (d['cpu_stats']['cpu_usage']['total_usage'] -
            d['precpu_stats']['cpu_usage']['total_usage']) / (allocated_nano_cpus) * 100


def visualise_results(cpu: pd.Series, memory: pd.Series, datetime: pd.Series,
                      event_timestamps: pd.Series, event_labels: pd.Series
                      ) -> go.Figure:

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(
        x=datetime,
        y=cpu,
        name="cpu %"),
        secondary_y=False)

    fig.add_trace(go.Scatter(
        x=datetime,
        y=memory,
        name="Memory (gb)"),
        secondary_y=True)

    fig.add_trace(go.Scatter(
        x=event_timestamps,
        y=event_timestamps.apply(lambda y: 0),
        name="Events", mode='markers',
        hovertemplate='%{text}',
        text=event_labels),
        secondary_y=False)

    fig.update_yaxes(title_text="cpu %", secondary_y=False)
    fig.update_yaxes(title_text="Memory (gb)", secondary_y=True)

    fig.update_layout(
        hovermode="x unified", legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5))

    return fig
