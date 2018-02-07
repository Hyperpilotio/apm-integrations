"""
This file shows how to retrieve metrics from Graphite. It contains two basic functions,
listing metrics and retrieving metrics.
"""

import requests
import json

GRAPHITE_ADDRESS = "localhost:80"


def list_all_metrics():
    """
    List all available metrics in Graphite.
    return ["stats.foo", "stats.example_metrics"]
    """
    r = requests.get("http://{}/metrics/index.json".format(GRAPHITE_ADDRESS))
    global METRICS_LIST
    return r.json()


def retrieve_metrics():
    """
    The structure of metrics object would be as follow:
    metrics['stats.foo'] = [{'datapoints': [[None, 1517881260],
   [None, 1517881800],
   [0.0, 1517881860],
   [0.0, 1517881920],
   [0.049999999999999996, 1517881980],
  'tags': {'name': 'stats.foo'},
  'target': 'stats.foo'}]
    """
    metric_list = list_metrics()
    metrics = {}

    for m in metric_list:
        metrics[m] = requests.get(
            "http://{}/render?target={}&format=json".format(
                GRAPHITE_ADDRESS, m)).json()

    return metrics


if __name__ == "__main__":
    print(retrieve_metrics())
