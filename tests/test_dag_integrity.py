import os
import sys
import pytest
from airflow.models import DagBag
sys.path.append(os.path.join(os.path.dirname(__file__), "../dags"))
sys.path.append(os.path.join(os.path.dirname(__file__), "../dags/utilities"))


@pytest.fixture(params=["..src/dags/benchmark_dataset"])
def dag_bag(request):
    return DagBag(dag_folder=request.param, include_examples=False)


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors


def test_requires_tags(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags
