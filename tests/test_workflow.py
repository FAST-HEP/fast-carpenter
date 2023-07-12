from collections import defaultdict
from importlib import reload
import pytest

from fasthep_carpenter.workflow import get_task_number, reset_task_counters, TaskCollection


@pytest.fixture(autouse=True)
def clear_task_counters():
    reset_task_counters()


def test_single_task():
    task_name = "task1"
    assert get_task_number(task_name) == 0
    assert get_task_number(task_name) == 1


def test_multiple_tasks():
    task_name1 = "task1"
    task_name2 = "task2"
    assert get_task_number(task_name1) == 0
    assert get_task_number(task_name2) == 0
    assert get_task_number(task_name1) == 1
    assert get_task_number(task_name2) == 1

def test_task_collection():
    task_collection = TaskCollection()
    assert task_collection.first_task is None
    assert task_collection.last_task is None
    task_collection.add_task("task1", "task1", 1)
    assert task_collection.first_task == "task1"
    assert task_collection.last_task == "task1"
    task_collection.add_task("task2", "task2", 2)
    assert task_collection.first_task == "task1"
    assert task_collection.last_task == "task2"
    assert task_collection.graph == {"task1": ("task1", 1), "task2": ("task2", 2)}
    assert repr(task_collection) == "TaskCollection({'task1': ('task1', 1), 'task2': ('task2', 2)}, first=task1, last=task2)"