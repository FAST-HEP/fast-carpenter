import pytest

import fast_carpenter.define as define
import fast_carpenter.selection.stage as filters
import fast_carpenter.summary.binned_dataframe as binned_dataframe
import fast_carpenter.testing.dummy_binning_descriptions as binning
from fast_carpenter.backends import InputData, Workflow
from fast_carpenter.testing import DummyDataset


class DummyPlugin:
    dataset_name: str
    dataset_eventtype: str

    def __call__(self, *args, **kwargs):
        return True

    def open(self, paths):
        return paths

    def add_dataset_info(self, dataset_name, dataset_eventtype):
        self.dataset_name = dataset_name
        self.dataset_eventtype = dataset_eventtype


@pytest.fixture
def sequence():
    return [
        define.Define(
            name="Muon_pt",
            out_dir="somewhere",
            variables=[{"Muon_pt": "sqrt(Muon_px**2 + Muon_py**2)"}],
        ),
        filters.CutFlow(
            name="test_selection_1", out_dir="somewhere", selection="NMuon > 1"
        ),
        binned_dataframe.BinnedDataframe(
            name="binned_df_1",
            out_dir="somewhere",
            binning=[binning.bins_met_px, binning.bins_py],
            weights=binning.weight_list,
        ),
    ]


@pytest.fixture
def sequence_dict(sequence):
    return {s.name: s for s in sequence}


@pytest.fixture
def workflow(sequence_dict):
    return Workflow(sequence_dict)


def test_workflow_init(sequence_dict):
    workflow = Workflow(sequence_dict)
    assert workflow.sequence == sequence_dict
    assert len(workflow.task_graph) == len(sequence_dict) + 1  # sequence + collector
    for name in sequence_dict:
        # workflow wraps each step in a task - the type should match the step
        assert isinstance(workflow.task_graph[name][0].task, type(sequence_dict[name]))
        # the wrapped task should be a deepcopy of the step
        assert workflow.task_graph[name][0].task is not sequence_dict[name]


@pytest.fixture
def input_data():
    return InputData(
        [
            DummyDataset("data", "data", ["data.root"]),
            DummyDataset("mc", "mc", ["mc.root"]),
        ]
    )


def has_properly_cloned_tasks(workflow_under_test):
    for name, (task_info) in workflow_under_test.task_graph.items():
        step_name = None
        for step in workflow_under_test.sequence:
            if name.startswith(step):
                step_name = step
                break

        if step_name is None:
            continue
        # workflow wraps each step in a task - the type should match the step
        assert isinstance(
            task_info[0].task, type(workflow_under_test.sequence[step_name])
        )
        # assert type(task_info[0].task) == type(workflow_under_test.sequence[step_name])
        # the wrapped task should be a deepcopy of the step
        assert task_info[0].task is not workflow_under_test.sequence[step_name]


def test_data_stage(workflow, input_data):
    data_import_plugin = DummyPlugin()
    assert (
        len(workflow.task_graph) == len(workflow.sequence) + 1
    )  # sequence + collector
    workflow = workflow.add_data_stage(data_import_plugin, input_data)
    # sequence + collector + data import
    assert len(workflow.task_graph) == (len(workflow.sequence) + 1 + 1) * len(
        input_data.files_by_dataset
    )
    has_properly_cloned_tasks(workflow)


def test_add_collectors(workflow, input_data):
    data_import_plugin = DummyPlugin()
    workflow = workflow.add_data_stage(data_import_plugin, input_data)
    length_before = len(workflow.task_graph)
    assert length_before == (len(workflow.sequence) + 1 + 1) * len(
        input_data.files_by_dataset
    )
    workflow = workflow.add_collector()
    # adds one collector per dataset and one collector for the whole workflow
    assert len(workflow.task_graph) == length_before + 1 + len(input_data.datasets)
    has_properly_cloned_tasks(workflow)
