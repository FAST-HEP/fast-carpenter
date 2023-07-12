from tkinter import E
import awkward as ak
import numpy as np
import pytest

from fasthep_carpenter.protocols import ProcessingStepResult, DataMapping
from fasthep_carpenter.stages import Define
from fasthep_carpenter.testing import execute_tasks
from fasthep_carpenter.expressions import register_function

from fasthep_carpenter.workflow import reset_task_counters

VarDefinitions = list[dict[str, str]]

@pytest.fixture(autouse=True)
def clear_task_counters():
    reset_task_counters()


@pytest.fixture
def simple_definition() -> VarDefinitions:
    return [{"simple_var": "Muon_Py**2"}]


@pytest.fixture
def multi_definition() -> VarDefinitions:
    return [
        {"Muon_Pt": "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"},
        {"IsoMuon_Idx": "(Muon_Iso / Muon_Pt) < 0.10"},
    ]


@pytest.fixture
def definition_with_function() -> VarDefinitions:
    return [
        {"Muon_Pt": "sqrt(Muon_Px ** 2 + Muon_Py ** 2)"},
        {"IsoMuon_Idx": "find_isolated_muons(Muon_Iso, Muon_Pt)"},
    ]


@pytest.fixture
def definition_with_slice() -> VarDefinitions:
    return [{
        "first_muon_py": "Muon_Py[:, 0]",
    }]


@pytest.fixture
def data_source(data_mapping_with_file: DataMapping):
    return ProcessingStepResult(
        data=data_mapping_with_file,
    )


@pytest.mark.asyncio
async def test_define_simple_var(simple_definition: VarDefinitions, data_source: ProcessingStepResult):
    stage = Define("test_simple_var", simple_definition)
    tasks = stage.tasks("test_data")
    # 1 eval, 1 add, 1 local data source
    assert len(tasks) == 3
    assert tasks.last_task == "define-test_simple_var-0-simple_var-0"

    tasks.prepend_task("test_data", lambda: data_source)

    result = execute_tasks(tasks)
    assert "simple_var" in result.data
    assert ak.almost_equal(result.data["simple_var"], (data_source.data["Muon_Py"] ** 2), rtol=1e-4)


@pytest.mark.asyncio
async def test_define_multi_var(multi_definition: VarDefinitions, data_source: ProcessingStepResult):
    stage = Define("test_define", multi_definition)
    tasks = stage.tasks("test_data")
    # two additions, two function calls, one evaluation, two local data sources
    assert len(tasks) == 7
    assert tasks.last_task == "define-test_define-1-IsoMuon_Idx-0"

    tasks.prepend_task("test_data", lambda: data_source)

    result = execute_tasks(tasks)
    assert "Muon_Pt" in result.data
    assert "IsoMuon_Idx" in result.data
    assert ak.almost_equal(result.data["Muon_Pt"], np.sqrt(
        data_source.data["Muon_Px"] ** 2 + data_source.data["Muon_Py"] ** 2), rtol=1e-4)
    assert ak.almost_equal(
        result.data["IsoMuon_Idx"],
        (data_source.data["Muon_Iso"] / result.data["Muon_Pt"]) < 0.10, rtol=1e-4)


@pytest.mark.asyncio
async def test_define_with_function(definition_with_function: VarDefinitions, data_source: ProcessingStepResult):
    stage = Define("test_define", definition_with_function)
    register_function("find_isolated_muons", lambda iso, pt: (iso / pt) < 0.10)
    tasks = stage.tasks("test_data")

    # two additions, two function calls, three evaluations, two local data sources
    assert len(tasks) == 9
    assert tasks.last_task == "define-test_define-1-IsoMuon_Idx-0"

    # 'eval-2': (<function apply at 0x7f60068ea7a0>, <function evaluate at 0x7f6006bec1f0>, ('Muon_Pt',), {'global_dict': '__data_source__define-test_define-0-Muon_Pt-0'}, '__data_source__define-test_define-0-Muon_Pt-0')
    # 'eval-1': (<function apply at 0x7fbb642827a0>, <function evaluate at 0x7fbb645841f0>, ('Muon_Iso / Muon_Pt < 0.1',), {'global_dict': '__data_source__define-test_define-0-Muon_Pt-0'}),

    tasks.prepend_task("test_data", lambda: data_source)
    from fasthep_carpenter.testing import visualize_tasks
    visualize_tasks(tasks, "test_define_with_function.png")
    result = execute_tasks(tasks)

    assert "Muon_Pt" in result.data
    assert "IsoMuon_Idx" in result.data
    assert ak.almost_equal(result.data["Muon_Pt"], np.sqrt(
        data_source.data["Muon_Px"] ** 2 + data_source.data["Muon_Py"] ** 2), rtol=1e-4)
    assert ak.almost_equal(
        result.data["IsoMuon_Idx"],
        (data_source.data["Muon_Iso"] / result.data["Muon_Pt"]) < 0.10, rtol=1e-4)


@pytest.mark.xfail
@pytest.mark.asyncio
async def test_define_with_slice(definition_with_slice: VarDefinitions, data_source: ProcessingStepResult):
    stage = Define("test_define", definition_with_slice)
    tasks = stage.tasks("test_data")
    # one addition, one evaluation
    assert len(tasks) == 2
    assert tasks.last_task == "define-test_define-0-first_muon_py-0"

    tasks.prepend_task("test_data", lambda: data_source)
    result = execute_tasks(tasks)

    assert "first_muon_py" in result
    assert ak.almost_equal(result["first_muon_py"], data_source.data["Muon_Py"][:, 0], rtol=1e-4)
