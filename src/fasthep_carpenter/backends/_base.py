
from dataclasses import dataclass
import itertools
from typing import Any, Dict, List, Protocol, Tuple

from fasthep_logging import get_logger, TRACE

from ..protocols import DataImportPlugin, ProcessingStep, Collector, InputData

log = get_logger("FASTHEP::Carpenter")
log.setLevel(TRACE)

class CollectionWrapper:
    dataset: str
    sequence: Dict[str, ProcessingStep]
    final: bool = False

    def __init__(self, dataset: str, sequence: Dict[str, ProcessingStep], final: bool = False) -> None:
        self.dataset = dataset
        self.sequence = sequence
        self.final = final

    def __get_relevant_tasks(self, previous_results) -> List[ProcessingStep]:
        local_tasks = []
        for results in previous_results:
            position_in_queue = 0
            if self.dataset == "all":
                print(results)
            for result in results:
                if self.dataset == "all":
                    log.trace("Found a task %s", result)
                if hasattr(result, "event"):
                    if self.dataset == "all":
                        print("  --> passed event check")
                    local_tasks.append((result, position_in_queue))
                    position_in_queue += 1
        return local_tasks

    def __get_collectors(
        self, previous_results
    ) -> Dict[str, Tuple[Collector, int]]:
        collectors: dict[str, tuple[Collector, int]] = {}
        for i, (name, step) in enumerate(self.sequence.items()):
            if not hasattr(step, "collector"):
                continue
            collectors[name] = (step.collector(), i, step)
        return collectors

    def __call__(self, previous_results):
        local_tasks = self.__get_relevant_tasks(previous_results)
        collectors = self.__get_collectors(previous_results)
        log.debug("Found {} local tasks".format(len(local_tasks)))
        log.debug("Found {} collectors".format(len(collectors)))

        output = {}
        for name, (collector, position, step) in collectors.items():
            tasks_to_collect = []
            for task, position_in_queue in local_tasks:
                if position_in_queue == position:
                    tasks_to_collect.append((self.dataset, (task,)))
                    if hasattr(step, "merge"):
                        log.debug("Found a step with merge %s", step)
                        step.merge(task)
            print(f"{self.dataset}: tasks to collect: {tasks_to_collect}")

            # Problem: this does write the output files instead of just collecting them
            # Solution: split the collection into two steps:
            # 1. merge all related steps
            # 2. collect the results
            # 3. write the output files

            output[name] = collector.collect(tasks_to_collect, writeFiles=self.final)

        # print(previous_results, results)
        return previous_results, output

class Workflow:
    # TODO: to be moved to fast-flow
    # workflow = fast_flow.from_carpenter_config(carpenter_config)
    # workflow = fast_flow.from_gitlab_ci_config(".gitlab-ci.yml")
    data_import_prefix = "data_import"
    collector_prefix = "collect_results"
    final_task_name = "__reduce__"
    sequence: Dict[str, ProcessingStep]

    def __init__(self, sequence):
        self.sequence = sequence
        self.task_graph = self.__create_task_graph()
        self.__task_graph = self.task_graph.copy()
        self.final_task = None

    def __create_task_graph(self):
        """Transforms fast-carpenter task list to Dask-like task graph.
        e.g.
        {
            'BasicVars': <fast_carpenter.define.variables.Define object at 0x7fca26d5c640>,
            'DiMuons': <cms_hep_tutorial.DiObjectMass object at 0x7fca26a881c0>,
            'NumberMuons': <fast_carpenter.summary.binned_dataframe.BinnedDataframe object at 0x7fca26d5c6d0>,
            'EventSelection': <fast_carpenter.selection.stage.CutFlow object at 0x7fca0bfd32b0>,
            'DiMuonMass': <fast_carpenter.summary.binned_dataframe.BinnedDataframe object at 0x7fca0b770dc0>
        }
        -->
        {
            'BasicVars': (<fast_carpenter.define.variables.Define object at 0x7fca26d5c640>),
            'DiMuons': (<cms_hep_tutorial.DiObjectMass object at 0x7fca26a881c0>, 'BasicVars'),
            'NumberMuons': (<fast_carpenter.summary.binned_dataframe.BinnedDataframe object at 0x7fca26d5c6d0>, 'DiMuons'),
            'EventSelection': (<fast_carpenter.selection.stage.CutFlow object at 0x7fca0bfd32b0>, 'NumberMuons'),
            'DiMuonMass': (<fast_carpenter.summary.binned_dataframe.BinnedDataframe object at 0x7fca0b770dc0>, 'EventSelection')
        }
        """
        task_graph = {}
        previous = None

        for task_name, task in self.sequence.items():
            if previous is not None:
                task_graph[task_name] = (task, previous)
            else:
                task_graph[task_name] = (task, "data_import")
            previous = task_name

        def collector_placeholder(previous_results):
            return previous_results

        task_graph[self.collector_prefix] = (collector_placeholder, previous)
        return task_graph

    def add_data_stage(
        self, data_import_plugin: DataImportPlugin, input_data: InputData
    ) -> None:
        """Adds data stage to the workflow. One node is generated for each file in each dataset"""
        task_graph = {}
        for dataset in input_data.datasets:
            dataset_suffix = dataset.name
            data_import_plugin.add_dataset_info(dataset.name, dataset.eventtype)
            for i, file_name in enumerate(dataset.files):
                file_suffix = f"file-{i}"
                task_suffix = f"{dataset_suffix}-{file_suffix}"
                task_graph[f"{self.data_import_prefix}-{task_suffix}"] = (
                    data_import_plugin.open,
                    [file_name],
                )
                for task, (payload, previous) in self.__task_graph.items():
                    if previous == self.data_import_prefix:
                        previous = f"{self.data_import_prefix}-{task_suffix}"
                    else:
                        previous = f"{previous}-{task_suffix}"
                    task_graph[f"{task}-{task_suffix}"] = (
                        payload,
                        previous,
                    )
        self.task_graph = task_graph

        return self

    def add_collector(self) -> None:
        # def collect(previous_results):
        #     """Collects results for a specific dataset"""
        #     # take the first result from each sublist
        #     data = [results[0] for results in previous_results]
        #     datasets = [data.config.dataset.name for data in data]
        #     print(datasets)
        #     # local_tasks = [result for results in previous_results for result in results if hasattr(result, "event")]
        #     local_tasks = []
        #     for results in previous_results:
        #         position_in_queue = 0
        #         for result in results:
        #             data = results[0]
        #             dataset = data.config.dataset.name
        #             if hasattr(result, "event"):
        #                 local_tasks.append((result, position_in_queue, dataset))
        #                 position_in_queue += 1
        #     # for step in local_tasks:
        #     #     if hasattr(step, "selection"):
        #     #         print(f"{step.name} has selection")
        #     #         selection = step.selection
        #     #         if selection is not None:
        #     #             print(f"{step=}")
        #     #             print(selection.to_dataframe())

        #     collectors: dict[str, tuple[ResultsCollector, int]] = {}
        #     for i, (name, step) in enumerate(self.sequence.items()):
        #         if not hasattr(step, "collector"):
        #             continue
        #         collectors[name] = (step.collector(), i)

        #     output = {}
        #     for name, (collector, position) in collectors.items():
        #         tasks_to_collect = []
        #         for task, position_in_queue, dataset in local_tasks:
        #             if position_in_queue == position:
        #                 tasks_to_collect.append((dataset, (task,)))
        #         print(f"tasks to collect:")
        #         for dataset, tasks in tasks_to_collect:
        #             print(f"{dataset}: {tasks}")
        #         # output[name] = collector.collect(tasks_to_collect)

        #     # print(previous_results, results)
        #     return previous_results, output

        collection_tasks = [
            task
            for task in self.task_graph.keys()
            if task.startswith(self.collector_prefix)
        ]
        # group collection tasks by dataset into a tuple of (dataset, list of collection tasks)
        grouped_tasks = itertools.groupby(
            collection_tasks, lambda task: task.split("-")[1]
        )
        dataset_collectors = []
        for dataset, tasks in grouped_tasks:
            # print(dataset)
            new_task = f"{self.collector_prefix}-{dataset}"
            collect = CollectionWrapper(dataset, self.sequence)
            self.task_graph[new_task] = (collect, list(tasks))
            dataset_collectors.append(new_task)
        # def postprocess(previous_results):
        #     results = {}
        #     print("Running postprocess", self.sequence)
        #     collectors: dict[str, ResultsCollector] = {}
        #     mergeable_stages = {}
        #     for i, (name, step) in enumerate(self.sequence.items()):
        #         print(f"Collecting results from step {i}: {step}")
        #         if hasattr(step, "merge"):
        #             mergeable_stages[name] = step
        #         if not hasattr(step, "collector"):
        #             continue
        #         collectors[name] = step.collector()
        #     #     if collector is None:
        #     #         collector =  step.collector()
        #     # TODO: each collector should collect from the previous results?
        #     # collectors expect tuples of (dataset, results)
        #     # this line looks like it is collecting horizontally for each dataset
        #     # output = collector.collect([(d, (s[i],)) for d, s in self.sequence.items()])
        #     # results[step.name] = output
        #     print("Found collectors:", collectors)
        #     print("Found mergeable stages:", mergeable_stages)
        #     return results
        # this task has only access to one file stream.
        # we need another level that groups datasets by dataset name

        def final_collector(inputs: List[Any]) -> List[Any]:
            # TODO: merge results from all datasets
            # TODO: if there are more than X results, merge in multiple steps
            # TODO: output files should be communicated here
            # for results in inputs:
            #     for result in results:
            #         print(result)
            collector = CollectionWrapper("all", self.sequence, final=True)
            # flatten inputs
            flat_inputs = [item for sublist in inputs for item in sublist]
            log.debug("Trying to merge results %s", flat_inputs)
            return collector(flat_inputs)
            return flat_inputs

        final_task = final_collector

        self.task_graph[self.final_task_name] = (final_task, dataset_collectors)
        self.final_task = final_task
        return self

    def to_image(self, output_file: str) -> None:
        import dask
        from dask.delayed import Delayed

        delayed_dsk = Delayed("w", self.task_graph)
        dask.visualize(delayed_dsk, filename=output_file, verbose=True)

    def to_gitlab_ci_yaml(self, output_file: str) -> None:
        pass

    def to_parsl(self) -> None:
        # 1. turn all data-import tasks into python apps
        # 2. turn all other tasks into parsl tasks
        # add final task and construct parsl graph
        pass

    def to_dask(self) -> None:
        pass

    def __repr__(self) -> str:
        output = ""
        for task, description in self.task_graph.items():
            output += f"{description[1]} -> {task} \n"
        return output
