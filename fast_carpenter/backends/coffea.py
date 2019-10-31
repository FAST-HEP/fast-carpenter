"""
Functions to run a job using Coffea
"""
import copy
from fast_carpenter.masked_tree import MaskedUprootTree
from collections import namedtuple
from coffea import processor
from coffea.processor import futures_executor, run_uproot_job


EventRanger = namedtuple("EventRanger", "start_entry stop_entry entries_in_block")
SingleChunk = namedtuple("SingleChunk", "tree config")
ChunkConfig = namedtuple("ChunkConfig", "dataset")
ConfigProxy = namedtuple("ConfigProxy", "name eventtype")


class stages_accumulator(processor.AccumulatorABC):
    def __init__(self, stages):
        self._zero = copy.deepcopy(stages)
        self._value = copy.deepcopy(stages)
    
    def identity(self):
        return stages_accumulator(self._zero)
    
    def __getitem__(self, idx):
        return self._value[idx]
    
    def add(self, other):
        for i, stage in enumerate(self._value):
            if not hasattr(stage, "merge"):
                continue
            stage.merge(other[i])


class FASTProcessor(processor.ProcessorABC):
    def __init__(self, sequence):
        
        self._columns = list()
        self._sequence = sequence
        accumulator_dict = {'stages': processor.dict_accumulator({})}
        self._accumulator = processor.dict_accumulator(accumulator_dict)

    @property
    def columns(self):
        return self._columns
    
    @property
    def accumulator(self):
        return self._accumulator

    def process(self, df):
        output = self.accumulator.identity()
        
        start = df._branchargs['entrystart']
        stop = df._branchargs['entrystop']
        tree = MaskedUprootTree(df._tree, EventRanger(start, stop, stop - start))
        dsname = df['dataset']
        cfg_proxy = ConfigProxy(dsname, 'data' if dsname == 'data' else 'mc')
        chunk = SingleChunk(tree, ChunkConfig(cfg_proxy))
        
        output['stages'][dsname] = stages_accumulator(self._sequence)
        
        for work in output['stages'][dsname]._value:
            work.event(chunk)
        
        return output

    def postprocess(self, accumulator):
        stages = accumulator['stages']
        results = {}
        
        wf = copy.deepcopy(self._sequence)
        for i_step, step in enumerate(wf):
            if not hasattr(step, "collector"):
                continue        
            collector = step.collector()
            output = collector.collect([(d, (s[i_step],)) for d, s in stages.items()])
            results[step.name] = output
        
        accumulator['results'] = results
        
        return accumulator


def execute(sequence, datasets, args):
    fp = FASTProcessor(sequence)

    executor = futures_executor
    exe_args = {'workers': 4,
                'function_args': {'flatten': False}}

    coffea_datasets = {}
    for ds in datasets:
        coffea_datasets[ds.name] = ds.__dict__.copy()
        coffea_datasets[ds.name].pop('name')
        coffea_datasets[ds.name]['treename'] = coffea_datasets[ds.name].pop('tree')

    out = run_uproot_job(coffea_datasets, 'events', fp, executor, executor_args = exe_args)
    return out["stages"], out["results"]
