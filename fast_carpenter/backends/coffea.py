"""
Functions to run a job using Coffea
"""
import copy
from fast_carpenter.masked_tree import MaskedUprootTree
from collections import namedtuple
from coffea import processor as cop
import logging


EventRanger = namedtuple("EventRanger", "start_entry stop_entry entries_in_block")
SingleChunk = namedtuple("SingleChunk", "tree config")
ChunkConfig = namedtuple("ChunkConfig", "dataset")
ConfigProxy = namedtuple("ConfigProxy", "name eventtype")


class stages_accumulator(cop.AccumulatorABC):
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


class FASTProcessor(cop.ProcessorABC):
    def __init__(self, sequence):

        self._columns = list()
        self._sequence = sequence
        accumulator_dict = {'stages': cop.dict_accumulator({})}
        self._accumulator = cop.dict_accumulator(accumulator_dict)

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


def load_execution_cfg(config):
    if not isinstance(config, str):
        return config

    import yaml
    with open(config, "r") as infile:
        cfg = yaml.safe_load(infile)
        return cfg


def configure_parsl(n_threads, monitoring, **kwargs):
    from parsl.config import Config
    from parsl.executors.threads import ThreadPoolExecutor
    from parsl.addresses import address_by_hostname

    if monitoring:
        from parsl.monitoring import MonitoringHub
        monitoring = MonitoringHub(hub_address=address_by_hostname(),
                                   hub_port=55055,
                                   logging_level=logging.INFO,
                                   resource_monitoring_interval=10,
                                   )
    else:
        monitoring = None

    local_threads = ThreadPoolExecutor(max_threads=n_threads,
                                       label='local_threads')
    config = Config(executors=[local_threads],
                    monitoring=monitoring,
                    strategy=None,
                    app_cache=True,
                    )
    return config


def configure_dask(client, **kwargs):
    from dask.distributed import Client

    if client:
        return Client(client)
    else:
        return Client(**kwargs)


def create_executor(args):
    exe_type = args.mode.split(":", 1)[-1].lower()
    exe_args = {}
    if getattr(args, "execution_cfg", None):
        exe_args = load_execution_cfg(args.execution_cfg)

    if exe_type == "local":
        executor = cop.futures_executor
        exe_args.setdefault('workers', args.ncores)
        exe_args.setdefault('flatten', False)
    elif exe_type == "parsl":
        executor = cop.parsl_executor
        exe_args.setdefault('n_threads', args.ncores)
        exe_args.setdefault('monitoring', False)
        if 'config' not in exe_args:
            exe_args['config'] = configure_parsl(**exe_args)
        exe_args.setdefault('flatten', False)
    elif exe_type == "dask":
        executor = cop.dask_executor
        exe_args.setdefault('processes', False)
        exe_args.setdefault('threads_per_worker', 2)
        exe_args.setdefault('n_workers', 2)
        exe_args.setdefault('memory_limit', '1GB')
        exe_args.setdefault('client', None)
        exe_args['client'] = configure_dask(**exe_args)
    else:
        msg = "Coffea executor not yet included in fast-carpenter: '%s'"
        raise NotImplementedError(msg % exe_type)

    return executor, exe_args


def execute(sequence, datasets, args):
    fp = FASTProcessor(sequence)

    executor, exe_args = create_executor(args)

    coffea_datasets = {}
    for ds in datasets:
        coffea_datasets[ds.name] = ds.__dict__.copy()
        coffea_datasets[ds.name].pop('name')
        coffea_datasets[ds.name]['treename'] = coffea_datasets[ds.name].pop('tree')

    maxchunks = args.nblocks_per_dataset
    if maxchunks < 1:
        maxchunks = None
    out = cop.run_uproot_job(coffea_datasets, 'events', fp,
                             executor, executor_args=exe_args,
                             chunksize=args.blocksize, maxchunks=maxchunks)

    return out["stages"], out["results"]
