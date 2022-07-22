"""
Functions to run a job using alphatwirl
"""
import operator
from functools import partial
from typing import Any, Dict

import awkward as awk
import numpy as np
from cachetools import cachedmethod
from cachetools.keys import hashkey

from fast_carpenter.data_import import DataImportPlugin
from fast_carpenter.tree_adapter import create_masked


class BEvents:
    """
    from https://github.com/shane-breeze/atuproot/blob/master/atuproot/BEvents.py
    """

    non_branch_attrs = [
        "tree",
        "nevents_in_tree",
        "nevents_per_block",
        "nblocks",
        "start_block",
        "stop_block",
        "iblock",
        "start_entry",
        "stop_entry",
        "_branch_cache",
        "_nonbranch_cache",
        "size",
        "config",
    ]

    def __init__(
        self,
        tree,
        nevents_per_block=100000,
        start_block=0,
        stop_block=-1,
        branch_cache=None,
    ):
        self.tree = tree
        self.nevents_in_tree = len(tree)
        self.nevents_per_block = (
            int(nevents_per_block) if nevents_per_block >= 0 else self.nevents_in_tree
        )

        nblocks = int((self.nevents_in_tree - 1) / self.nevents_per_block + 1)
        start_block = min(nblocks, start_block)
        if stop_block > -1:
            self.nblocks = min(nblocks - start_block, stop_block)
        else:
            self.nblocks = nblocks - start_block
        self.stop_block = stop_block
        self.start_block = start_block
        self.iblock = -1

        self._branch_cache = {} if branch_cache is None else branch_cache
        self._nonbranch_cache = {}

    def __len__(self):
        return self.nblocks

    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__,
            self._repr_content(),
        )

    def _repr_content(self):
        return (
            "tree = {!r}, nevents_in_tree = {!r}, nevents_per_block = {!r}, "
            "nblocks = {!r}, iblock = {!r}, start_block = {!r}, "
            "stop_block = {!r}".format(
                self.tree,
                self.nevents_in_tree,
                self.nevents_per_block,
                self.nblocks,
                self.iblock,
                self.start_block,
                self.stop_block,
            )
        )

    def __getitem__(self, i):
        if i >= self.nblocks:
            self.iblock = -1
            raise IndexError("The index is out of range: " + str(i))
        self._branch_cache.clear()

        self.iblock = i
        return self

    def __iter__(self):
        for self.iblock in range(self.nblocks):
            self._branch_cache.clear()
            yield self
        self.iblock = -1
        self._nonbranch_cache = {}

    def __getattr__(self, attr):
        if attr in self.non_branch_attrs:
            return getattr(self, attr)
        elif attr in self._nonbranch_cache:
            return self._nonbranch_cache[attr]
        return self._get_branch(attr)

    def __setattr__(self, attr, val):
        if attr in self.non_branch_attrs:
            super().__setattr__(attr, val)
        else:
            if not (isinstance(val, awk.Array) or isinstance(val, np.ndarray)):
                self._nonbranch_cache[attr] = val
            else:
                key = hashkey("BEvents._get_branch", attr)
                self._branch_cache[key] = val

    @cachedmethod(
        operator.attrgetter("_branch_cache"),
        key=partial(hashkey, "BEvents._get_branch"),
    )
    def _get_branch(self, name):
        self.start_entry = (self.start_block + self.iblock) * self.nevents_per_block
        self.stop_entry = min(
            (self.start_block + self.iblock + 1) * self.nevents_per_block,
            (self.start_block + self.nblocks) * self.nevents_per_block,
            self.nevents_in_tree,
        )
        self.size = self.stop_entry - self.start_entry
        try:
            branch = "asdsd"
            print(branch)
            # branch = self.tree.array(
            #     name,
            #     entrystart=self.start_entry,
            #     entrystop=self.stop_entry,
            # )
        except KeyError as e:
            raise AttributeError(e)
        return branch

    def hasbranch(self, branch, encoding="utf-8"):
        return (
            branch.encode(encoding) in self.tree.keys()
            or hashkey("BEvents._get_branch", branch) in self._branch_cache
            or branch in self._nonbranch_cache
        )

    def delete_branches(self, branches):
        for branch in branches:
            key = hashkey("BEvents._get_branch", branch)
            if key in self._branch_cache:
                self._branch_cache.popitem(key)
            elif branch in self._nonbranch_cache:
                self._nonbranch_cache.popitem(branch)

    def __contains__(self, branch):
        return branch in self.tree


class EventRanger:
    def __init__(self):
        self._owner = None

    def set_owner(self, owner):
        self._owner = owner

    @property
    def start_entry(self):
        return (
            self._owner.start_block + self._owner.iblock
        ) * self._owner.nevents_per_block

    @property
    def stop_entry(self):
        i_block = min(self._owner.iblock + 1, self._owner.nblocks)
        stop_entry = (self._owner.start_block + i_block) * self._owner.nevents_per_block
        return min(self._owner.nevents_in_tree, stop_entry)

    @property
    def entries_in_block(self):
        if self._owner and self._owner.iblock > -1:
            return self.stop_entry - self.start_entry
        return None


class BEventsWrapped(BEvents):
    def __init__(self, tree, *args, **kwargs):
        ranges = EventRanger()

        super().__init__(tree, *args, **kwargs)
        ranges.set_owner(self)
        tree = create_masked(
            {
                "tree": tree,
                "start": self.start_entry,
                "stop": self.stop_entry,
                "adapter": "uproot4",
            }
        )
        self.tree = tree

    def _block_changed(self):
        self.tree.reset_mask()
        self.tree.reset_cache()

    def __getitem__(self, i):
        result = super().__getitem__(self, i)
        self._block_changed()
        return result

    def __iter__(self):
        for value in super().__iter__():
            self._block_changed()
            yield value
        self._block_changed()

    @property
    def start_entry(self):
        return (self.start_block + self.iblock) * self.nevents_per_block

    @property
    def stop_entry(self):
        i_block = min(self.iblock + 1, self.nblocks)
        stop_entry = (self.start_block + i_block) * self.nevents_per_block
        return min(self.nevents_in_tree, stop_entry)


class EventBuilder:
    data_import_plugin: DataImportPlugin = None

    def __init__(self, config):
        self.config = config

    def __repr__(self):
        return "{}({!r})".format(
            self.__class__.__name__,
            self.config,
        )

    def __call__(self):
        data = EventBuilder.data_import_plugin.open(self.config.inputPaths)
        tree = data[self.config.treeName]

        events = BEventsWrapped(
            tree,
            self.config.nevents_per_block,
            self.config.start_block,
            self.config.stop_block,
        )
        events.config = self.config
        return events


class DummyCollector:
    def collect(self, *args, **kwargs):
        pass


class AtuprootContext:
    def __init__(self, plugins: Dict[str, Any] = None) -> None:
        self.plugins = plugins

    def __enter__(self):
        import atuproot.atuproot_main as atup

        self.atup = atup
        self._orig_event_builder = atup.EventBuilder
        self._orig_build_parallel = atup.build_parallel

        from atsge.build_parallel import build_parallel

        atup.EventBuilder = EventBuilder
        atup.EventBuilder.data_import_plugin = self.plugins["data_import"]
        atup.build_parallel = build_parallel
        return self

    def __exit__(self, *args, **kwargs):
        self.atup.EventBuilder = self._orig_event_builder
        self.atup.build_parallel = self._orig_build_parallel


def execute(sequence, datasets, args, plugins: Dict[str, Any] = None):
    """
    Run a job using alphatwirl and atuproot
    """

    if args.ncores < 1:
        args.ncores = 1

    sequence = [
        (s, s.collector() if hasattr(s, "collector") else DummyCollector())
        for s in sequence
    ]

    with AtuprootContext(plugins) as runner:
        process = runner.atup.AtUproot(
            args.outdir,
            quiet=args.quiet,
            parallel_mode=args.mode,
            process=args.ncores,
            max_blocks_per_dataset=args.nblocks_per_dataset,
            max_blocks_per_process=args.nblocks_per_sample,
            nevents_per_block=args.blocksize,
            profile=args.profile,
            profile_out_path="profile.txt",
        )

        ret_val = process.run(datasets, sequence)

    if not args.profile:
        # This breaks in AlphaTwirl when used with the profile option
        summary = {
            s[0].name: list(df.index.names)
            for s, df in zip(sequence, ret_val[0])
            if df is not None
        }
    else:
        summary = " (Results summary not available with profile mode) "

    return summary, ret_val
