"""
Chop up those trees into nice little tables and dataframes
"""
from __future__ import print_function
import fast_flow.v1 as fast_flow
import fast_curator
import logging
import atuproot.atuproot_main as atup
from .event_builder import EventBuilder
from atsge.build_parallel import build_parallel
from .utils import mkdir_p
atup.EventBuilder = EventBuilder
atup.build_parallel = build_parallel
logging.getLogger(__name__).setLevel(logging.INFO)


class DummyCollector():
    def collect(self, *args, **kwargs):
        pass


def process_args(args=None):
    from argparse import ArgumentParser
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("dataset_cfg", type=str,
                        help="Dataset config to run over")
    parser.add_argument("sequence_cfg", type=str,
                        help="Config for how to process events")
    parser.add_argument("--outdir", default="output", type=str,
                        help="Where to save the results")
    parser.add_argument("--mode", default="multiprocessing", type=str,
                        help="Which mode to run in (multiprocessing, htcondor, sge)")
    parser.add_argument("--ncores", default=0, type=int,
                        help="Number of cores to run on")
    parser.add_argument("--nblocks-per-dataset", default=-1, type=int,
                        help="Number of blocks per dataset")
    parser.add_argument("--nblocks-per-sample", default=-1, type=int,
                        help="Number of blocks per sample")
    parser.add_argument("--blocksize", default=1000000, type=int,
                        help="Number of events per block")
    parser.add_argument("--quiet", default=False, action='store_true',
                        help="Keep progress report quiet")
    parser.add_argument("--profile", default=False, action='store_true',
                        help="Profile the code")
    return parser.parse_args()


def main(args=None):
    args = process_args(args)

    sequence = fast_flow.read_sequence_yaml(args.sequence_cfg, output_dir=args.outdir)

    datasets = fast_curator.read.from_yaml(args.dataset_cfg)

    mkdir_p(args.outdir)

    process = atup.AtUproot(args.outdir,
                            quiet=args.quiet,
                            parallel_mode=args.mode,
                            process=args.ncores,
                            max_blocks_per_dataset=args.nblocks_per_dataset,
                            max_blocks_per_process=args.nblocks_per_sample,
                            nevents_per_block=args.blocksize,
                            profile=args.profile,
                            profile_out_path="profile.txt",
                            )

    sequence = [(s, s.collector() if hasattr(s, "collector") else DummyCollector()) for s in sequence]
    return process.run(datasets, sequence)


if __name__ == "__main__":
    main()
