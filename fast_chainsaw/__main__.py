"""
Summarize them trees into nice little dataframe
"""
from __future__ import print_function
from fast_flow.v1 import read_sequence_yaml
import logging
logging.getLogger(__name__).setLevel(logging.INFO)


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
                        help="Which mode to run in (multiprocessing, htcondor, "
                             "sge)")
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

def run(sequence, datasets, options):
    from atuproot.AtUproot import AtUproot
    process = AtUproot(options.outdir,
        quiet = options.quiet,
        parallel_mode = options.mode,
        process = options.ncores,
        max_blocks_per_dataset = options.nblocks_per_dataset,
        max_blocks_per_process = options.nblocks_per_sample,
        blocksize = options.blocksize,
        profile = options.profile,
        profile_out_path = "profile.txt",
    )
    return process.run(datasets, sequence)
    

def main(args=None):
    args = process_args(args)

    sequence = read_sequence_yaml(sequence_cfg)

    


if __name__ == "__main__":
    main()
