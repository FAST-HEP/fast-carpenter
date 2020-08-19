"""
Chop up those trees into nice little tables and dataframes
"""
from __future__ import print_function
from . import known_stages
import os
import fast_flow.v1 as fast_flow
from fast_flow.help import argparse_help_stages
import fast_curator
import logging
from .backends import get_backend
from .utils import mkdir_p
from .bookkeeping import write_booking
from .version import __version__
logging.getLogger(__name__).setLevel(logging.INFO)


def create_parser():
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
    parser.add_argument("--ncores", default=1, type=int,
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
    parser.add_argument("--execution-cfg", "-e", default=None,
                        help="A configuration file for the execution system.  The exact format "
                             "and contents of this file will depend on the value of the `--mode` option.")
    parser.add_argument("--help-stages", metavar="stage-name-regex", nargs="?", default=None,
                        action=argparse_help_stages(known_stages, "fast_carpenter", full_output=False),
                        help="Print help specific to the available stages")
    parser.add_argument("--help-stages-full", metavar="stage",
                        action=argparse_help_stages(known_stages, "fast_carpenter", full_output=True),
                        help="Print the full help specific to the available stages")
    parser.add_argument("-v", "--version", action="version", version='%(prog)s ' + __version__)
    parser.add_argument("--bookkeeping", default=True, action='store_true',
                        help="Enable creation of book-keeping tarball")
    parser.add_argument("--no-bookkeeping", action='store_false', dest="bookkeeping",
                        help="Disable creation of book-keeping tarball")

    return parser


def main(args=None):
    args = create_parser().parse_args(args)

    sequence, seq_cfg = fast_flow.read_sequence_yaml(args.sequence_cfg, output_dir=args.outdir,
                                                     backend="fast_carpenter", return_cfg=True)
    datasets = fast_curator.read.from_yaml(args.dataset_cfg)
    backend = get_backend(args.mode)

    mkdir_p(args.outdir)
    if args.bookkeeping:
        book_keeping_file = os.path.join(args.outdir, "book-keeping.tar.gz")
        write_booking(book_keeping_file, seq_cfg, datasets, cmd_line_args=args)
    results, _ = backend.execute(sequence, datasets, args)

    print("Summary of results")
    print(results)
    print("Output written to directory '%s'" % args.outdir)
    return 0


if __name__ == "__main__":
    main()
