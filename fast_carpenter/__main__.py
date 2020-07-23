"""
Chop up those trees into nice little tables and dataframes
"""
from __future__ import print_function
import sys
from .help import help_stages
import fast_flow.v1 as fast_flow
import fast_curator
import logging
from .backends import get_backend
from .utils import mkdir_p
from .version import __version__
logging.getLogger(__name__).setLevel(logging.INFO)


def create_parser():
    from argparse import ArgumentParser, Action

    class StagesHelp(Action):
        def __call__(self, parser, namespace, values, option_string=None):
            full_output = option_string == "--help-stages-full"
            help_stages(values, full_output=full_output)
            sys.exit(0)

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
    parser.add_argument("--help-stages", nargs="?", default=None, action=StagesHelp,
                        metavar="stage-name-regex",
                        help="Print help specific to the available stages")
    parser.add_argument("--help-stages-full", action=StagesHelp, metavar="stage",
                        help="Print the full help specific to the available stages")
    parser.add_argument("-v", "--version", action="version", version='%(prog)s ' + __version__)

    return parser


def main(args=None):
    args = create_parser().parse_args(args)

    sequence = fast_flow.read_sequence_yaml(args.sequence_cfg, output_dir=args.outdir, backend="fast_carpenter")
    datasets = fast_curator.read.from_yaml(args.dataset_cfg)
    backend = get_backend(args.mode)

    mkdir_p(args.outdir)
    results, _ = backend.execute(sequence, datasets, args)

    print("Summary of results")
    print(results)
    print("Output written to directory '%s'" % args.outdir)
    return 0


if __name__ == "__main__":
    main()
