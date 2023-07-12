import os
import sys
from pathlib import Path

import rich
import typer

app = typer.Typer()


def run(dataset_cfg: Path, sequence_cfg: Path, output_dir: str, processing_backend: str, store_bookkeeping: bool):
    """Entry point for running fasthep_carpenter from python

    Args:
        dataset_cfg (Path): Dataset config to run over
        sequence_cfg (Path): Config for how to process dataset
        output_dir (str): Where to save the results
        processing_backend (str): Backend to use for processing
        store_bookkeeping (bool): Store bookkeeping information
    """
    try:
        import fast_curator
        import fast_flow.v1

        from . import backends, bookkeeping, data_import
        from .settings import Settings
        from .utils import mkdir_p
        from .workflow import Workflow
    except ImportError as e:
        rich.print("[red]Failed to import required package:[/red]", e)
        sys.exit(1)

    # prepare output directory
    mkdir_p(output_dir)

    # read datasets and processing sequence
    datasets = fast_curator.read.from_yaml(dataset_cfg)
    sequence, sequence_cfg = fast_flow.v1.read_sequence_yaml(
        sequence_cfg,
        output_dir=output_dir,
        backend="fasthep_carpenter",
        return_cfg=True,
    )
    # store bookkeeping information if requested
    if store_bookkeeping:
        book_keeping_file = os.path.join(output_dir, "book-keeping.tar.gz")
        bookkeeping.write_booking(
            book_keeping_file, sequence_cfg, datasets, cmd_line_args=sys.argv[1:]
        )

    # load settings
    settings = Settings(
        ncores=1,
        outdir=output_dir,
        plugins={
            "data_import": data_import.get_data_import_plugin("uproot4", None)
        },
    )
    # extract workflow
    workflow = Workflow(sequence, datasets, settings)
    # get backend
    backend = backends.get_backend(processing_backend)
    # execute workflow
    results = backend.execute(workflow, settings)

    # generate report and summary
    # TODO

    # print summary of results (details in processing_report.html)
    # rich.print(f"[blue]Results[/]: {results}")
    rich.print(f"[blue]Output written to directory {output_dir}[/]")


@app.command()
def main(
    dataset_cfg: Path = typer.Argument(
        ...,
        help="Dataset config to run over",
        exists=True,
        readable=True,
        resolve_path=True,
    ),
    sequence_cfg: Path = typer.Argument(
        ...,
        help="Config for how to process dataset",
        exists=True,
        readable=True,
    ),
    output_dir: str = typer.Option(
        "output", "--outdir", "-o", help="Where to save the results"
    ),
    processing_backend: str = typer.Option(
        "multiprocessing", "--backend", "-b", help="Backend to use for processing"
    ),
    store_bookkeeping: bool = typer.Option(
        True, "--store-bookkeeping", "-s", help="Store bookkeeping information"
    ),
):
    """Entry point for running fasthep_carpenter from command line"""
    run(dataset_cfg, sequence_cfg, output_dir, processing_backend, store_bookkeeping)


if __name__ == "__main__":
    typer.run(main)
