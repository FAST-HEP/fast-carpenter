"""
Functions to run a job using alphatwirl
"""


class DummyCollector():
    def collect(self, *args, **kwargs):
        pass


class AtuprootContext:
    def __enter__(self):
        import atuproot.atuproot_main as atup
        self.atup = atup
        self._orig_event_builder = atup.EventBuilder
        self._orig_build_parallel = atup.build_parallel

        from ..event_builder import EventBuilder
        from atsge.build_parallel import build_parallel
        atup.EventBuilder = EventBuilder
        atup.build_parallel = build_parallel
        return self

    def __exit__(self, *args, **kwargs):
        self.atup.EventBuilder = self._orig_event_builder
        self.atup.build_parallel = self._orig_build_parallel


def execute(sequence, datasets, args):
    """
    Run a job using alphatwirl and atuproot
    """

    if args.ncores < 1:
        args.ncores = 1

    sequence = [(s, s.collector() if hasattr(s, "collector") else DummyCollector()) for s in sequence]

    with AtuprootContext() as runner:
        process = runner.atup.AtUproot(args.outdir,
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
        summary = {s[0].name: list(df.index.names) for s, df in zip(sequence, ret_val[0]) if df is not None}
    else:
        summary = " (Results summary not available with profile mode) "

    return summary, ret_val
