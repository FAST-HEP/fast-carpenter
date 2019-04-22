import inspect
import re
from . import known_stages


class StageGuidanceHelper:
    _common_config = ["name", "out_dir", "self"]

    def __init__(self, stage_class, module_name):
        self._stage = stage_class
        self.module_name = module_name

    @property
    def stage(self):
        return self._stage

    @property
    def class_name(self):
        return self.module_name + "." + self.stage.__name__

    def matches(self, regex):
        if regex:
            return re.match(regex, self.class_name)
        return True

    def parameters(self):
        args, vargs, kwargs, defaults = inspect.getargspec(self.stage.__init__)
        args = [a for a in args if a not in self._common_config]
        if defaults:
            ndefs = min(len(defaults), len(args))
            def_args = ["{}={}".format(a, d) for a, d in zip(args[-ndefs:], defaults[-ndefs:])]
            args = args[:-ndefs] + def_args
        if vargs:
            args.append("*" + vargs)
        if kwargs:
            args.append("**" + kwargs)
        return args

    def docstring(self):
        doc = inspect.getdoc(self.stage)
        if doc:
            return doc
        return "<Missing docstring>"


all_stages = tuple(StageGuidanceHelper(s, "fast_carpenter") for s in known_stages)


def help_stages(stage_name, full_output=False):
    stages = all_stages
    if stage_name and stage_name.lower() != "all":
        stages = tuple(s for s in stages if s.matches(stage_name))
        if not stages:
            raise RuntimeError("Unknown stage:", stage_name)

    for i, stage in enumerate(stages):
        name = stage.class_name
        args = ", ".join(stage.parameters())
        header = "{name}\n   config: ({args})".format(name=name, args=args)
        print(header)
        if full_output:
            print(stage.docstring())
        if i != len(stages) - 1:
            end_stage = "-------\n" if full_output else ""
            print(end_stage)
