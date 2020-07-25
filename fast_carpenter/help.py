import inspect
import sys
import re


class StageGuidanceHelper:
    _common_config = ["name", "out_dir", "self"]

    def __init__(self, stage_class, module_name=""):
        self._stage = stage_class
        self.module_name = module_name

    @property
    def stage(self):
        return self._stage

    @property
    def class_name(self):
        name = self.stage.__name__
        if self.module_name:
            name = self.module_name + "." + name
        return name

    def matches(self, regex):
        if regex:
            return re.search(regex, self.class_name)
        return True

    def parameters(self):
        args, vargs, kwargs, defaults, _, _, annots = get_signature(self.stage.__init__)
        args = [a for a in args if a not in self._common_config]
        return format_signature(args, vargs, kwargs, defaults, annots)

    def docstring(self, nlines=-1):
        doc = inspect.getdoc(self.stage)
        if not doc:
            return "<Missing docstring>"
        if nlines > 0:
            output = []
            while nlines > 0 and doc:
                lines = doc.split("\n", nlines)
                lines, remainder = lines[:nlines], lines[nlines:]
                lines = [l for l in lines if l.strip()]
                output += lines
                if not remainder:
                    break
                doc = remainder[0].strip()
                nlines -= len(lines)
            doc = "\n".join(output)
        return doc


def get_signature(function):
    if sys.version[0] < "3":
        return inspect.getargspec(function) + (None, None, None)
    return inspect.getfullargspec(function)


def format_signature(args, vargs, kwargs, defaults, annots):
    if annots:
        args = ["%s:%s" % (a, annots[a].__name__) if a in annots else a for a in args]
    if defaults:
        ndefs = min(len(defaults), len(args))
        def_args = ["{}={}".format(a, d) for a, d in zip(args[-ndefs:], defaults[-ndefs:])]
        args = args[:-ndefs] + def_args
    if vargs:
        args.append("*" + vargs)
    if kwargs:
        args.append("**" + kwargs)
    return args


def help_stages(stage_name, full_output, known_stages):
    if stage_name and stage_name.lower() != "all":
        stages = tuple(s for s in known_stages if s.matches(stage_name))
        if not stages:
            raise RuntimeError("Unknown stage:", stage_name)
    else:
        stages = known_stages[:]

    for i, stage in enumerate(stages):
        name = stage.class_name
        args = ", ".join(stage.parameters())
        header = "{name}\n   config: {args}".format(name=name, args=args)
        print(header)
        if full_output:
            print(stage.docstring())
        else:
            print("   purpose:", stage.docstring(1))
        if i != len(stages) - 1:
            end_stage = "=" * 80 + "\n" if full_output else ""
            print(end_stage)


def argparse_help_stages(known_stages, main_module, full_output):
    from argparse import Action
    stages = tuple(StageGuidanceHelper(s, main_module) if not isinstance(s, StageGuidanceHelper) else s
                   for s in known_stages)

    class StagesHelp(Action):
        def __call__(self, parser, namespace, values, option_string=None):
            help_stages(values, full_output=full_output, known_stages=stages)
            sys.exit(0)

    return StagesHelp
