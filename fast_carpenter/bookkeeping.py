from datetime import datetime
import os
import io
import pwd
import platform
import importlib
import tarfile
import yaml
import pandas as pd


_dependencies = ["pandas",
                 "ROOT",
                 "fast_flow",
                 "fast_curator",
                 "fast_plotter",
                 "fast_datacard",
                 "alphatwirl",
                 "yaml",
                 "coffea",
                 "alphatwirl",
                 "atuproot",
                 "atsge",
                 "numpy",
                 "matplotlib",
                 "pip",
                 'atpbar',
                 'mantichora',
                 'awkward',
                 'numba',
                 'numexpr',
                 'uproot',
                 ]


def _to_yaml(contents):
    # https://stackoverflow.com/questions/25108581/python-yaml-dump-bad-indentation
    class MyDumper(yaml.Dumper):
        def increase_indent(self, flow=False, indentless=False):
            return super(MyDumper, self).increase_indent(flow, False)

        # disable aliases and anchors, see https://github.com/yaml/pyyaml/issues/103
        def ignore_aliases(self, data):
            return True

    def replace_types(contents):
        if isinstance(contents, tuple):
            return list(contents)
        if isinstance(contents, list):
            return [replace_types(c) for c in contents]
        if isinstance(contents, dict):
            return type(contents)((k, replace_types(v)) for k, v in contents.items())
        if hasattr(contents, "__dict__"):
            return vars(contents)
        return contents

    return yaml.dump(replace_types(contents), Dumper=MyDumper, default_flow_style=False)


def _add_textfile(filename, tarball, contents):
    if not isinstance(contents, str):
        contents = _to_yaml(contents)
    data = contents.encode('utf8')
    info = tarfile.TarInfo(name=filename)
    info.size = len(data)
    info.mtime = datetime.now().timestamp()
    info.mode = 0o444
    tarball.addfile(info, io.BytesIO(data))


def write_booking(outfilename, sequence, datasets, extra_dependencies=[], **other):

    with tarfile.open(outfilename, "w:gz") as outfile:
        _add_textfile("sequence.yml", outfile, sequence)
        _add_textfile("datasets.yml", outfile, datasets)
        meta = prepare_metadata(other, extra_dependencies)
        _add_textfile("metadata.yml", outfile, meta)


def get_version(name):
    try:
        module = importlib.import_module(name)
    except ImportError:
        return "<not installed>"

    version = getattr(module, "__version__", "<version unknown>")
    return version


def get_platform_details():
    attrs = ["machine", "node", "processor", "release", "uname", "system", "architecture"]
    attrs += ["python_" + a for a in ["build", "compiler", "version", "implementation"]]
    return {a: getattr(platform, a)() for a in attrs}


def get_user_details():
    # Only reliable for *nix:
    details = pwd.getpwuid(os.getuid())
    return dict(name=details.pw_name)


def prepare_metadata(other, extra_dependencies=[]):
    metadata = other.copy()
    metadata["sys_info"] = pd.util._print_versions._get_sys_info()
    deps = _dependencies + extra_dependencies
    metadata["versions"] = {pkg: get_version(pkg) for pkg in deps}
    metadata["workdir"] = os.getcwd()
    metadata["platform"] = get_platform_details()
    metadata["execution_time_iso"] = datetime.now().isoformat()
    metadata["user"] = get_user_details()
    return metadata
