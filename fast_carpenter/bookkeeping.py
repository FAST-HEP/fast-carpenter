from datetime import datetime
import io
import tarfile
import yaml
import pandas as pd


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


def write_booking(outfilename, sequence, datasets, **other):

    with tarfile.open(outfilename, "w:gz") as outfile:
        _add_textfile("sequence.yml", outfile, sequence)
        _add_textfile("datasets.yml", outfile, datasets)
        _add_textfile("metadata.yml", outfile, prepare_metadata(other))


def prepare_metadata(other):
    metadata = other.copy()
    metadata["sys_info"] = pd.util._print_versions.get_sys_info()
    # metadata["pandas_dependencies"] = pd.util._get_#dependency_info()
    return metadata
