import awkward0
import uproot3

SETUP_DONE = False


class FakeBranch(object):
    def __init__(self, name, values, event_ranger):
        self.name = name
        self._values = values
        self._fLeaves = []
        self.fLeaves = []
        self.event_ranger = event_ranger

    @property
    def _recoveredbaskets(self):
        return []

    def array(self, entrystart=None, entrystop=None, blocking=True, **kws):
        array = self._values
        if entrystart:
            entrystart -= self.event_ranger.start_entry
        if entrystop:
            entrystop -= self.event_ranger.start_entry

        def wait():
            values = array[entrystart:entrystop]
            return values

        if not blocking:
            return wait
        return wait()

    def __getattr__(self, attr):
        return getattr(self._values, attr)

    def __len__(self):
        return len(self._values)


def recursive_type_wrap(array):
    if isinstance(array, awkward0.JaggedArray):
        return uproot3.asjagged(recursive_type_wrap(array.content))
    return uproot3.asdtype(array.dtype.fields)


class wrapped_asgenobj(uproot3.asgenobj):
    def finalize(self, *args, **kwargs):
        result = super(wrapped_asgenobj, self).finalize(*args, **kwargs)
        result = awkward0.JaggedArray.fromiter(result)
        return result


def wrapped_interpret(branch, *args, **kwargs):
    from uproot3.interp.auto import interpret
    result = interpret(branch, *args, **kwargs)
    if result:
        return result

    if isinstance(branch, FakeBranch):
        return recursive_type_wrap(branch._values)

    return None


def uproot3_setup() -> None:
    global SETUP_DONE
    if SETUP_DONE:
        return

    uproot3.interp.auto.asgenobj = wrapped_asgenobj
    uproot3.tree.interpret = wrapped_interpret
    SETUP_DONE = True


def add_new_variable(name):
    uproot3_setup()
    name = uproot3.rootio._bytesid(name)
    return name
