from .variables import Define
import six


class BadSystematicWeightsConfig(Exception):
    pass


class SystematicWeights():

    def __init__(self, name, out_dir, weights):
        self.name = name
        self.out_dir = out_dir
        weights = _normalize_weights(name, weights)
        variations = _build_variations(name, weights)
        self.variable_maker = Define(name + "_builder", out_dir, variations)

    def event(self, chunk):
        if not chunk.config.dataset.eventtype == "mc":
            return True
        return self.variable_maker.event(chunk)


def _normalize_weights(stage_name, variable_list):
    if not isinstance(variable_list, dict):
        msg = "{}: Didn't receive a list of variables"
        raise BadSystematicWeightsConfig(msg.format(stage_name))
    return {name: _normalize_one_variation(stage_name, cfg) for name, cfg in variable_list.items()}


def _build_variations(stage_name, weights, out_name="weight_{}"):
    nominal_weights = {n: w["nominal"] for n, w in weights.items()}
    variations = [{out_name.format("nominal"): "*".join(nominal_weights.values())}]
    weights_to_vary = {(n, var): w[var] for n, w in weights.items() for var in ("up", "down") if var in w}
    for (name, direction), variable in weights_to_vary.items():
        combination = nominal_weights.copy()
        combination[name] = variable
        combination = "*".join(combination.values())
        variations.append({out_name.format(name + "_" + direction): combination})
    return variations


def _normalize_one_variation(stage_name, cfg):
    if isinstance(cfg, six.string_types):
        return dict(nominal=cfg)
    if not isinstance(cfg, dict):
        msg = "{}: Each systematic weight should be either a dict or just a string"
        raise BadSystematicWeightsConfig(msg.format(stage_name))
    bad_keys = [key for key in cfg if key not in ("nominal", "up", "down")]
    if bad_keys:
        msg = "{}: Received unknown keys '{}'"
        raise BadSystematicWeightsConfig(msg.format(stage_name, bad_keys))
    return cfg
