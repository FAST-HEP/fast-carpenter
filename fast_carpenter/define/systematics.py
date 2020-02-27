from .variables import Define
import six


class BadSystematicWeightsConfig(Exception):
    pass


class SystematicWeights():
    """Combines multiple weights and variations to produce a single event weight

    To study the impact of systematic uncertainties it is common to re-weight events
    using a variation of the weights representing, for example, a 1-sigma increase or decrease
    in the weights.  Once there are multiple weight schemes involved writing out each possible combination
    of these weights becomes tedious and potentially error-prone; this stage makes it easier.

    It forms the ``nominal`` weight for each event by multiplying all nominal weights together,
    then the specific variation by replacing a given nominal weight with its corresponding "up" or "down" variation.

    Each variation of a weight should just be a string giving an expression to
    use for that variation.  This stage then combines these into a single
    expression by joining each set of variations with `"*"`, i.e. multiplying
    them together.  The final results then use an internal :py:class:`Define`
    stage to do the calculation.

    Parameters:
      weights (dictionary[str, dictionary]):  A Dictionary of weight variations
          to combine.  The keys in this dictionary will determine how this
          variation is called in the output variable.  The values of this
          dictionary should either be a single string -- the name of the input
          variable to use for the "nominal" variation, or a dictionary containing
          any of the keys, ``nominal``, ``up``, or ``down``.  Each of these should
          then have a value providing the expression to use for that variation/
      out_format (str): The format string to use to build the name of the
          output variations.  Defaults to "weight_{}".  Should contain a pair
          of empty braces which will be replaced with the name for the current
          variation, e.g. "nominal" or "PileUp_up".
      extra_variations (list[str]): A list of additional variations to allow

    Other Parameters:
      name (str):  The name of this stage (handled automatically by fast-flow)
      out_dir (str):  Where to put the summary table (handled automatically by
          fast-flow)

    Example:
      ::

        syst_weights:
          energy_scale: {nominal: WeightEnergyScale, up: WeightEnergyScaleUp, down: WeightEnergyScaleDown}
          trigger: TriggerEfficiency
          recon: {nominal: ReconEfficiency, up: ReconEfficiency_up}

      which will create 4 new variables:
      ::

        weight_nominal =  WeightEnergyScale * TriggerEfficiency * ReconEfficiency
        weight_energy_scale_up =  WeightEnergyScaleUp * TriggerEfficiency * ReconEfficiency
        weight_energy_scale_down =  WeightEnergyScaleDown * TriggerEfficiency * ReconEfficiency
        weight_recon_up =  WeightEnergyScale * TriggerEfficiency * ReconEfficiency_up
    """
    def __init__(self, name, out_dir, weights, out_format="weight_{}", extra_variations=[]):
        self.name = name
        self.out_dir = out_dir
        weights = _normalize_weights(name, weights, tuple(extra_variations))
        variations = _build_variations(name, weights, out_fmt=out_format)
        self.variable_maker = Define(name + "_builder", out_dir, variations)

    def event(self, chunk):
        if not chunk.config.dataset.eventtype == "mc":
            return True
        return self.variable_maker.event(chunk)


def _normalize_weights(stage_name, variable_list, valid_vars):
    if not isinstance(variable_list, dict):
        msg = "{}: Didn't receive a list of variables"
        raise BadSystematicWeightsConfig(msg.format(stage_name))
    return {name: _normalize_one_variation(stage_name, cfg, name, valid_vars=valid_vars)
            for name, cfg in variable_list.items()}


def _build_variations(stage_name, weights, out_fmt="weight_{}"):
    def _combine_weights(w):
        return "(" + ")*(".join(w) + ")"

    nominal_weights = {n: w["nominal"] for n, w in weights.items()}
    variations = [{out_fmt.format("nominal"): _combine_weights(nominal_weights.values())}]
    weights_to_vary = {(n, var): w[var] for n, w in weights.items() for var in w if var != "nominal"}
    for (name, direction), variable in weights_to_vary.items():
        combination = nominal_weights.copy()
        combination[name] = variable
        combination = _combine_weights(combination.values())
        variations.append({out_fmt.format(name + "_" + direction): combination})
    return variations


def _normalize_one_variation(stage_name, cfg, name, valid_vars):
    if isinstance(cfg, six.string_types):
        return dict(nominal=cfg)
    if not isinstance(cfg, dict):
        msg = "{}: Each systematic weight should be either a dict or just a string"
        raise BadSystematicWeightsConfig(msg.format(stage_name))
    if "nominal" not in cfg:
        msg = "{}: No nominal weight provided for '{}'"
        raise BadSystematicWeightsConfig(msg.format(stage_name, name))

    bad_keys = [key for key in cfg if key not in ("nominal", "up", "down") + valid_vars]
    if bad_keys:
        msg = "{}: Received unknown keys,'{}', for '{}'"
        raise BadSystematicWeightsConfig(msg.format(stage_name, bad_keys, name))
    return cfg
