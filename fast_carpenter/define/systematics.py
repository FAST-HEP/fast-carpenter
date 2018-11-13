from .variables import Define


class BadSystematicWeightsConfig(Exception):
    pass


class SystematicWeights():

    def __init__(self, name, out_dir, variables,
                 up="Up", down="Down", nominal="nominal"):
        self.name = name
        self.out_dir = out_dir
        variations = _build_variations(name, variables, up, down, nominal)
        variations = [{k: v} for k, v in variations.items()]
        self.variable_maker = Define(name + "_builder", out_dir, variations)

    def event(self, chunk):
        return self.variable_maker.event(chunk)


def _build_variations(stage_name, variable_list, up, down, nominal):
    if not isinstance(variable_list, list):
        msg = "{}: Didn't receive a list of variables"
        raise BadSystematicWeightsConfig(msg.format(stage_name))
    variables = {"weight_" + nominal: "*".join(variable_list)}
    for var in variable_list:
        for direction in [up, down]:
            this_var_list = variable_list[:]
            this_var_list.remove(var)
            this_var_list += [var + direction]
            name = "weight_{}_{}".format(var, direction)
            variables[name] = "*".join(this_var_list)
    return variables
