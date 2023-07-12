
class PandasExpressionEngine:
    methods: ArrayMethodsProtocol
    supported_methods: dict[str, Callable[[Any], Any]]

    def __init__(self, methods: ArrayMethodsProtocol):
        self.methods = methods
        self.supported_methods = deepcopy(SUPPORTED_FUNCTIONS)
        self.supported_methods.update(
            {
                "all": self.methods.all,
                "any": self.methods.any,
                "argmax": self.methods.argmax,
                "argmin": self.methods.argmin,
                "count_nonzero": self.methods.count_nonzero,
                "max": self.methods.max,
                "min": self.methods.min,
                "prod": self.methods.prod,
                "sum": self.methods.sum,
            }
        )
        self.task_counters = defaultdict(int)

    def evaluate(self, data: DataMapping, expressions: list[str]):
        pd_eval = partial(pd.eval, engine="numexpr", local_dict=data)
        return [pd_eval(expression) for expression in expressions]

    def replace_attributes(self, expression: str):
        return _replace_attributes(expression)

    def reset_task_counters(self):
        self.task_counters = defaultdict(int)


class DaskExpressionEngine:
    methods: ArrayMethodsProtocol
    supported_methods: dict[str, Callable[[Any], Any]]
    task_counters: dict[str, int]

    def __init__(self, methods: ArrayMethodsProtocol):
        self.methods = methods
        self.supported_methods = deepcopy(SUPPORTED_FUNCTIONS)
        self.supported_methods.update(
            {
                "all": self.methods.all,
                "any": self.methods.any,
                "argmax": self.methods.argmax,
                "argmin": self.methods.argmin,
                "count_nonzero": self.methods.count_nonzero,
                "max": self.methods.max,
                "min": self.methods.min,
                "prod": self.methods.prod,
                "sum": self.methods.sum,
            }
        )
        self.task_counters = defaultdict(int)

    def reset_task_counters(self):
        self.task_counters = defaultdict(int)

    def extract_functions_and_parameters(self, expression: str) -> dict[str, Any]:
        pass


class ASTExpressionEngine:
    methods: ArrayMethodsProtocol
    supported_methods: dict[str, Callable[[Any], Any]]
    task_counters: dict[str, int]

    def __init__(self, methods: ArrayMethodsProtocol):
        self.methods = methods
        self.supported_methods = deepcopy(SUPPORTED_FUNCTIONS)
        self.supported_methods.update(
            {
                "all": self.methods.all,
                "any": self.methods.any,
                "argmax": self.methods.argmax,
                "argmin": self.methods.argmin,
                "count_nonzero": self.methods.count_nonzero,
                "max": self.methods.max,
                "min": self.methods.min,
                "prod": self.methods.prod,
                "sum": self.methods.sum,
            }
        )
        self.task_counters = defaultdict(int)

    def reset_task_counters(self):
        self.task_counters = defaultdict(int)

    def evaluate(self, data: DataMapping, expression: str):
        expression = self.replace_attributes(expression)

        as_tree = expression_to_ast(expression)

        return ast

    def _traverse(self, node, data):
        if isinstance(node, SymbolNode):
            return self._evaluate_symbol(node, data)
        elif isinstance(node, FunctionNode):
            return self._evaluate_function(node, data)
        else:
            raise ValueError(f"Invalid node type: {type(node)}")

    def replace_attributes(self, expression: str):
        return _replace_attributes(expression)


ExpressionEngine = ASTExpressionEngine