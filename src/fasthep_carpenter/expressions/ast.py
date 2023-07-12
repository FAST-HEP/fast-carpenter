import ast
import numpy as np
from typing import Any
from collections import defaultdict
from functools import partial

from ..protocols import DataMapping
from ..workflow import Task, get_task_number, TaskCollection, TaskGraph
from .custom import SUPPORTED_FUNCTIONS
from .symbols import symbol_to_str


class SymbolNode(ast.AST):
    """A node representing a symbol in the expression."""
    _fields = ("value", "slice")

    def __init__(self, value, slice=None):
        self.value = value
        self.slice = slice

    def __repr__(self):
        if self.slice is not None:
            return f"SymbolNode({self.value}, {self.slice})"
        else:
            return f"SymbolNode({self.value})"


class FunctionNode(ast.AST):
    """A node representing a function in the expression."""

    _fields = ("name", "arguments", "slice")

    def __init__(self, name, arguments=None, slice=None):
        self.name = name
        self.arguments = arguments or []
        self.slice = slice

    def __repr__(self):
        if self.slice is not None:
            return f"FunctionNode({self.name}, {self.arguments}, {self.slice})"
        else:
            return f"FunctionNode({self.name}, {self.arguments})"


class Transformer(ast.NodeTransformer):
    def visit_Name(self, node):
        slice = None
        if isinstance(node.ctx, ast.Load) and node.id.endswith(']'):
            slice_node = ast.parse(f"[{node.id.split('[')[1]}", mode='eval').body
            slice = self.visit(slice_node)
            node = ast.Name(id=node.id.split('[')[0], ctx=node.ctx)
        return SymbolNode(node.id, slice=slice)

    def visit_Subscript(self, node):
        if isinstance(node.slice, ast.Slice):
            lower = self.visit(node.slice.lower) if node.slice.lower else None
            upper = self.visit(node.slice.upper) if node.slice.upper else None
            step = self.visit(node.slice.step) if node.slice.step else None
            return FunctionNode('slice', [self.visit(node.value)], slice=(lower, upper, step))
        else:
            return self.generic_visit(node)

    def visit_Call(self, node):
        name = node.func.id
        arguments = [self.visit(arg) for arg in node.args]
        slice = None
        if isinstance(node.func.ctx, ast.Load) and name.endswith(']'):
            slice_node = ast.parse(f"[{name.split('[')[1]}", mode='eval').body
            slice = self.visit(slice_node)
            name = name.split('[')[0]
        return FunctionNode(name, arguments, slice=slice)

    def visit_Index(self, node):
        return self.visit(node.value)

    def visit_Slice(self, node):
        lower = self.visit(node.lower) if node.lower else None
        upper = self.visit(node.upper) if node.upper else None
        step = self.visit(node.step) if node.step else None
        return (lower, upper, step)


def get_symbol(data: DataMapping, symbol: SymbolNode):
    if symbol.slice is not None:
        return data[symbol.value][symbol.slice]
    else:
        return data[symbol.value]


def ast_to_expression(node: ast.AST) -> str:
    if isinstance(node, SymbolNode):
        if node.slice is not None:
            return f"{node.value}{node.slice}"
        else:
            return node.value
    elif isinstance(node, ast.BinOp):
        return f"{ast_to_expression(node.left)} {symbol_to_str(node.op)} {ast_to_expression(node.right)}"
    elif isinstance(node, ast.UnaryOp):
        return f"{symbol_to_str(node.op)}{ast_to_expression(node.operand)}"
    elif isinstance(node, ast.Compare):
        return f"{ast_to_expression(node.left)} {symbol_to_str(node.ops[0])} {ast_to_expression(node.comparators[0])}"
    else:
        return node.value


class ASTWrapper:

    ast: Any
    tasks: TaskCollection
    data_source: str = "__joint__"

    def __init__(self, abstrac_syntax_tree) -> None:
        self.ast = abstrac_syntax_tree
        self.tasks = None

    def __repr__(self) -> str:
        return f"ASTWrapper({self.ast})"

    def _get_task_name(self, task_type: str) -> str:
        task_id = get_task_number(task_type)
        return f"{task_type}-{task_id}"

    def _is_pure(self, node: Any) -> bool:
        for node in ast.walk(node):
            if isinstance(node, SymbolNode) and node.slice is not None:
                yield False
                continue
            yield not isinstance(node, (FunctionNode, ))

    def __build(self, node: Any):
        from dask.utils import apply
        # if node only contains symbols or binary operators, create eval task
        is_pure = all(self._is_pure(node))
        if isinstance(node, ast.Constant):
            task_name = self._get_task_name("constant")
            self.tasks.add_task(task_name, SUPPORTED_FUNCTIONS["constant"], node.value)
            return task_name

        if is_pure:
            task_name = self._get_task_name("eval")
            # partial_eval = partial(SUPPORTED_FUNCTIONS["eval"], global_dict=data)
            self.tasks.add_task(
                task_name, SUPPORTED_FUNCTIONS["eval"],
                ast_to_expression(node),
                global_dict=f"{self.data_source}")
            return task_name

        if isinstance(node, FunctionNode):
            var_name = node.name
            previous_tasks = []
            for arg in node.arguments:
                previous_tasks.append(self.__build(arg))
            task_name = self._get_task_name(f"func-{var_name}")

            slice_args = []
            if node.slice is not None:
                # convert slice to tuple
                for item in node.slice:
                    if item is None:
                        slice_args.append(None)
                        continue
                    slice_args.append(self.__build(item))
                self.tasks.add_task(task_name, SUPPORTED_FUNCTIONS[var_name], previous_tasks[0], slice_args)
            else:
                self.tasks.add_task(task_name, SUPPORTED_FUNCTIONS[var_name], *previous_tasks)
            return task_name

    def build(self, join_task: str = "__joint__") -> None:
        self.data_source = join_task
        self.tasks = TaskCollection()
        self.__build(self.ast)

    def to_tasks(self, join_task: str = "__joint__") -> dict[str, Task]:
        if not self.tasks:
            self.build(join_task)
        return self.tasks

    @property
    def graph(self) -> TaskGraph:
        if not self.tasks:
            self.tasks = TaskCollection()
            self.__build(self.ast)
        return self.tasks.graph

    def get_graph(self, join_task: str = "__joint__") -> TaskGraph:
        self.data_source = join_task
        return self.graph


def expression_to_ast(expression):
    tree = ast.parse(expression, mode='eval')
    transformer = Transformer()
    new_tree = transformer.visit(tree.body)
    return ASTWrapper(new_tree)
