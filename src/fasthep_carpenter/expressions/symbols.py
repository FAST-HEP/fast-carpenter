import ast

op_data = """
              Or   or           1
             And   and          1
             Not   not          1
              Eq   ==           1
              Gt   >            0
             GtE   >=           0
              In   in           0
              Is   is           0
           NotEq   !=           0
              Lt   <            0
             LtE   <=           0
           NotIn   not in       0
           IsNot   is not       0
           BitOr   |            1
          BitXor   ^            1
          BitAnd   &            1
          LShift   <<           1
          RShift   >>           0
             Add   +            1
             Sub   -            0
            Mult   *            1
             Div   /            0
             Mod   %            0
        FloorDiv   //           0
         MatMult   @            0
          Invert   ~            1
            UAdd   +            0
            USub   -            0
             Pow   **           1
"""
op_data = [x.split() for x in op_data.splitlines()]
op_data = [[x[0], ' '.join(x[1:-1]), int(x[-1])] for x in op_data if x]
for index in range(1, len(op_data)):
    op_data[index][2] *= 2
    op_data[index][2] += op_data[index - 1][2]


symbol_data = dict((getattr(ast, x, None), y) for x, y, z in op_data)

def symbol_to_str(node):
    if isinstance(node, ast.AST):
        return symbol_data[type(node)]
    else:
        return str(node)