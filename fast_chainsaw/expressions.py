import StringIO
import tokenize


def get_branches(cut, valid):
    branches = []
    string = StringIO.StringIO(cut).readline
    tokens = tokenize.generate_tokens(string)
    current_branch = []
    for toknum, tokval, _, _, _ in tokens:
        if toknum == tokenize.NAME:
            if ".".join(current_branch + [tokval]) in valid:
                current_branch.append(tokval)
                continue
        if tokval == ".":
            continue
        if current_branch:
            branches.append(".".join(current_branch))
            current_branch = []
    return branches


