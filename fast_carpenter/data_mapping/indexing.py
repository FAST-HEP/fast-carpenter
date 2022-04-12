from dataclasses import field
from typing import Dict, List, Optional, Protocol


class IndexProtocol(Protocol):
    def resolve_index(self, index: str) -> str:
        pass


class IndexWithAliases(IndexProtocol):
    aliases: Dict[str, str] = field(default_factory=dict)

    def __init__(self, aliases):
        self.aliases = aliases
        if self.aliases is None:
            self.aliases = {}

    def resolve_index(self, index):
        if not self.aliases:
            return index
        if index in self.aliases:
            return self.aliases[index]
        return self.index


class TokenMapIndex(IndexProtocol):
    _token_map: Dict[str, str] = {
        ".": "__DOT__",
    }

    def __init__(self, token_map: Optional[Dict[str, str]] = None):
        if token_map is not None:
            self._token_map = token_map

    def resolve_index(self, index):
        new_index = index
        for token in self._token_map:
            if token in index:
                new_index = new_index.replace(token, self._token_map[token])
        return new_index


class MultiTreeIndex(IndexProtocol):
    _prefixes: List[str] = field(default_factory=list)

    def __init__(self, prefixes: Optional[List[str]] = None):
        self._prefixes = prefixes

    def resolve_index(self, index):
        index = index.replace(".", "/")
        if self._prefixes:
            for prefix in self._prefixes:
                if index.startswith(prefix):
                    index = index.replace(prefix, "", 1)
                    if index.startswith("/"):
                        index = index[1:]
                    if index:
                        return f"{prefix}/{index}"
                    return prefix
                return f"{prefix}/{index}"
        return index
