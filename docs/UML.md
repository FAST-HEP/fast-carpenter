## Intro

The purpose of `fast_carpenter` is to process HEP data using standard tools.
As such most of the code consists of bridges/adapters between data import tools,
data processing tools, data export tools and various other tools for tasks in between.


```mermaid
graph TD;
    abc.MutableMapping-->TreeToDictAdaptor;

    TreeLike-->TreeToDictAdaptor;
    AdapterMethods-->Uproot4Methods;
    AdapterMethods-->Uproot3Methods;

    TreeToDictAdaptor-->TreeToDictAdaptorV0;
    Uproot3Methods-->TreeToDictAdaptorV0;

    TreeToDictAdaptor-->TreeToDictAdaptorV1;
    Uproot4Methods-->TreeToDictAdaptorV1;
```

```mermaid
classDiagram
class IndexProtocol{
<<Protocol>>
resolve_index(index: str) str
}
class IndexWithAliases{
    aliases: Dict[str, str]
    resolve_index(index: str) str
}
class TokenMapIndex{
    token_map: Dict[str, str]
    resolve_index(index: str) str
}
class MultiTreeIndex{
    file_index: nested Dict[str, str]
    resolve_index(index: str) str
}

IndexProtocol <|-- IndexWithAliases : implements
IndexProtocol <|-- TokenMapIndex : implements
IndexProtocol <|-- MultiTreeIndex : implements
```

```mermaid
classDiagram
class FileLike
<<Protocol>> FileLike
class TreeLike
<<Protocol>> TreeLike
class ArrayLike
<<Protocol>> ArrayLike
```

```mermaid
classDiagram
class TreeToDictAdaptor
<<Protocol>> TreeToDictAdaptor
TreeToDictAdaptor : __getitem__()
TreeToDictAdaptor : __setitem__()
TreeToDictAdaptor : __iter__()
TreeToDictAdaptor : __iter__()
```

```mermaid
classDiagram
    TreeToDictAdaptor --> "contains" FileLike
    FileLike "1" --> "many" TreeLike
    TreeLike "1" --> "many" ArrayLike
```




