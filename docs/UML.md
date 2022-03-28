## Intro

The purpose of `fast_carpenter` is to process HEP data using standard tools.
As such most of the code consists of bridges/adapters between data import tools,
data processing tools, data export tools and various other tools for tasks in between.





```mermaid
graph TD;
    A-->B;
    A-->C;
    B-->D;
    C-->D;
```


```mermaid
graph TD;
    abc.MutableMapping-->TreeToDictAdaptor;
    IndexProtocol-->IndexWithAliases;
    IndexProtocol-->IndexDotTransform;
    TreeLike-->TreeToDictAdaptor;
    AdapterMethods-->Uproot3Methods;
    AdapterMethods-->Uproot4Methods;

    TreeToDictAdaptor-->TreeToDictAdaptorV0;
    IndexDotTransform-->TreeToDictAdaptorV0;
    IndexWithAliases-->TreeToDictAdaptorV0;
    Uproot3Methods-->TreeToDictAdaptorV0;

    TreeToDictAdaptor-->TreeToDictAdaptorV1;
    IndexDotTransform-->TreeToDictAdaptorV1;
    IndexWithAliases-->TreeToDictAdaptorV1;
    Uproot4Methods-->TreeToDictAdaptorV1;
```

```mermaid
classDiagram
class TreeToDictAdaptor
TreeToDictAdaptor : arrays()
TreeToDictAdaptor : keys()
```





