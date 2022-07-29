
## Workflows

[]: # Language: markdown
[]: # Path: docs/workflows.md

Workflows are a way to organize the data analysis pipeline.
They take a sequence from fast-flow and form a task graph from it.

Task graphs should start with a data input task and end with a data collector task.
Every task in between has an additional requirement: the ability to chain.

Example:
Structure: `name of task`: (task_function, task_arguments)

- data-import task: (f.open, "filename") --> returns a file handle/ data chunk
- data processing task: (task, previous_task) --> a task needs to return a chunk
Here, the data processing task is either selecting a chunk of data, adding to it (e.g. new variables) or histogramming it.

The latter, would be an additional result. So the more general form for a task is:

- data processing task: (task, previous_task) --> returns (chunk, result1, ..., resultN)
