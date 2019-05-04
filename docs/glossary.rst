Glossary
========
.. glossary::

  cut-flow
    A series of "cuts" which remove events from the processing.

  data-space
    The current set of variables known to fast-carpenter and passed between stages.
    Stages can modify the data-space which will affect what subequent stages see.
    Before any stages have been run, the data-space contains only those variables
    given in the input datasets.

  processing config
    A YAML-based description of the way the input data should be processed.

  processing stage
    A single step in the processing chain, which can modify the data-space or produce new outputs.

  dataset config
    A YAML-based description of the input files which form the datasets to be processed.

  expression
    A string representing some mathematical manipulation of variables in the data-space.

  dataframe
    A programmatic interface to a table-like representation of data.  
    In the context of fast-carpenter, "dataframe" will usually refer to the 
    :py:class:`pandas.DataFrame` implementation.

  jagged array
    A generalisation of a multi-dimensional numpy array where the length of
    each sub-array in the second (and third, and fourth, and so on) dimension
    can vary.  For example, if each event contains a list of particles produced
    in that event, this would be represented by a jagged array, since there can
    be different numbers of particles in each event.  Typically for
    fast-carpenter, a jagged array refers to the specific implementation from
    the `awkward-array package <https://github.com/scikit-hep/awkward-array>`_
