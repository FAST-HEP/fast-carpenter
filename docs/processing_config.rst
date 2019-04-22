.. _ref-processing_config:

The Processing Config
=====================

The config file is written in `YAML <https://en.wikipedia.org/wiki/YAML>`_ format.  In essence it is a large dictionary to describe how we should process our data.

An example config file looks like:

.. literalinclude:: demo_process_cfg.yml
    :linenos:
    :language: yaml


Note that since this is a YAML file, things like anchors and block syntax are totally valid, which can be helpful, especially to define "aliases" or reuse certain parts of a config.
In addition, note that since YAML is a superset of JSON, it's perfectly valid to use a JSON file instead.

Other, more complete examples are listed in :ref:`ref-example_repos`.

Stages
^^^^^^

The most important section is the ``stages`` section.  The list contained in this section describes what steps will be taken with the data, and in what order.
It is formed by a list of single-length dictionaries, whose key is the name for the stage and whose value is the python class that implements that stage.

The list of stages known to fast_carpenter already can be found using the built-in ``--help-stages`` option.

.. command-output:: fast_carpenter --help-stages

Each stage must then be given a complete description by adding a top-level section in the yaml file with the name provided in the ``stages`` section.
Then write a dictionary below this, whose arguments will then be passed to the underlying class's init method as keyword-arguments.
Therefore, to check how to configure a specific stage look at the documentation for that class.

------------

``CutFlow`` stages
^^^^^^^^^^^^^^^^^^^

+---------------------+--------------+------------------------------------------------------------------------------------------------------+
| Parameter           | Default      | Description                                                                                          |
+=====================+==============+======================================================================================================+
| ``selection``       |              | | The most important part of this stage's description:  how do you actually want to select events?   |
|                     |              | | This is a nested set of dictionaries, each with a single key being one of ``Any``, ``All``,        |
|                     |              | | or ``Not``, and each value in the dictionary should either be a list of cuts or another such       |
|                     |              | | dictionary. Each cut is a string that should return true if the event is to pass the cut.          |
|                     |              | | The event object is represented in the string using the name given to the ``lambda_arg`` variable. |
+---------------------+--------------+------------------------------------------------------------------------------------------------------+
| ``selection_file``  |              | | The name of a file containing just an event selection, in the same format as described above.      |
|                     |              | | Primarily to help share selections between analyses, or to re-use the same selection multiple      |
|                     |              | | times in one config.                                                                               |
|                     |              | | *Note: One and only one of* ``selection`` *and* ``selection_file`` *must be provided.*             |
+---------------------+--------------+------------------------------------------------------------------------------------------------------+
| ``counter``         | ``True``     | | Use to control whether or not we store the number of events passing each cut in an output file.    |
|                     |              | | Can be omitted.                                                                                    |
+---------------------+--------------+------------------------------------------------------------------------------------------------------+
| ``weights``         | ``1.0``      | | How should each event that is cut be weighted?  Only used if ``counter == True``.  Can be either a |
|                     |              | | a float, the name of an event attribute or a dictionary with the name to include in the output     |
|                     |              | | filename, and the event attribute to be used.  Can be omitted.                                     |
+---------------------+--------------+------------------------------------------------------------------------------------------------------+

``BinnedDataframe`` stages
^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-------------------+--------------+--------------------------------------------------------------------------------------------------------+
| Parameter         | Default      | Description                                                                                            |
+===================+==============+========================================================================================================+
| ``weights``       | ``1.0``      | | How to weight events.  A single weight                                                               |
|                   |              |   scheme can be specified by a float, an event attribute to use, or a                                  |
|                   |              | | single length dictionary with the output name and input event attribute as key and                   |
|                   |              |   value.  Alternatively,                                                                               |
|                   |              | | a list of such schemes can be provided, in which case                                                |
|                   |              |   multiple binned dataframes are produced,                                                             |
|                   |              | | one for each weight scheme.                                                                          |
+-------------------+--------------+--------------------------------------------------------------------------------------------------------+
| ``binning``       |              | | A list of binning dimensions.  Each dimension should be configured with a                            |
|                   |              | | dictionary as described  `below <ref_bin_params_>`_.                                                 |
+-------------------+--------------+--------------------------------------------------------------------------------------------------------+


Each item in the list under the ``binning`` key should be a dictionary, where the following parameters are accepted or even required.

.. _ref_bin_params:


+-------------------+--------------+-----------------------------------------------------------------------------------------------------------------------+
| Parameter         | Default      | Description                                                                                                           |
+===================+==============+=======================================================================================================================+
| ``in``            |              | The name of the attribute on the event to use.                                                                        |
+-------------------+--------------+-----------------------------------------------------------------------------------------------------------------------+
| ``out``           |              | The name of the column to be filled in the output dataframe.                                                          |
+-------------------+--------------+-----------------------------------------------------------------------------------------------------------------------+
| ``index``         | ``'*'``      | | If the variable is a list (eg. a list of jet P\ :sub:`T`\ s), then use this element with this index in the list.    |
|                   |              |   If ``*`` then                                                                                                       |
|                   |              | | every element in the list is included.                                                                              |
+-------------------+--------------+-----------------------------------------------------------------------------------------------------------------------+
| ``bins``          | ``None``     | | Must be either ``None`` or a dictionary.  If a dictionary, it must contain one of the follow                        |
|                   |              |   sets of                                                                                                             |
|                   |              | | key-value pairs:                                                                                                    |
|                   |              | |   1. ``nbins``, ``low``, ``high``: which are used to produce a list of bin edges equivalent to:                     |
|                   |              | |      ``numpy.linspace(low, high, nbins + 1)``                                                                       |
|                   |              | |   2. ``edges``: which is treated as the list of bin edges directly.                                                 |
|                   |              | | If set to ``None``, then the input variable is assumed to already be categorical                                    |
|                   |              |   (ie. binned or discrete)                                                                                            |
+-------------------+--------------+-----------------------------------------------------------------------------------------------------------------------+


``Scribbler`` stages
^^^^^^^^^^^^^^^^^^^^

+-------------------+--------------+--------------------------------------------------------------------------------------------------------+
| Parameter         | Default      | Description                                                                                            |
+===================+==============+========================================================================================================+
| ``module``        |              | | The python module that defines and implements the scribbler(s) that you wish to use.                 |
|                   |              |   Must be importable                                                                                   |
|                   |              | | within python, ie. you might need to modify ``PYTHONPATH``.                                          |
+-------------------+--------------+--------------------------------------------------------------------------------------------------------+
| ``scribblers``    |              | | A list of the scribblers to use from the imported module.  Each scribbler must be specified either   |
|                   |              |   by a single                                                                                          |
|                   |              | | string, in which case the scribbler will be initialised without any arguments being passed in.       |
|                   |              | | If the scribbler should be initialised with arguments, then you can pass these as a dictionary of    |
|                   |              |   keyword                                                                                              |
|                   |              | | argument pairs.                                                                                      |
+-------------------+--------------+--------------------------------------------------------------------------------------------------------+
