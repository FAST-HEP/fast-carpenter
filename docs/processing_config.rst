.. _ref-processing_config:

The Processing Config
=====================
The processing config file tells fast-carpenter what to do with your data and
is written in `YAML <https://en.wikipedia.org/wiki/YAML>`_.

An example config file looks like:

.. literalinclude:: demo_process_cfg.yml
    :linenos:
    :language: yaml

Other, more complete examples are listed in :ref:`ref-example_repos`.

.. tip::
  Since this is a YAML file, things like anchors and block syntax are totally valid, which can be helpful to define "aliases" or reuse certain parts of a config.
  For more guidance on YAML, this is a good overview of the concepts and syntax: https://kapeli.com/cheat_sheets/YAML.docset/Contents/Resources/Documents/index.

Anatomy of the config
---------------------

The ``stages`` section
  This is the most important section of the config because it defines what steps to take with the data.
  It uses a list of single-length dictionaries, whose key is the name for the stage (e.g. ``histogram``) and whose values is the python-importable class that implements it (e.g. ``fast_carpenter.BinnedDataframes``).
  The following sections discuss what are valid stage classes.
  Lines 1 to 4 of the config above show an example of this section and others can be found in the linked :ref:`ref-example_repos`.

Stage configuration sections
  Each stage must be given a complete description by adding a top-level section in the yaml file with the same name provided in the ``stages`` section.
  This should then contain a dictionary which will be passed as keyword-arguments to the underlying class' init method.
  Lines 22 to 26 of the above example config file show how the stage called ``histogram`` is configured.
  See below for more help on configuring specific stages.

Importing other config files
  Sometimes it can be helpful to re-use one config in another, for example, defining a list of common variables and event selections, but then changing the BinnedDataframes that are produced.
  The processing config supports this by using the reserved word ``IMPORT`` as the key for a stage, followed by the path to the config file to import.
  If the path starts with ``{this_dir}`` then the imported file will be located relative to the directory of the importing config file.
  
  For example::
    
    - IMPORT: "{this_dir}/another_processing_config.yml"

.. seealso::
    The interpretation of the processing config is handled by the `fast-flow
    package <https://gitlab.cern.ch/fast-hep/public/fast-flow>`_ so its
    documentation can also be helpful to understanding the basic anatomy and
    handling.

Built-in Stages
---------------

The list of stages known to fast_carpenter already can be found using the built-in ``--help-stages`` option.

.. command-output:: fast_carpenter --help-stages

Further guidance on the built-in stages can be found using ``--help-stages-full`` and giving the name of the stage.
All the built-in stages of ``fast_carpenter`` are available directly from the ``fast_carpenter`` module, e.g. ``fast_carpenter.Define``.

.. seealso::
  In-depth discussion of the built-in stages and their configuration can be found on the ``fast_carpenter`` module page: :mod:`fast_carpenter`.


.. .. automodule:: fast_carpenter
..     :members:
..     :undoc-members:
..     :show-inheritance:
..     :noindex:
.. ..    The following line can exclude members from the list if needed
.. ..    :exclude-members: SystematicWeights


Used-defined Stages
-------------------
fast-carpenter is still evolving, and so it is natural that many analysis tasks cannot be implemented using the existing stages.
In this case, it is possible to implement your own stage and making sure it can be imported by python (e.g. by setting the ``PYTHONPATH`` variable to point to the directory containing its code).
The class implementing a custom stage should provide the following methods:

.. py:method:: __init__(name, out_dir, ...)

    This is the method that will receive configuration from the config file, creating the stage itself.

    :param str name: will contain the name of the stage as given in the config file.
    :param path out_dir: receives the path to the output directory that should be used if the stage produces output.

    Additional arguments can be added, which will be configurable from the processing config file.
   
.. py:method:: event(chunk)

  Called once for each chunk of data.  

  :param chunk: provides access to the dataset configuration (``chunk.config``)
    and the current data-space (``chunk.tree``).  Typically one wants an array,
    or set of arrays representing the data for each event, in which case these
    can be obtained using::
      
       jet_pt = chunk.tree.array("jet_pt")
       jet_pt, jet_eta = chunk.tree.arrays(["jet_pt", "jet_eta", outputtype=tuple)

    If your stage produces a new variable, which you want other stages to be able to see, then use the `new_variable` method::

       chunk.tree.new_variable("number_good_jets", number_good_jets)

    For more details on working with ``chunk.tree``, see :class:`fast_carpenter.masked_tree.MaskedUprootTree`.
  :return: ``True`` or ``False`` for whether to continue processing the chunk through subsequent stages.
  :rtype: bool

.. seealso::
  An example of such a user stage can be seen in the cms_public_tutorial demo repository:
  https://gitlab.cern.ch/fast-hep/public/fast_cms_public_tutorial/blob/master/cms_hep_tutorial/__init__.py

.. warning::
  Make sure that your stage can be imported by python, most likely by setting the ``PYTHONPATH`` variable to point to the containing directory.
  Then to check a stage called ``AddMyFancyVar`` and defined in a module called ``my_custom_module`` can be imported, make sure no errors are raised by doing::
    
     python -c "import my_custom_module.AddMyFancyVar"

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
