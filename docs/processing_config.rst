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
    In-depth discussion of the built-in stages and their configuration can be found on the ``fast_carpenter`` module page: :mod:`fast_carpenter`, or directly at:

      * :py:class:`fast_carpenter.Define`
      * :py:class:`fast_carpenter.SystematicWeights`
      * :py:class:`fast_carpenter.CutFlow`
      * :py:class:`fast_carpenter.SelectPhaseSpace`
      * :py:class:`fast_carpenter.BinnedDataframe`

.. todo::
   Build that list programmatically, so its always up to date and uses the built-in docstrings for a description.

.. At this point, I would love to link to the docementation for the known classes in a more methodical way, since they're all imported to fast_carpenter.__init__.py
   I'd need a special directive which produces a list of links to each class' documentation, and ideally adds the first line of the docstrings as a simple summary.
   Useful links for that task: 
    * http://www.sphinx-doc.org/en/master/extdev/appapi.html#sphinx.application.Sphinx.add_directive
    * http://docutils.sourceforge.net/docs/howto/rst-directives.html
   These methods are close, but not quite there:

.. .. toctree::
..    :maxdepth: 3
.. 
..    api/fast_carpenter

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

.. todo::
  Describe the collector and merge methods to allow a user stage to save results to disk.
