.. _ref-cli:

Command-line Usage
==================
The command-line tools are the primary way to use fast-carpenter and friends at this point.
All of the FAST commands provide built-in help by providing the ``--help`` option.

.. _ref-cli_fast_curator:

``fast_curator``
----------------
The fast-curator package handles the description of the input datasets.
These are saved as `YAML <https://en.wikipedia.org/wiki/YAML>`_ files, which contain a dictionary that lists the different datasets, the list of files for each dataset, and additional meta-data.

You can build these files semi-automatically by using the ``fast_curator`` command.
This can be called once per dataset and given a wildcarded expression for the input files of this dataset, which it will then expand, build some simple summary meta-data (number of events, number of files, etc) and write this to an output YAML file.
If the output file already exists, and is a valid fast-curator description, the new information will be appended to the existing file.

For an in-depth description of the dataset description files, see the `fast-curator pages <https://gitlab.cern.ch/fast-hep/public/fast-curator>`_.

.. command-output:: fast_curator --help


.. _ref-cli_fast_curator_check:

``fast_curator_check``
----------------------

.. command-output:: fast_curator_check --help


.. _ref-cli_fast_carpenter:

``fast_carpenter``
------------------

.. command-output:: fast_carpenter --help

.. 1. Generate an input file list, by either:
.. 
..    * Using the ``t2df_find_files`` command, for local files or files on xrootd (but not published to DAS),  eg: ::
.. 
..        t2df_find_files.py -o file_list.txt \
..                           --mc \
..                           -d dataset_name \
..                           root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/ttH_HToInvisible_M125_13TeV_powheg_pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/*.root
.. 
..    * Using the das_query script under: https://github.com/shane-breeze/CMS-DAS-Query, for files published to DAS (eg official productions)
.. 
