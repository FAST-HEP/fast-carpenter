.. _ref-to binned dataframes:

From trees to binned dataframes
===============================
* Produce initial "binned" dataframes that we then combine and manipulate to produce analysis results.
* "Back-end" of the code in this step uses AlphaTwirl.
* Two executables of interest: ``nanoaod2dataframes_cfg`` and ``trees2dataframes_cfg`` (these might be renamed and merged in future).
* Each one receives a config file to define how many binned dataframes are made, how they are binned, and what event selection should be applied (if any).


trees2dataframes_cfg
--------------------
* This command processes a set of Heppy-produced trees as inputs.
* The directory where the heppy output directories are located is supplied as a command-line argument.
* The command identifies which sub-directories are valid Heppy components.
* By default it will run over all of these, but you can limit this using the ``--components`` option.
* To control the way the trees are processed see "`the config file format`_"
* See :ref:`ref-The old trees2dataframes executable` for more details although much of this is out of date.

Example command:
::

   trees2dataframes_cfg -o outputs \
                        --components="SMS-T1bbbb_mGluino-1300_mLSP-1100_25ns SMS-T2tt_mStop-450_mLSP-400_25ns" \
                        input_heppy_output_directory/ \
                        config.yaml

nanoaod2dataframes_cfg
----------------------
1. Generate an input file list, by either:

   * Using the ``t2df_find_files`` command, for local files or files on xrootd (but not published to DAS),  eg: ::

       t2df_find_files.py -o file_list.txt \
                          --mc \
                          -d dataset_name \
                          root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/ttH_HToInvisible_M125_13TeV_powheg_pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/*.root

   * Using the das_query script under: https://github.com/shane-breeze/CMS-DAS-Query, for files published to DAS (eg official productions)

2. Setup the config file (see  :ref:`ref_t2df_config`) 
3. Run the nanoaod2dataframes_cfg command: ::

      nanoaod2dataframes_cfg --mode htcondor \
                             --dont-use-nanoaodtools \
                             -o outputs/ \
                             --components file_list.txt \
                             config.yaml

Including NanoAODTools to apply scale factors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
1. Set up the nanoAODTools code within CMSSW: 
   `CMS Twiki workbook <https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookNanoAOD#Recipe_for_CMSSW_9_4_X_and_the_c>`_
2. Run ``cmsenv`` within CMSSW, and then source the FAST-RA1 setup.sh script
3. Run the ``nanoaod2dataframes_cfg`` command and make sure ``--dont-use-nanoaodtools`` is *NOT* provided.

The next time you set up FAST-RA1, make sure to run ``cmsenv`` *before* you ``source ./setup.sh``


Difference to `trees2dataframes_cfg`: 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* Specify files and datasets to be processed in another config file, as opposed to giving the directory on the command line.
* Number of events processed, process cross-sections, etc aren't automatically
  summarized in output dataframes.  You need to make these yourself for later
  analysis stages (this might be automated in the future).
* The standard set of scribblers is different
* Branch names will typically be different to those from Heppy (eg. ``Jet_pt`` vs ``jet_pt``)


.. _ref_t2df_config:

The config file format
----------------------
The config file is written in `YAML <https://en.wikipedia.org/wiki/YAML>`_ format.  In essence it is a large dictionary to describe how we should process our data.

An example config file looks like:

.. literalinclude:: demo_cfg.yml
    :linenos:
    :language: yaml


Other examples can be seen in `fast_ra1/trees_to_dataframe/configs <https://gitlab.cern.ch/fast-cms/FAST-RA1/tree/master/fast_ra1/trees_to_dataframe/configs>`_.

Note that since this is a YAML file, things like anchors and block syntax are totally valid, which can be helpful, especially to define "aliases" or reuse certain parts of a config.
In addition, note that since YAML is a superset of JSON, it's perfectly valid to use a JSON file instead.

Stages
^^^^^^

The most important section is the *stages* section.  The list contained in this section describes what steps will be taken with the data, and in what order.
At the moment there are two types of stage available, and these should be provided for each stage so we can validate their descriptions later on.
The two types of stage available (at this time) are:

1. ``CutFlow`` -- Apply a series of selections or filters to remove events from being processed in later stages, and
2. ``BinnedDataframe`` -- Produce a dataframe file with a binning based on attributes of the event.
3. ``Scribbler`` -- Create new quantities and add these to the event.

Each stage must then have a complete description provided.
To do this, make a new section in the yaml file with the name provided in the `stages` section.

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
| ``counter_weights`` | ``1.0``      | | How should each event that is cut be weighted?  Only used if ``counter == True``.  Can be either a |
|                     |              | | a float, the name of an event attribute or a dictionary with the name to include in the output     |
|                     |              | | filename, and the event attribute to be used.  Can be omitted.                                     |
+---------------------+--------------+------------------------------------------------------------------------------------------------------+
| ``lambda_arg``      | ``"ev"``     | How should we call the event in the strings describing the individual cuts. Can be omitted.          |
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
