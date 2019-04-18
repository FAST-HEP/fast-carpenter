Command-line Usage
==================

.. _ref-cli_fast_curator:

fast_curator
------------

.. _ref-cli_fast_curator_check:

fast_curator_check
------------------

.. _ref-cli_fast_carpenter:

fast_carpenter
--------------

The command-line tools are the primary entry points for the codebase at this point.

1. Generate an input file list, by either:

   * Using the ``t2df_find_files`` command, for local files or files on xrootd (but not published to DAS),  eg: ::

       t2df_find_files.py -o file_list.txt \
                          --mc \
                          -d dataset_name \
                          root://cms-xrd-global.cern.ch//store/mc/RunIISummer16MiniAODv2/ttH_HToInvisible_M125_13TeV_powheg_pythia8/MINIAODSIM/PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/*.root

   * Using the das_query script under: https://github.com/shane-breeze/CMS-DAS-Query, for files published to DAS (eg official productions)

