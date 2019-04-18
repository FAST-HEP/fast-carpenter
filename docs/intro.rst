Introduction
============

The FAST-RA1 code performs all the main operations of the analysis workflow, starting from root trees of event data and outputting data cards ready for limit setting in combine.

The executables are found in `bin <https://gitlab.cern.ch/fast-cms/FAST-RA1/tree/master/bin>`_.

The :ref:`ref-trees2dataframes` (t2df) steps produce the data frame analogue of the set of signal and control region histograms used in a typical analysis.

The :ref:`ref-dataframe2datacards` (df2dc) steps convert these data frames into data cards for limit setting in combine.

The main code executed by the binaries is found in `fast_ra1 <https://gitlab.cern.ch/fast-cms/FAST-RA1/tree/master/fast_ra1>`_.
