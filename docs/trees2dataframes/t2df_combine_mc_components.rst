.. _ref-t2df_combine_mc_components:

t2df_combine_mc_components
==========================

t2df_combine_mc_components executable
-------------------------------------

The `t2df_combine_mc_components <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/bin/t2df_combine_mc_components>`_ executable runs the second step, contained in `combine_mc_components.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/combine_mc_components.py>`_.
The MC samples (`components`) often need to be combined to produce event samples of the fundamental physics process of interest. This could be either because there are seperate event samples for different regions of phase space, or because more statistics are added in a separate sample. In general, for any given normalisation of the process, the events from different samples will require different weights. This part of the analysis combines the different `components` into physics processes, including the correct weights for a given total cross section.

We start by running trees2dataframes over a set of components to give a set of dataframes we can work with::

 python bin/trees2dataframes --events-per-dataset 1000 \ 
 /afs/cern.ch/work/b/bkrikler/FAST/trees/20170129_Summer16_newJECs/AtLogic_MCSummer16_SM

The ``--events-per-dataset 1000`` option limits the processing to just the first 1000 events from each component, and is useful for testing.

When we don't specify a ``--components "x y z"`` list (as we did in Section :ref:`ref-The old trees2dataframes executable`), all components found in the given folder are processed.


We now have data frames that look like ::

 component    region  ht_low      mht_low  njet  nbjet          n       nvar
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     200         -inf     2      0   3.267774   3.563971
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     200         -inf     2      1   0.000000   0.000000
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     200         -inf     3      0   0.000000   0.000000
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     200   200.000000     2      0   2.108803   2.223554
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     200   200.000000     2      1   0.000000   0.000000
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     200   200.000000     3      0   0.000000   0.000000
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     200   400.000000     2      0   0.000000   0.000000
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     400         -inf     2      0   0.000000   0.000000
      DYJetsToLL_M50_HT100to200_madgraphMLM    Signal     400   200.000000     2      0   0.000000   0.000000
 DYJetsToLL_M50_HT100to200_madgraphMLM_ext1    Signal     200         -inf     2      0   1.202083   1.445003
 DYJetsToLL_M50_HT100to200_madgraphMLM_ext1    Signal     200         -inf     2      1   0.000000   0.000000
 DYJetsToLL_M50_HT100to200_madgraphMLM_ext1    Signal     200         -inf     3      0   2.238620   2.506928
 ...


The first 6 columns contain `categorical` data, while the last two are `numerical` and give the event count and its variance for each bin (`category`).

Apart from the pileup reweighting, these numbers are just raw MC event counts, and further manipulations are required before they will represent the real physics processes in the data:

* any extensions of a sample in the same phase space need to be combined
* each process needs to normalised to the theoretical cross section in each region of phase space
* all the phase space regions for each process need to be combined

These steps of data frame manipulation are done using `pandas` and are illustrated in https://indico.cern.ch/event/384920/contributions/1813060/attachments/768278/1053817/tai_20150430_offline_pandas.pdf .

This is performed by the :file:`bin/t2df_combine_mc_components` executable as follows::

 python bin/t2df_combine_mc_components output

where the :file:`output` folder contains the data frames we just produced. The other necessary input is the mapping between component, phase space, and process::

 cp -a fast_ra1/trees_to_dataframe/inputs/tbl_cfg_component_phasespace_process.txt output/

The output data frames are saved in the :file:`output` folder with names the same as the input, but with "component" replaced by "process": https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/combine_mc_components.py#L22



combine_mc_components (data frame manipulation in pandas)
---------------------------------------------------------

We now look at the pandas data frame manipulation in more detail.

At this stage, we can replace the data frames we just produced (only a subset of datasets and events) with pre-computed data frames summarising the complete datasets::

 cp -a /afs/cern.ch/work/b/bkrikler/FAST/t2df_demo_dataframes/trees2dataframes/* output/

and, if you haven't already done so, ::

 cp -a fast_ra1/trees_to_dataframe/inputs/tbl_cfg_component_phasespace_process.txt output/


The main functions used come from the external `aggregate <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/external/aggregate/aggregate/>`_ library.

`combine_mc_yields_in_datasets_into_xsec_in_processes() <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/external/aggregate/aggregate/combine_mc_components.py#L33>`_ carries out all the steps illustrated in https://indico.cern.ch/event/384920/contributions/1813060/attachments/768278/1053817/tai_20150430_offline_pandas.pdf::

 def combine_mc_yields_in_datasets_into_xsec_in_processes(
         tbl_yield, tbl_process, tbl_nevt, tbl_xsec,
         dataset_column = 'component', nevt_column = 'nevt'):

The input data frames to this function are the ones from :file:`output`:

* ``tbl_yield``: the input data frame, e.g. tbl_n.component.region.ht_low.mht_low.njet.nbjet.--nominal.txt
* ``tbl_process``: tbl_cfg_component_phasespace_process.txt
* ``tbl_nevt``: tbl_nevt.txt
* ``tbl_xsec``: tbl_xsec.txt

The first step is to merge tbl_process and tbl_xsec by the common columns, to add the process and phase space information to tbl_xsec::

 tbl_xsec = pd.merge(tbl_process, tbl_xsec)

Next, we merge tbl_process and tbl_yield to add the process and phase space information to tbl_yield, and `sum_over_categories <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/external/aggregate/aggregate/sum_over_categories.py>`_ (which performs a groupby operation, summing over ``dataset_column = 'component'``)::

 tbl = pd.merge(tbl_process, tbl_yield)
 tbl = sum_over_categories(tbl, categories = (dataset_column, ), variables = ('n', 'nvar'))

Now, we do the same with tbl_nevt and then merge the output data frames together:: 
 
 tbl_nevt = tbl_nevt[[dataset_column, nevt_column]]
 tbl_nevt = pd.merge(tbl_process, tbl_nevt)
 tbl_nevt = sum_over_categories(tbl_nevt, categories = (dataset_column, ), variables = (nevt_column, ))
 tbl_nevt = pd.merge(tbl_nevt, tbl_xsec)
 tbl = pd.merge(tbl, tbl_nevt)

Finally, we multiply columns to normalise each process to the xsec in each region of phase space, drop unneeded columns, and perform one more groupby operation (via ``sum_over_categories()``) to sum over the phase-space regions of each physics process::

 tbl['xsecvar'] = tbl.nvar*(tbl.xsec/tbl[nevt_column])**2
 tbl['xsec'] = tbl.n*(tbl.xsec/tbl[nevt_column])
 tbl.drop(nevt_column, axis = 1, inplace = True)
 tbl.drop(['n', 'nvar'], axis = 1, inplace = True)
 ret = sum_over_categories(tbl, categories = ('phasespace', ), variables = ('xsec', 'xsecvar'))

It is instructive to run through the above steps interactively, and watch how the data frames evolve.

We can now run these steps by executing ``t2df_combine_mc_components``::

 python bin/t2df_combine_mc_components --force output

The ``--force`` option controls whether to overwrite pre-existing output.
