.. _ref-The old trees2dataframes executable:

The old trees2dataframes executable
===================================

The `trees2dataframes <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/bin/trees2dataframes>`_ executable runs the first step, which is contained in `read_trees.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/read_trees.py>`_. It takes Heppy ntuples as input and is currently configured for the RA1 analysis, but the code is compatible with any flat root tree.

The event data are summarised in the output data frames, which are configured to contain all the information required for the analysis. **In particular, the cutflow, binning, event weighting, systematics, and any additional variables (derived from those in the input tree) are defined in this step.** 

The code is executed as follows::

 python bin/trees2dataframes --components DYJetsToLL_M50_HT100to200_madgraphMLM \
 /afs/cern.ch/work/b/bkrikler/FAST/trees/20170129_Summer16_newJECs/AtLogic_MCSummer16_SM

The event loop is controlled by :ref:`ref-alphatwirl`, and you will see a progress bar::

 44.43% :::::::::::::::::                       |    63662 /   143294 |:  DYJetsToLL_M50_HT100to200_madgra

Here we are running over a single DY sample (*component*), which should complete in under a minute. It takes under 1 hour to run over the entire set of data and MC heppy ntuples via HTCondor.

When it has finished, it will print some of the output data frames. By default, the output data frames are saved in :file:`output/`, and are named as follows:

* :file:`tbl_heppyresult.txt`: information about the input files
* :file:`tbl_dataset.txt`: dataframe containing the input CMS dataset for each sample (*component*)
* :file:`tbl_xsec.txt`: dataframe containing the xsec for each component, needed in later steps. Note, where there are two samples in the same phase space (e.g. when there is an extension), both must have the same xsec.
* :file:`tbl_nevt.txt`: dataframe containing the unweighted and weighted number of events in each component, needed in later steps
* :file:`cut_flow_table.txt` :  cut flow table for each signal and control region

The remaining text files are the data frames summarising the yield of each sample in each of the defined bins (denoted by the column labels defined in the :ref:`ref-binning` section). There is one data frame per systematic variation (currently limited to pileup weights):

* :file:`tbl_n.component.region.ht_low.mht_low.njet.nbjet.--unweighted.txt`
* :file:`tbl_n.component.region.ht_low.mht_low.njet.nbjet.--weight_pileup_up.txt`
* :file:`tbl_n.component.region.ht_low.mht_low.njet.nbjet.--nominal.txt`
* :file:`tbl_n.component.region.ht_low.mht_low.njet.nbjet.--weight_pileup_down.txt`

The data frame names are constructed from the names of the columns that they contain.

The output data frames look like ::

 component  region  ht_low     mht_low  njet  nbjet          n       nvar
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200        -inf     2      0  25.082336  27.154705
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200        -inf     2      1   1.200176   1.440423
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200        -inf     2      2   0.000000   0.000000
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200        -inf     3      0   6.432789   6.966186
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200        -inf     3      1   1.176027   1.383040
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200        -inf     3      2   0.000000   0.000000
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200        -inf     4      0   0.000000   0.000000
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200        -inf     4      1   0.000000   0.000000
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200  200.000000     2      0  25.547156  27.312325
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200  200.000000     2      1   1.065602   1.135508
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200  200.000000     2      2   0.000000   0.000000
 DYJetsToLL_M50_HT100to200_madgraphMLM  Signal     200  200.000000     3      0   3.548164   4.198738
 ...

The first 6 columns contain `categorical` data, while the last two are `numerical` and give the event count and its variance for each bin (`category`).

Later, in :ref:`ref-t2df_combine_mc_components`, we’ll want to merge the several components that make up each physics process: first, any extensions of a sample in the same phase space need to be combined, and second all the phase space regions for each process need to be combined. The mapping between component, phase space, and process is stored in the repository in this manually created data frame:

* `fast_ra1/trees_to_dataframe/inputs/tbl_cfg_component_phasespace_process.txt <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/inputs/tbl_cfg_component_phasespace_process.txt>`_.

In the remainder of this section, we describe how to set up the cutflow, binning, event weighting, systematics, and additional variables.


.. _ref-cuts:

cuts
----

The cuts defining all signal and control region(s) are described in `cut_flow.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/cut_flow.py>`_. The cuts are specified in a dictionary as follows (in this case, the baseline selection that is applied to all events)::

 baseline_selection = dict(All = ( 
         'ev : ev.nJet40[0] >= 2',
         'ev : ev.nJet100[0] >= 1',
         'ev : ev.ht40[0] >= 200',
         'ev : ev.mht40_pt[0] >= 130',
         'ev : ev.nIsoTracksVeto[0] <= 0',
         #'ev : ev.nJet40failIdEta5p0[0] == 0',
         'ev : ev.nJet40Fwd[0] == 0',
         'ev : ev.jet_pt[0] > 100',
         'ev : -2.5 < ev.jet_eta[0] < 2.5',
         'ev : 0.1 <= ev.jet_chHEF[0] < 0.95',
         'ev : ev.MhtOverMet[0] < 1.25',
         # 'ev : ev.failed_alphatools[0]',
     ))


The variable names (such as ``nJet40``) correspond to the branch names of the input root tree, except for those which are calculated by :ref:`ref-scribblers` (such as ``MhtOverMet``). To run on a different type of tree (e.g. nanoAOD), we would simply change them to the corresponding branch names.

`Or` cuts are specified by changing ``All`` to ``Any`` (which is interpreted by :ref:`ref-alphatwirl`)::

 signal_selection = dict(All = (
                 'ev : ev.ra1_cutflow[0] == "Signal"',
                 dict(Any=(
                     'ev : 200 < ev.ht40[0] <= 250 and ev.alphaT[0] > 0.65',
                     'ev : 250 < ev.ht40[0] <= 300 and ev.alphaT[0] > 0.60',
                     'ev : 300 < ev.ht40[0] <= 350 and ev.alphaT[0] > 0.55',
                     'ev : 350 < ev.ht40[0] <= 400 and ev.alphaT[0] > 0.53',
                     'ev : 400 < ev.ht40[0] <= 900 and ev.alphaT[0] > 0.52',
                 )),
             ))

Each cutflow dictionary can be used as sub-component of another, here used to define the whole event selection cutflow (defining all signal and control regions)::

 event_selection = dict(All = (
     baseline_selection,
     dict(Any=(signal_selection,
          single_mu_selection,
          double_mu_selection
         )),
 ))

The cutflow table for each component is saved in :file:`cut_flow_table.txt`, e.g.::

                             component depth     class                                                   name   pass  total
 DYJetsToLL_M50_HT100to200_madgraphMLM     1 AllwCount                                                    All    296 143294
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                               "ev : ev.nJet40[0] >= 2" 101777 143294
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                              "ev : ev.nJet100[0] >= 1"  97185 101777
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                               "ev : ev.ht40[0] >= 200"  35580  97185
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                           "ev : ev.mht40_pt[0] >= 130"   6156  35580
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                       "ev : ev.nIsoTracksVeto[0] <= 0"   1173   6156
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                            "ev : ev.nJet40Fwd[0] == 0"    796   1173
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                              "ev : ev.jet_pt[0] > 100"    796    796
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                      "ev : -2.5 < ev.jet_eta[0] < 2.5"    663    796
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                   "ev : 0.1 <= ev.jet_chHEF[0] < 0.95"    598    663
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 LambdaStr                         "ev : ev.MhtOverMet[0] < 1.25"    296    598
 DYJetsToLL_M50_HT100to200_madgraphMLM     1 AnywCount                                                    Any     67    296
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 AllwCount                                                    All     67    296
 DYJetsToLL_M50_HT100to200_madgraphMLM     3 LambdaStr                 "ev : ev.ra1_cutflow[0] == \"Signal\""    278    296
 DYJetsToLL_M50_HT100to200_madgraphMLM     3 AnywCount                                                    Any     67    278
 DYJetsToLL_M50_HT100to200_madgraphMLM     4 LambdaStr "ev : 200 < ev.ht40[0] <= 250 and ev.alphaT[0] > 0.65"     61    278
 DYJetsToLL_M50_HT100to200_madgraphMLM     4 LambdaStr "ev : 250 < ev.ht40[0] <= 300 and ev.alphaT[0] > 0.60"      5    217
 DYJetsToLL_M50_HT100to200_madgraphMLM     4 LambdaStr "ev : 300 < ev.ht40[0] <= 350 and ev.alphaT[0] > 0.55"      1    212
 DYJetsToLL_M50_HT100to200_madgraphMLM     4 LambdaStr "ev : 350 < ev.ht40[0] <= 400 and ev.alphaT[0] > 0.53"      0    211
 DYJetsToLL_M50_HT100to200_madgraphMLM     4 LambdaStr "ev : 400 < ev.ht40[0] <= 900 and ev.alphaT[0] > 0.52"      0    211
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 AllwCount                                                    All      0    229
 DYJetsToLL_M50_HT100to200_madgraphMLM     3 LambdaStr               "ev : ev.ra1_cutflow[0] == \"SingleMu\""      0    229
 DYJetsToLL_M50_HT100to200_madgraphMLM     2 AllwCount                                                    All      0    229
 DYJetsToLL_M50_HT100to200_madgraphMLM     3 LambdaStr               "ev : ev.ra1_cutflow[0] == \"DoubleMu\""      0    229




.. _ref-binning:

binning
-------

The analysis binning is defined in `df_builder.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/df_builder.py#L30>`_ . This file configures the structure of the output dataframes as follows::

 base = dict(
             keyAttrNames=('componentName', 'ra1_cutflow', 'ht40', 'mht40_pt', 'nJet40', 'nBJet40'),
             keyOutColumnNames=('component', 'region', 'ht_low', 'mht_low', 'njet', 'nbjet'),
             binnings=(component, region, htbin, mhtbin, njetbin, nbjetbin),
             )

``keyAttrNames`` contains the list of branch names (of the input root tree) to be used to define bins (`categories`) in the dataframe. As in :ref:`ref-cuts`, we can also use any derived quantity names defined by :ref:`ref-scribblers` (e.g. ``ra1_cutflow``).

``keyOutColumnNames`` gives the corresponding column labels to be used in the dataframe.

``binnings`` gives the binning to be used for each quantity. The variables containing the binning are defined on `lines 17--27 <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/df_builder.py#L17>`_, for example::

 htbin = Binning(boundaries=range(200, 2000, 50))

 nbjetbin = Echo()

The ``range`` is specified as (lower,upper,stepsize).
The ``Echo()`` function means that every unique value encountered will be given its own bin -- in this case N different bins for events with 1 to N b-jets.
Various other binning functions are available in alphatwirl, such as automatic uniform and log-scale uniform binning with ``Round(width)`` and ``RoundLog(width)``: https://github.com/alphatwirl/alphatwirl/tree/master/alphatwirl/binning .

An example data frame illustrating the chosen binning can be seen above in Section :ref:`ref-The old trees2dataframes executable`.


.. _ref-systematics:

weights and systematics
-----------------------

The event weights and corresponding systematics are also set up in `df_builder.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/df_builder.py#L48>`_ .

The weights and their systematic variations are calculated by :ref:`ref-scribblers`, and for now only ``weight_pileup`` is implemented.

The product of all defined weights is used for the nominal event weight, and each weight is varied up and down to estimate the corresponding systematic.


.. _ref-scribblers:

scribblers
----------

The scribblers are initialised in `trees_to_dataframe/scribblers/__init__.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/scribblers/__init__.py>`_::

 scribblers = [
     ComponentName(),
     DivideNumpyArrays(['mht40_pt', 'met_pt'],'MhtOverMet'),
     Ra1CutFlowId(),
 ]
 
 if not isdata:
     scribblers.append(PileupReweight(pileup_file))


Scribblers perform some operation per event on variable(s) available in the input root tree, and output new variable(s). For example:

* ``ComponentName()`` maps the Heppy event.component.name to the componentName column
* ``DivideNumpyArrays`` divides the contents of the ``mht40_pt`` and ``met_pt`` branches to produce ``MhtOverMet``
* ``Ra1CutFlowId()`` converts the region ID number ``cutflowId[0]`` into a string (named ``ra1_cutflow``):  https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/scribblers/ra1_cut_flow_id.py
* ``PileupReweight()`` calculate the pileup weight from ``event.nVert[0]``: https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/scribblers/pile_up_weight.py
* Other weightings and their systematic variations will be handled similarly: https://github.com/alphatwirl/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/scribblers/systematic.py
    
Many examples of additional scribblers are available at https://github.com/alphatwirl/scribblers/ , which can be tried by importing them in `trees_to_dataframe/scribblers/__init__.py <https://gitlab.cern.ch/fast-cms/FAST-RA1/blob/master/fast_ra1/trees_to_dataframe/scribblers/__init__.py>`_.

For example, the components of jet 4-momenta from a flat tree (with branch names following the naming convention ``prefix_variable``, e.g. ``jet_pt``, ``jet_eta``, and ``jet_phi``) can be recombined into a jet object::

    from scribblers.heppy import ArraysIntoObjectZip

    scr_ = [
        ArraysIntoObjectZip(
            in_array_prefix = 'jet',
            in_array_names = ['pt', 'eta', 'phi'],
            out_obj = 'JetObject'
            )
    ]
    scribblers.extend(scr_)


Operations can be performed on this object (here, a pt and eta selection is applied to the ``JetObject`` array to produce the ``Jet40Object`` array)::

    from scribblers.selection import ObjectSelection
    scr_ = [
        ObjectSelection(
            in_obj = 'JetObject',
            out_obj = 'Jet40Object',
            selection = alphatwirl.selection.build_selection(
                path_cfg = dict(All = ('o : o.pt >= 40', 'o : -5 <= o.eta <= 5'))
            )
        )
    ]
    scribblers.extend(scr_)


Finally, the object can be converted (`flattened`) back to numerical variables which can be used to define :ref:`ref-cuts` or :ref:`ref-binning` (in this case, ``jet40_pt``, ``jet40_eta``, and ``jet40_phi``, and also ``njet40`` derived from the length of the array)::

    from scribblers.obj import Flatten
    from scribblers.essentials import Len
    scr_ = [
        Flatten(
            in_obj = 'Jet40Object',
            in_attr_names = ['pt', 'eta', 'phi'],
            out_array_prefix = 'jet40'
        ),
        Len(src_name = 'jet40_pt', out_name = 'njet40'),
    ]
    scribblers.extend(scr_)






.. _ref-alphatwirl:

alphatwirl
----------

The trees2dataframes executable is based on `alphatwirl <https://github.com/alphatwirl/alphatwirl>`_.

Each operation (event selection, scribblers, creation of output dataframes) is specified in the form of a reader_collecter_pair.

The `reader` part operates during the loop over events, and reads and defines any operations on the input data (e.g. applying event selection or a scribbler operation).
The `collector` part collects results from readers, then combines and delivers them.
Scribblers have a null collector.

Basic documentation of the functions is available at http://alphatwirl.readthedocs.io/en/latest/


