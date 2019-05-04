.. _ref-philosophy:

Key Concepts
============

Goals of fast-carpenter
-----------------------

From the user's perspective
^^^^^^^^^^^^^^^^^^^^^^^^^^^
fast-carpenter's principal goal is to help a user ask **"what do I want to see"** as opposed to **"how do I implement this"** which has been the more traditional way of thinking for a particle physics analyst.

In that sense, most of the control of this code is "declarative" in the sense that a user should typically not have to say how to move data through the analysis, only what they want it to do.
That way fast-carpenter can make decisions behind the scenes as to how to handle this. 
Python dictionaries are therefore the main way to configure carpenter, which we describe using `YAML <https://en.wikipedia.org/wiki/YAML>`_.

The net result of this should mean:
 * What the user writes is closer to the actual mathematical description of what they want to do.
 * There is less actual analysis "code" and so less opportunity to put bugs in the analysis.
 * It should be quick to do a simple study, and then scale smoothly to a full-blown analysis.
 * Python dictionaries can be built easily in other tools, and so fast-carpenter directly called from inside a Jupyter notebook, for example.
 * When you want to do something more exotic, which is not (yet) catered for in fast-carpenter itself, there is an easy "plugin" style system to add your own custom code into the processing.

Although fast-carpenter is focussed on input ROOT trees at this point (which inspires its name), this may well evolve in the future.

From the code and development perspective
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* We have tried to make the code as modular as possible (hence fast-flow and fast-curator not being contained in this package).
* Wherever possible, we've tried to avoid writing code; if an existing package does that task, use it.
* If another tool provides most but not all of a wanted functionality, we prefer contributing to that package over putting code in here.
* Keep functions simple (e.g. under the mccabe metrics).
* Unit-tests should be clear, and are as important as documentation; ideally the two can serve a similar purpose.

Overall approach for data-processing
------------------------------------
fast-carpenter is intended to be the first step in the main analysis pipeline.  
It is expected to be the only part of the processing chain which sees "event-level" data, and produces the necessary summary of this in a tabular form (which invariably means binned as histograms).
Subsequent steps can then manipulate these to produce final analysis results, such as graphical figures, or doing some functional fit to the binned data.

For public examples of working with fast-carpenter and the other FAST tools, see :ref:`ref-example_repos`.

Step 1: Create dataset configs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
fast-carpenter needs to know what input files to use, and will often need extra metadata (for example, does this data represent real or simulated data).
This is where the `fast-curator <https://gitlab.cern.ch/fast-hep/public/fast-curator>`_ package comes in.
It provides the `fast_curator` command which we use to generate descriptions of the input files in a format that is both human and machine readable, using `YAML <https://en.wikipedia.org/wiki/YAML>`_.
These can then be put in a repository and updated periodically with extra meta-data, or when new data becomes available.

See the :ref:`ref-cli_fast_curator` section of the command lines tools for in-depth discussion of the fast_curator command which can automate the process of making these configs.

Step 2: Write a processing config
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The next step is to prepare the "processing configuration" which defines what you want to do with this data.
This has to be written by hand, and is the core of what you want to spend time on as an analyzer.
Behind the scenes, this config file is interpreted using the small `fast-flow <https://gitlab.cern.ch/fast-hep/public/fast-flow>`_ package; documentation there might also be of interest.

For details about how to write this config file, see :ref:`ref-processing_config`.

Step 3: Run fast_carpenter
^^^^^^^^^^^^^^^^^^^^^^^^^^
You can now run the fast_carpenter command, giving it the dataset and processing configurations from the previous steps.
Depending on how many files you have and what type of computing resources (e.g. multiple cpus, batch systems) you might want to use some of the different processing modes.

For more on how to use the ``fast_carpenter`` command, see the :ref:`ref-cli_fast_carpenter` section of the command line tools.

Step 4: Produce plots
^^^^^^^^^^^^^^^^^^^^^
In order to visualise the results of running ``fast_carpenter``, you can use the ``fast-plotter`` package, which gives you both a command-line interface and helper python functions to 
produce plots from the dataframes.
Fine tuning of the command-line plots is again possible using a YAML configuration file.

See the `dedicated fast-plotter documentation <http://fast-plotter.readthedocs.io/>`_ for more guidance on this package.
