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

Input ROOT files can also be stored on xrootd servers, with a file-path specified by the ``root://`` protocol.  
You can also provide wild-cards for such files, but make sure to check that you pick all files that you expect; wildcarded searches on xrootd directories can depend on permissions, access rights, storage mirroring and so on.

For an in-depth description of the dataset description files, see the `fast-curator pages <https://gitlab.cern.ch/fast-hep/public/fast-curator>`_.

.. command-output:: fast_curator --help


.. _ref-cli_fast_curator_check:

``fast_curator_check``
----------------------
Sometimes it can be useful to check that you're dataset config files are valid, in particular if you use the ``import`` section (which allows you to include dataset configs from another file).
The ``fast_curator_check`` command can help you by expanding such sections and dumping the result to screen or to an output file.

.. command-output:: fast_curator_check --help


.. _ref-cli_fast_carpenter:

``fast_carpenter``
------------------
The ``fast_carpenter`` is the star of the show. 
It is what actually converts your event-level datasets to the binned summaries.

The built-in help should tell you everything you need to know:

.. command-output:: fast_carpenter --help

In its simplest form therefore, you can just provide a dataset config and a processing config and run:
::

    fast_carpenter datasets.yml processing.yml

Quite often you will want to use some of the acceleration options which allow you to process the jobs more quickly using multiple cpu cores, or by running things on a batch system.
When you do this, the ``fast_carpenter`` command will submit tasks to the batch system and wait for them to finish, so you need to make sure the command is not killed in between e.g. by running fast-carpenter on a remote login node and breaking or losing the connection to that login.  Use a tool tmux or screen in such cases.

To use multiple CPUs on the local machine, use ``--mode multiprocessing`` (this is the default, so not generally needed) and specify the number of cores to use, ``--ncores 4``.
::

    fast_carpenter --ncores 4 datasets.yml processing.yml

Alternatively, if you have access to an htcondor or SGE batch system (i.e. ``qsub``), then the ``fast_carpenter`` command can submit many tasks to un at the same time using the batch system.
In this case you need to choose an appropriate option for the ``--mode`` option.  In addition the options with ``block`` in them can control how many events are processed on each task and for each dataset.

For all modes,  the ``--blocksize`` option can be helpful to present fast-carpenter reading too many events into memory in one go.
It's default value of 100,000 might be too large, in which case reducing it to some other value (e.g. 20,000) can help.
Common symptons of the blocksize being too large are:

 * Extremely slow processing, or
 * Batch jobs crashing or not being started
