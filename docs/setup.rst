Installing
==========
From Pypi
---------
The simplest way to install things is from pypi.
::

        pip install fast-carpenter

**Note: In general it's better to install this in a specific environment (e.g. using** `virtualenv <https://virtualenv.pypa.io/en/stable/>`_ **or better still** `conda <https://docs.conda.io/en/latest/miniconda.html>`_ **).**

Otherwise you might need to use the ``--prefix`` or ``--user`` options for ``pip install``.
If you do provide any of these options, make sure you have the ``/bin`` directory in your ``PATH`` (e.g. if you used ``--user`` this will be mean you need ``~/.local/bin`` included in the PATH).
Using virtaulenv or conda will avoid this complication

From Source
-----------
If you want to edit the code or contribute something back, you might want to install things directly from gitlab.
Any of these options should work:

Directly with pip:
::

        pip install -e 'git+https://gitlab.cern.ch/fast-hep/public/fast-carpenter.git#egg=fast_carpenter' --src .

Clone first and install:
::

        git clone https://gitlab.cern.ch/fast-hep/public/fast-carpenter.git
        cd fast-carpenter
        python setup.py develop

