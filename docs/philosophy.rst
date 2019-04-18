Key Concepts
============

Goals of fast-carpenter
-----------------------

From the user's perspective
^^^^^^^^^^^^^^^^^^^^^^^^^^^
fast-carpenter's principal goal is to help a user ask **"what do I want to see"** as opposed to **"how do I implement this"** which has been the more traditional way of thinking for a particle physics analyst.

In that sense, most of the control of this code is "declarative" in the sense that a user should typically not have to say how to move data through the analysis, only what they want it to do.
That way fast-carpenter can make decisions behind the scenes as to how to handle this. 

The net result of this should mean:
 * What the user writes is closer to the actual mathematical description of what they want to do.
 * There is less actual analysis "code" and so less opportunity to put bugs to the analysis.
 * It should be quick to do a simple study, and scale smoothly to a full-blown analysis.
 * Although the primary interface so far is through the command-line, the commands use simple python functions which can be directly called from inside other scripts or inside a Jupyter notebook.
 * When you want to do something more exotic, which is not (yet) catered for in fast-carpenter itself, there is an easy "plugin" style system to add your own custom code into the processing.

From the code and development perspective
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
* We have tried to make the code as modular as possible (hence fast-flow and fast-curator not being contained in this package).
* Wherever possible, we've tried to avoid writing code; if an existing or upcoming package does that task, use it.
