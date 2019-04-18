Key Concepts
============

Goals of fast-carpenter
-----------------------
From the user's perspective, fast-carpenter's principal goal is to help them ask **"what do I want to see"** as opposed to **"how do I implement this"** which has been the more traditional way of thinking for a particle physics analyst.

In that sense, most of the control of this code is "declarative" in the sense that a user should typically not have to say how to process 



* Produce initial "binned" dataframes that we then combine and manipulate to produce analysis results.
* "Back-end" of the code in this step uses AlphaTwirl.
* Two executables of interest: ``nanoaod2dataframes_cfg`` and ``trees2dataframes_cfg`` (these might be renamed and merged in future).
* Each one receives a config file to define how many binned dataframes are made, how they are binned, and what event selection should be applied (if any).


