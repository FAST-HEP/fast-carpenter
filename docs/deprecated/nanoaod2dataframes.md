# nanoaod2dataframes

## Setup instructions

To run the nanoaod2dataframes executable with the nanoaod-tools modules you will
have to follow the following procedure.

1. Setup CMSSW and NanoAODTools as per instructions on the
   [CMS Twiki workbook](https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookNanoAOD#Recipe_for_CMSSW_9_4_X_and_the_c)

2. Create a dataset dataframe which will specify which DAS datasets to run over
   (You will need your grid certificate setup for this). For example:

   ```
   das_query -o data/dataset_ttjets.txt "/TTJets_TuneCUETP8M1_*/*05Feb2018*/NANOAODSIM"
   ```

   For tests you can remove some of the files to speed-up the processing time.

   NB: If the nanoaod files are located on your local file system you will have to
   create the dataframe yourself. However, you can use the above command to create
   a 'template' and replace the filelist as required.

3. Setup NanoTwirl
   Clone the FAST-RA1 repository and checkout the correct branch (currently
   sbreeze/FAST-RA1:master). Update the submodules then run the setup script:
   ```
   source setup.sh
   ```

4. Run NanoTwirl with profiling (You will need your grid certificate setup for
   this):

   ```
   python bin/nanoaod2dataframes -o output --components data/dataset_ttjets.txt --ncores 0 --profile
   ```

   The profile command will generate a `prof.txt` file with the profiling
   breakdown. With a few adjustments you can export this as a dataframe for ease of
   manipulation.
