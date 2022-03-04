# This file is meant to explain how the script `index_draft.sh` is supposed to be used


The for loop can be used to create files that shares an attributes, in the draft its the REGION because the files are split in different regions but still share the same path pattern in order to just run 1 script instead of running 9 for the 9 different regions.

## To correctly use the script you will first need to update the following lines  
------------------------------------------------------------------------------------------------
    PBS -J 1-9

>- This line is creating the job array and so you will need as many jobs as directory.
    In the draft case, there is 9 diferent regions, so 9 job array.

    PATHS="/home/ref-marc/f1_e2500_agrif/"

>- Update your path to the directory were the path splits.

    FILE="$PATHS$REGION/best_estimate/2018/"

>- Update this variable to reach the files.

    GLOB="*20180101*Z.nc"

>- Update this variable to set the glob that will list all the file you want to combine to json format.

    RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/$REGION.json.zst"

>- Update this variable with the path you want the combined json file to be stored, be careful and don't remove the $REGION.json.zst

    INTAKE="/home/datawork-lops-iaocea/catalog/intake/$REGION.yaml"

>- Update this variable with the path you want the intake catalog to be stored, be careful and don't remove the $REGION.yaml

## There is an other variable that you can modify if you want but that's not mandatory
    TMP=$DATAWORK/tmp/$REGION 
    
>- This variable is the place where the individual json files will be created, the script remove them in the end so you can modify this variable with the path that you want them to be write and remove the line rm -rf $TMP if you don't want the individual json to be deleted
