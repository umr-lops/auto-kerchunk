# This file is meant to explain how the script `draft_big.sh` is supposed to be used


The for loop can be used to create files that shares an attributes, in the draft its the DATA because the files are split in different kind of datas from different sources.
This script convert the files and then divide them by year creating a file for each year.
You will need to make sure that the files that you want to convert are located on the following way:

    ~/dir1/dir2/*/dir3/files.nc

This exemple just states that you need to have the same architecture before and after the directory that splits the file so all the directories inside of dir2 must be shaped the same way.
If not, the script can't reach the files and can't convert them.

## To correctly use the script you will first need to update the following lines  
------------------------------------------------------------------------------------------------
    PBS -J 1-16

>- This line is creating the job array and so you will need as many jobs as directory.
    In the draft case, there is 16 diferent type of datas, so 16 job array.

    PATHS="/home/ref-marc/"

>- Update your path to the directory were the path splits.

    FILE="$PATHS$DATA/best_estimate/"

>- Update this variable to reach the files.

    GLOB="*/*Z.nc"

>- Update this variable to set the glob that will list all the file you want to combine to json format.

    RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/$DATA"

>- Update this variable with the path you want the combined json file to be stored, be careful and don't remove the $DATA

    INTAKE="/home/datawork-lops-iaocea/catalog/intake/$DATA.yaml"

>- Update this variable with the path you want the intake catalog to be stored, be careful and don't remove the $DATA.yaml

## There is an other variable that you can modify if you want but that's not mandatory
    TMP=$DATAWORK/tmp/$DATA 
    
>- This variable is the place where the individual json files will be created, the script remove them in the end so you can modify this variable with the path that you want them to be write and remove the line rm -rf $TMP if you don't want the individual json to be deleted.
