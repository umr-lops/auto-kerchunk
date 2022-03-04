# This file is meant to explain how the script `grid.sh` is supposed to be used

This script creates a json file and add in the end the location to the grid_file in order to have them in the same intake catalog.

## To correctly use the script you will first need to update the following lines  
------------------------------------------------------------------------------------------------
    FILE="/home/ref-marc/f1_e2500_agrif/*SEINE*/best_estimate/2018/"

>- Update this variable to reach the files.

    NAME="testseine"

>- Update this variable with the name you want for the file

    GLOB="/*Z.nc"

>- Update this variable to set the glob that will list all the file you want to combine to json format.

    GRIDNAME=marc_f1-mars3d_grid

>- Update this variable with the name of the grid.

    GRID="/home/datawork-lops-iaocea/catalog/grid/marc_f1-mars3d-seine-grid.nc"

>- Update this variable with the path to the grid file

    MODEL="mars"
    
>- Update this variable with the name of the model that provided you your data

    RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/$NAME"

>- Update this variable with the path you want the combined json file to be stored, be careful and don't remove the $NAME

    INTAKE="/home/datawork-lops-iaocea/catalog/intake/$NAME.yaml"

>- Update this variable with the path you want the intake catalog to be stored, be careful and don't remove the $NAME.yaml

-----
## There is an other variable that you can modify if you want but that's not mandatory
    TMP=$DATAWORK/tmp/$NAME 
    
>- This variable is the place where the individual json files will be created, the script remove them in the end so you can modify this variable with the path that you want them to be write and remove the line rm -rf $TMP if you don't want the individual json to be deleted.
