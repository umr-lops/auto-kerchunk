#!/bin/bash
#PBS -q mpi_1
#PBS -l walltime=10:00:00
#PBS -m e
# requires:
# - auto-kerchunk
# - dask-hpcconfig
# execute:
#   qsub create_yaml.sh


source "/appli/anaconda/versions/4.8.2/etc/profile.d/conda.sh"
which conda
conda activate auto-kerchunk

# !!! USER SETTINGS !!!

# Select the files of interest
FILE="/home/ref-marc/f1_e2500_agrif/*SEINE*/best_estimate/201*/"  #UPDATE THIS LINE with the location of the files
GLOB="*201*T0100Z.nc"  #UPDATE THIS LINE with the glob of your choice

# Set the path to the grid if any
GRID="/home/datawork-lops-iaocea/catalog/grid/marc_f1-mars3d-seine-grid.nc" #UPDATE THIS LINE with THE PATH to the gridfile you want 

# Specify some global attributes (for C-grid output only)
MODEL="mars" #UPDATE THIS LINE with the name of the model
POSITIVE="up" # up or down. up is default. Tina suggest this to be takeout later with update of osdyn.

# Set json and yaml file names
FREQ=1Y
NAME="seine_yearly"
RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/$NAME.json.zst"   #UPDATE THIS LINE on where you want the combined json to be stored
INTAKE="/home/datawork-lops-iaocea/catalog/intake/seine_yearly.yaml"  #UPDATE THIS LINE on where you want the intake catalog to be stored

# !!! END USER SETTINGS !!!

TMP=$DATAWORK/tmp/$NAME
#TMP=$TMPDIR/tmp/$NAME

GRIDNAME=${NAME}_grid 
CATALOGNAME=$NAME

rm -rf scheduler_address

# create cluster and wait for the scheduler to have started
python -m dask_hpcconfig create datarmor-local --workers 14 --pidfile scheduler_address --silent &
until [ -f scheduler_address ]; do sleep 1; done

date

# create a .json metadata file for each file of interest
python -m auto_kerchunk single-hdf5-to-zarr --cluster $(cat scheduler_address) $FILE --glob $GLOB $TMP

# create .json.zst
python -m auto_kerchunk multi-zarr-to-zarr --freq $FREQ --compression zstd "file://$TMP/*.json" $RESULT

chmod go+w $RESULT

# create .yaml related to the selected dataset
python -m auto_kerchunk   create-intake --catalog-name  $CATALOGNAME --name $NAME --freq $FREQ  --model $MODEL --positive $POSITIVE  "file://$RESULT" "file://$INTAKE"

chmod go+w $INTAKE

date

# shut down the cluster
python -m dask_hpcconfig shutdown $(cat scheduler_address) --silent

rm -rf $TMP

# Add metadata in the .yaml (for C-grid output only). Make sure the variables are set.
#sed -i '1d' $INTAKE
#if [ -n "$MODEL" ]; then
##  sed -i "1s/^/metadata:\n    model:  $MODEL \n    positive:  $POSITIVE \n/" $INTAKE
#fi

# Add the grid file as a new source if any  (see user setting part)
if [ -n "$GRID" ]; then
  echo "
    $GRIDNAME:
      args:
        urlpath: $GRID
      description: description
      driver: netcdf
      name: $GRIDNAME" >> $INTAKE
fi


