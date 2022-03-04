#!/bin/bash
#PBS -q omp
#PBS -l select=1:ncpus=4:mem=60gb
#PBS -l walltime=10:00:00
#PBS -m e
# requires:
# - auto-kerchunk
# - dask-hpcconfig


source "/appli/anaconda/versions/4.8.2/etc/profile.d/conda.sh"
which conda
#conda activate auto-kerchunk
conda activate /home/datawork-lops-iaocea/conda-env/auto-kerchunk


FILE="/home/ref-marc/f1_e2500_agrif/*SEINE*/best_estimate/2018/"  #UPDATE THIS LINE with the location of the files
NAME="testseine"
GLOB="*20180101*Z.nc"  #UPDATE THIS LINE with the glob of your choice

GRIDNAME=marc_f1-mars3d_grid #UPDATE THIS LINE with the name of the grid you want 
GRID="/home/datawork-lops-iaocea/catalog/grid/marc_f1-mars3d-seine-grid.nc" #UPDATE THIS LINE with THE PATH to the gridfile you want 
MODEL="mars" #UPDATE THIS LINE with the name of the model


TMP=$DATAWORK/tmp/JSONS

CATALOGNAME=$NAME

RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/$NAME.json.zst"   #UPDATE THIS LINE on where you want the combined json to be stored
INTAKE="/home/datawork-lops-iaocea/catalog/intake/$NAME.yaml"  #UPDATE THIS LINE on where you want the intake catalog to be stored

# create cluster and wait for the scheduler to have started

rm -rf scheduler_address

python -m dask_hpcconfig create datarmor --workers 28 --pidfile scheduler_address --silent &
until [ -f scheduler_address ]; do sleep 1; done

date

python -m auto_kerchunk single-hdf5-to-zarr --cluster $(cat scheduler_address) $FILE --glob $GLOB $TMP

python -m auto_kerchunk multi-zarr-to-zarr --compression zstd "file://$TMP/*.json" $RESULT

chmod go+w $RESULT

python -m auto_kerchunk   create-intake --catalog-name $CATALOGNAME --name $NAME "file://$RESULT" "file://$INTAKE"

chmod go+w $INTAKE

date

# shut down the cluster

python -m dask_hpcconfig shutdown $(cat scheduler_address) --silent

rm -rf $TMP


echo "
  $GRIDNAME:
    args:
      urlpath: $GRID
    description: $MODEL 
    driver: netcdf
    name: $GRIDNAME" >> $INTAKE


