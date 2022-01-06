#!/bin/bash
#PBS -q mpi_1
#PBS -l walltime=24:00:00
#PBS -l select=1:ncpus=28:mem=128000mb
#PBS -m e
#mamba create -n auto-kerchunk python==3.9 xarray kerchunk ujson h5py zarr  fsspec  dask rich  typer zstandard intake intake-xarray
#conda activate auto-kerchunk
#python -m pip install .

# requires:
# - auto-kerchunk
# - dask-hpcconfig

#which conda
source "/appli/anaconda/versions/4.8.2/etc/profile.d/conda.sh"
conda activate /home/datawork-lops-iaocea/conda-env/auto-kerchunk
cd $TMPDIR

FILES="file:///home/ref-marc/f1_e2500/best_estimate/2011/*Z.nc"  #done
NAME="tinatest_marc_f1_2500_hourly"

TMP=$TMPDIR/JSONS
CATALOGNAME=$NAME

RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/$NAME.json.zst"
#INTAKE="file:///home/datawork-lops-iaocea/catalog/intake/$NAME.yaml"
INTAKE="/home/datawork-lops-iaocea/catalog/intake/$NAME.yaml"

# create cluster and wait for the scheduler to have started
python -m dask_hpcconfig create datarmor-local --workers 14 --pidfile scheduler_address --silent &
until [ -f scheduler_address ]; do sleep 1; done

date
python -m auto_kerchunk single-hdf5-to-zarr \
       --cluster $(cat scheduler_address) \
       $FILES \
       $TMP
python -m auto_kerchunk multi-zarr-to-zarr \
       --cluster $(cat scheduler_address) \
       --compression zstd \
       "file://$TMP/*.json" \
       $RESULT
chmod go+w $RESULT
python -m auto_kerchunk   create-intake \
       --catalog-name $CATALOGNAME \
       --name  $NAME \
       "file://$RESULT" \
       "file://$INTAKE"
chmod go+w $INTAKE
date

# shut down the cluster
python -m dask_hpcconfig shutdown $(cat scheduler_address) --silent
