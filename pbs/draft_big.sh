#!/bin/bash
#PBS -q omp
#PBS -l select=1:ncpus=4:mem=120gb
#PBS -l walltime=10:00:00
#PBS -m e
# requires:
# - auto-kerchunk
# - dask-hpcconfig
#PBS -m e
#PBS -J 1-16

source "/appli/anaconda/versions/4.8.2/etc/profile.d/conda.sh"
which conda
#conda activate auto-kerchunk
conda activate /home/datawork-lops-iaocea/conda-env/auto-kerchunk

PATHS="/home/ref-marc/"
i=0

for DATA in `ls -1 $PATHS`; do
    i=`expr $i + 1`
    if test $i = $PBS_ARRAY_INDEX; then
    
        echo "calculation for "$DATA
        
    FILE="$PATHS$DATA/best_estimate/"  #UPDATE THIS LINE with the location of the files
    GLOB="*/*Z.nc"  #UPDATE THIS LINE with the glob of your choice
    
    TMP=$DATAWORK/tmp/$DATA

    RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/$DATA"   #UPDATE THIS LINE on where you want the combined json to be stored
    INTAKE="/home/datawork-lops-iaocea/catalog/intake/$DATA.yaml"  #UPDATE THIS LINE on where you want the intake catalog to be stored

    # create cluster and wait for the scheduler to have started

    CATALOGNAME=$DATA

    rm -rf scheduler_address

    # python -m dask_hpcconfig create datarmor --workers 28 --pidfile scheduler_address --silent &
    # until [ -f scheduler_address ]; do sleep 1; done

    date

    python -m auto_kerchunk single-hdf5-to-zarr $FILE --glob $GLOB $TMP

    python -m auto_kerchunk multi-zarr-to-zarr --compression zstd --freq "1Y" "file://$TMP/*.json" $RESULT

    chmod go+w $RESULT

    python -m auto_kerchunk   create-intake --catalog-name $CATALOGNAME --name $DATA "file://$RESULT" "file://$INTAKE"

    chmod go+w $INTAKE

    date

    # shut down the cluster

    # python -m dask_hpcconfig shutdown $(cat scheduler_address) --silent

    rm -rf $TMP
    
fi
done
