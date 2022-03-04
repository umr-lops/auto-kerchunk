#!/bin/bash
#PBS -q omp
#PBS -l select=1:ncpus=4:mem=60gb
#PBS -l walltime=10:00:00
#PBS -m e
# requires:
# - auto-kerchunk
# - dask-hpcconfig
#PBS -m e
#PBS -J 1-9

source "/appli/anaconda/versions/4.8.2/etc/profile.d/conda.sh"
which conda
#conda activate auto-kerchunk
conda activate /home/datawork-lops-iaocea/conda-env/auto-kerchunk

PATHS="/home/ref-marc/f1_e2500_agrif/"
i=0
for REGION in `ls -1 $PATHS`; do
    i=`expr $i + 1`
    if test $i = $PBS_ARRAY_INDEX; then
    
        echo "calculation for "$REGION
        
    FILE="$PATHS$REGION/best_estimate/2018/"  #UPDATE THIS LINE with the location of the files
    GLOB="*20180101*Z.nc"  #UPDATE THIS LINE with the glob of your choice
    
    TMP=$DATAWORK/tmp/$REGION


    RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/$REGION.json.zst"   #UPDATE THIS LINE on where you want the combined json to be stored
    INTAKE="/home/datawork-lops-iaocea/catalog/intake/$REGION.yaml"  #UPDATE THIS LINE on where you want the intake catalog to be stored

    # create cluster and wait for the scheduler to have started

    rm -rf scheduler_address

    python -m dask_hpcconfig create datarmor --workers 28 --pidfile scheduler_address --silent &
    until [ -f scheduler_address ]; do sleep 1; done

    date

    python -m auto_kerchunk single-hdf5-to-zarr --cluster $(cat scheduler_address) $FILE --glob $GLOB $TMP

    python -m auto_kerchunk multi-zarr-to-zarr --compression zstd "file://$TMP/*.json" $RESULT

    chmod go+w $RESULT

    python -m auto_kerchunk   create-intake --catalog-name $REGION --name $REGION "file://$RESULT" "file://$INTAKE"

    chmod go+w $INTAKE

    date

    # shut down the cluster

    python -m dask_hpcconfig shutdown $(cat scheduler_address) --silent

    rm -rf $TMP
    
fi
done
