# `auto-kerchunk`

Create data repositories using `kerchunk`.

## dependencies

required dependencies:
- kerchunk
- ujson
- h5py
- zarr
- fsspec
- dask
- rich
- typer

optional dependencies:
- zstandard
- distributed
- intake
- intake-xarray



#  How to install

Create your conda enviroment with following command;

```bash
conda create -c conda-forge -n auto-kerchunk python=3.9
conda install -c conda-forge -n auto-kerchunk mamba
conda activate auto-kerchunk
mamba install xarray kerchunk ujson h5py zarr  fsspec  dask rich  typer zstandard intake intake-xarray -c conda-forge
```

Then install auto-kerchunk to your enviroment as;
ATT: use your extranet login instead of to1efa9

```bash

python -m pip install dask-hpcconfig

python -m pip install git+https://to1efa9@gitlab.ifremer.fr/iaocea/auto-kerchunk.git

```

If you use auto-kerchunk from jupyterlab, and want to install the kernel,
```bash
ipython kernel install --user --name=auto-kerchunk --display-name=auto-kerchunk
```

## Usage

### simple usage
Simplest way to use auto-kerchunk is to use pbs submitting script.
First copy the example.
```bash
cp examples/auto_kerchunk.pbs
cat auto_kerchunk.pbs  |grep -i marc
FILES="file:///home/ref-marc/f1_e2500_agrif/MARC_F1-MARS3D-SEINE/best_estimate/*/*Z.nc"
NAME="marc_f1_2500_agrif_seine_hourly"
```
Then update the enviroment variable 'FILES' as path to your original netcdf data sets
and 'NAME' as the name you would like to call the catalogue.

### advanced usage

You can observe how your computation is going on with dask.  To do so you can start a jupyterlab on datarmor with a

dask-dashboard extension, start one terminal, then execute auto-chunk as follows.

```bash
TMP=$TMPDIR/JSONS
FILES="file:///home/ref-marc/f1_e2500_agrif/MARC_F1-MARS3D-SEINE/best_estimate/*/*Z.nc"
NAME="marc_f1_2500_agrif_seine_hourly"
CATALOGNAME=$NAME
RESULT="/home/datawork-lops-iaocea/catalog/kerchunk/"$NAME".json.zst"
INTAKE="file:///home/datawork-lops-iaocea/catalog/intake/"$NAME".yaml"

python -m auto_kerchunk   --workers 14  single-hdf5-to-zarr $FILES $TMP
python -m auto_kerchunk   --workers 14  multi-zarr-to-zarr --compression zstd "file://$TMP/*.json" $RESULT
python -m auto_kerchunk   create-intake --catalog-name $CATALOGNAME --name  $NAME "file://$RESULT" $INTAKE
```
