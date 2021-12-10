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
- ifremer-clusters
- distributed
- intake


#  How to install

Create your conda enviroment with following command;



use


```bash
python -m pip install https://gitlab.ifremer.fr/iaocea/auto-kerchunk
```
or clone the source:
```bash
git clone https://gitlab.ifremer.fr/iaocea/auto-kerchunk
cd auto-kerchunk
```
and then install from there:
```bash
python -m pip install .
```
or as "editable":
```bash
python -m pip install -e .
```

If you use auto-kerchunk from jupyterlab, and want to install the kernel,
```bash
ipython kernel install --user --name=auto-kerchunk --display-name=auto-kerchunk
```

## Usage

It is better if you can observe how your computation is going on with dask.  Thus you can start a jupyterlab on datarmor with a daskdashboard extension, start one terminal, then execute auto-chunk as follows.  

```bash
cd $TMPDIR
(/home1/datahome/todaka/conda-env/auto-kerchunk) todaka@r2i0n15:/dev/shm/pbs.8216049.datarmor0> python -m auto_kerchunk  --cluster local --workers 14 single-hdf5-to-zarr /home/ref-marc/f2_1200_sn/best_estimate/ --glob "**/*Z.nc" $TMPDIR/f2_1200_sn/
```

```bash
(/home1/datahome/todaka/conda-env/auto-kerchunk) todaka@r2i0n15:/dev/shm/pbs.8216049.datarmor0> python -m auto_kerchunk  --cluster local --workers 14  multi-zarr-to-zarr $TMPDIR/f2_1200_sn/ /home/datawork-lops-iaocea/catalog/f2_1200_sn
```
=> taking long time here


