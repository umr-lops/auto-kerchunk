import itertools
import os
import pathlib

import ujson
from kerchunk.hdf import SingleHdf5ToZarr

from .utils import parse_url


def compute_outpath(url, outroot, *, relative_to=None, type="json"):
    """construct the outpath for the metadata file

    Parameters
    ----------
    url : str or path-like
        url or path to the file
    outroot : path-like
        path to the directory where the new file should be placed
    relative_to : str or path-like, optional
        url or path to the root of the input file. If given, the
        outpath will follow the same directory structure. By default,
        the file will be placed directly in `outroot`.
    type : str, default: "json"
        file extension for the output file name

    Returns
    -------
    url : str
        the input url. If the input was a path-like (or a string
        containing a file path), the result will be prefixed with
        'file://'.
    outpath : path-like
        the path to the new file

    Examples
    --------
    >>> url, outpath = compute_outpath(
    ...     "file:///home/ref-marc/f1_e2500/best_estimate/2011/abc.nc",
    ...     "metadata/single",
    ... )
    >>> url
    'file:///home/ref-marc/f1_e2500/best_estimate/2011/abc.nc'
    >>> str(outpath)
    'metadata/single/abc.nc.json'

    Using `relative_to`:

    >>> url, outpath = compute_outpath(
    ...     "file:///home/ref-marc/f1_e2500/best_estimate/2011/abc.nc",
    ...     "metadata/single",
    ...     relative_to="file:///home/ref-marc",
    ... )
    >>> url
    'file:///home/ref-marc/f1_e2500/best_estimate/2011/abc.nc'
    >>> str(outpath)
    'metadata/single/f1_e2500/best_estimate/2011/abc.nc.json'
    """
    scheme, path = parse_url(os.fspath(url))
    if not scheme:
        url = f"file://{path}"

    path = pathlib.Path(path)
    name = f"{path.name}.{type}"

    if not isinstance(outroot, os.PathLike):
        outroot = pathlib.Path(outroot)

    outroot = outroot.absolute()

    if relative_to is None:
        return url, outroot.joinpath(name)

    _, root = parse_url(os.fspath(relative_to))
    relpath = path.relative_to(root)
    return url, outroot / relpath.with_name(name)


def correct_fill_values(data):
    def fix_variable(values):
        zattrs = values[".zattrs"]

        if "_FillValue" not in zattrs:
            return values

        _FillValue = zattrs["_FillValue"]
        if values[".zarray"]["fill_value"] != _FillValue:
            values[".zarray"]["fill_value"] = _FillValue

        return values

    refs = data["refs"]
    prepared = (
        (tuple(key.split("/")), value) for key, value in refs.items() if "/" in key
    )
    filtered = (
        (key, ujson.loads(value))
        for key, value in prepared
        if key[1] in (".zattrs", ".zarray")
    )
    key = lambda i: i[0][0]
    grouped = (
        (name, {n[1]: v for n, v in group})
        for name, group in itertools.groupby(sorted(filtered, key=key), key=key)
    )
    fixed = ((name, fix_variable(var)) for name, var in grouped)
    flattened = {
        f"{name}/{item}": ujson.dumps(data, indent=4)
        for name, var in fixed
        for item, data in var.items()
    }
    data["refs"] = dict(sorted((refs | flattened).items()))
    return data


def gen_json_hdf5(fs, url, outpath, **storage_options):
    """extract the metadata of a HDF5 file and save it

    Parameters
    ----------
    fs : fsspec.filesystem
        The fsspec filesystem object that should be used to open the url
    url : str
        The url to the input HDF5 file
    outpath : path-like
        The path to the output file
    **storage_options
        Additional options to `fs.open`.
    """
    so = {"mode": "rb"} | storage_options

    with fs.open(url, **so) as inf:
        h5chunks = SingleHdf5ToZarr(inf, url, inline_threshold=300)
        metadata = h5chunks.translate()
        metadata = correct_fill_values(metadata)
        bytes_ = ujson.dumps(metadata).encode()

    with open(outpath, "wb") as outf:
        outf.write(bytes_)
