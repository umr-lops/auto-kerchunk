import os
import pathlib

import ujson
from kerchunk.hdf5 import SingleHdf5ToZarr


def parse_url(url):
    pattern = "://"
    if pattern not in url:
        return "", url

    scheme, path = url.split(pattern)
    return scheme, path


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

    if relative_to is None:
        return url, outroot.joinpath(name)

    _, root = parse_url(os.fspath(relative_to))
    relpath = path.relative_to(root.path)
    return url, outroot / relpath.with_name(name)


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
        data = ujson.dumps(h5chunks.translate()).encode()

    with open(outpath, "wb") as outf:
        outf.write(data)
