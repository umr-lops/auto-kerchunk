from __future__ import annotations

import functools
import itertools
import pathlib
from glob import has_magic
from typing import Optional

import dask
import fsspec
import rich.console
import typer
from dask.diagnostics import ProgressBar

from .compression import CompressionAlgorithms
from .utils import parse_dict_option, parse_url

app = typer.Typer()
console = rich.console.Console()


def glob_url(fs, url, default):
    if fs.isfile(url):
        return [url]

    if has_magic(url):
        globbed = url
    # TODO: handle archive urls
    else:
        globbed = url.rstrip("/") + "/" + default

    console.log("globbing:", globbed)
    return [f"{fs.protocol}://{p}" for p in fs.glob(globbed)]


@app.command("single-hdf5-to-zarr")
def cli_single_hdf5_to_zarr(
    urls: list[str] = typer.Argument(
        ...,
        help=(
            "The input files. A list of paths or urls (as understood by `fsspec`)."
            " Reading from multiple sources is not supported. If a path or url"
            " points to a directory, that directory is searched using `glob`."
        ),
    ),
    root: pathlib.Path = typer.Argument(
        ..., help="the directory to write the metadata files to."
    ),
    relative_to: str = typer.Option(
        None,
        help=(
            "if given, the metadata file paths will recreate the directory structure"
            " of the input paths relative to `relative_to`. For example, if the input"
            " path is 'a/b/c/file.nc' and `relative_to` is 'a/b', the metadata file"
            " path relative to `root` will be 'c/file.nc'."
        ),
    ),
    glob: str = typer.Option(
        "**/*.nc",
        help=("search directories using this glob"),
    ),
    chunksize: int = typer.Option(
        100,
        help="dask chunk size",
    ),
    cluster: str = typer.Option(None, help="The address of a scheduler server."),
):
    """extract the metadata from HDF5 files and write it to separate files"""
    import dask.bag as db

    from . import convert

    if cluster is not None:
        from distributed import Client

        client = Client(cluster)

        console.log("connected to:", cluster)
        console.log("dashboard at:", client.dashboard_link)

    with console.status("[blue bold] extracting metadata", spinner="dots") as status:
        status.update(
            "[blue bold] extracting metadata: [/][white]collecting input files"
        )
        protocols = {parse_url(url)[0] or "file" for url in urls}
        if len(protocols) != 1:
            console.log("[red bold] reading using multiple protocols is not supported")
            raise SystemExit(1)

        (protocol,) = protocols
        fs = fsspec.filesystem(protocol)
        all_urls = list(
            itertools.chain.from_iterable(glob_url(fs, url, glob) for url in urls)
        )

        if not len(all_urls):
            console.log("[red bold] no files found. Try setting `--glob`")
            raise SystemExit(1)
        console.log(f"collected {len(all_urls)} input files")

        status.update(
            "[blue bold] extracting metadata:[/] [white]preparing computation"
        )
        console.log(f"output files are to be written to: {root}")
        tasks = dict(
            convert.compute_outpath(u, root, relative_to=relative_to) for u in all_urls
        )
        console.log("constructed output paths")
        for parent in {p.parent for p in tasks.values()}:
            parent.mkdir(exist_ok=True, parents=True)
        console.log("created output folders")

        data = db.from_sequence(tasks.items(), partition_size=chunksize)
        dsk = data.starmap(functools.partial(convert.gen_json_hdf5, fs))
        console.log("created the task graph")

        status.update("[blue bold] extracting metadata:[/] [white]computing ...")

    with ProgressBar():
        console.log("starting the computation")
        _ = dask.compute(dsk)

    console.print("[green bold] metadata extraction successfully completed")


@app.command("multi-zarr-to-zarr")
def cli_multi_zarr_to_zarr(
    urls: list[str] = typer.Argument(..., help="input urls / paths"),
    outpath: pathlib.Path = typer.Argument(..., help="output path / root"),
    remote_protocol: str = typer.Option(
        "file", help="The protocol used to access the data"
    ),
    open_kwargs: str = typer.Option(
        "",
        help=(
            "options to pass to `xarray.open_dataset`. Separate keys from"
            " values by '=' and entries by ';'"
        ),
    ),
    concat_kwargs: str = typer.Option(
        "",
        help=(
            "options to pass to `xarray.concat`. Separate keys from"
            " values by '=' and entries by ';'"
        ),
    ),
    glob: str = typer.Option(
        "**/*.json", help="pattern to search directories and archives"
    ),
    compression: Optional[CompressionAlgorithms] = typer.Option(
        None,
        help=(
            "compress the files using this algorithm. Don't compress by"
            " default or if an empty string was passed."
        ),
    ),
    timestamp_regex: str = typer.Option(
        r"\d{1,4}[-.]?\d{2}[-.]?\d{2}(T\d{2}:?\d{2}(:?\d{2})?Z?)?",
        help="regex to extract timestamps from file names",
    ),
    freq: Optional[str] = typer.Option(
        None,
        help=(
            "divide the files into groups (only for files divided by time for now)."
            " Can either be the size of the group or a frequency like '6M'."
        ),
    ),
    cluster: str = typer.Option(None, help="The address of a scheduler server."),
):
    """combine the metadata of netcdf files into a single file"""
    from . import combine

    open_kwargs = parse_dict_option(open_kwargs)
    concat_kwargs = parse_dict_option(concat_kwargs)

    if cluster is not None:
        from distributed import Client

        client = Client(cluster)

        console.log("connected to:", cluster)
        console.log("dashboard at:", client.dashboard_link)

    with console.status("[blue bold] combining metadata", spinner="dots") as status:
        status.update(
            "[blue bold] combining metadata: [/][white]collecting input files"
        )
        protocols = {parse_url(url)[0] or "file" for url in urls}
        if len(protocols) != 1:
            console.log("[red bold] reading using multiple protocols is not supported")
            raise SystemExit(1)
        (protocol,) = protocols
        fs = fsspec.filesystem(protocol)
        all_urls = sorted(
            itertools.chain.from_iterable(glob_url(fs, url, glob) for url in urls)
        )

        if not len(all_urls):
            console.log("[red bold] no files found. Try setting `--glob`")
            raise SystemExit(1)
        console.log(f"collected {len(all_urls)} files")

        status.update("[blue bold] combining metadata:[/] [white]preparing tasks")
        outpath = outpath.absolute()
        if freq:
            groups = {
                outpath.joinpath(f"{name}.json"): data
                for name, data in combine.group_urls(all_urls, timestamp_regex, freq)
            }
        else:
            groups = {outpath: all_urls}
        console.log(f"determined {len(groups)} groups")

        for path in {p.parent for p in groups.keys()}:
            path.mkdir(exist_ok=True, parents=True)
        console.log("created out directories")

        preopened_files = {
            out: [dask.delayed(combine.load_json)(fs, url) for url in urls]
            for out, urls in groups.items()
        }
        tasks = [
            dask.delayed(combine.combine_json)(
                data,
                out,
                compression=compression,
                remote_protocol=remote_protocol,
                open_kwargs=open_kwargs,
                concat_kwargs=concat_kwargs,
            )
            for out, data in preopened_files.items()
        ]
        console.log("created tasks")
        # TODO: use live-view to use both spinner and progress bar
        status.update("[blue bold] combining metadata: [/][white]executing")

    with ProgressBar():
        _ = dask.compute(tasks)

    console.print(
        f"[green bold]combined {len(all_urls)} metadata files to {len(groups)} files"
    )


@app.command("create-intake")
def cli_create_intake(
    url: str = typer.Argument(..., help="the url to the kerchunk metadata file"),
    out: str = typer.Argument(..., help="the url to the catalog file"),
    catalog_name: str = typer.Option("catalog", help="name of the catalog"),
    catalog_description: str = typer.Option(None, help="description of the catalog"),
    name: str = typer.Option("source", help="the name of the catalog entry"),
    description: str = typer.Option(
        "description", help="description of the catalog entry"
    ),
    freq: Optional[str] = typer.Option(
        None,
        help=(
            "divide the files into groups (only for files divided by time for now)."
            " Can work only with '1Y' now."
        ),
    ),
    model: Optional[str] = typer.Option(
        None,
        help=(
            " set parameter model in the yaml file if not None"
        ),
    ),
    positive: Optional[str] = typer.Option(
        None,
        help=(
            " set parameter positive in the yaml file if not None"
        ),
    ),
):
    """create a intake catalog for a kerchunk metadata file

    See `single-hdf5-to-zarr` and `multi-zarr-to-zarr`.
    """
    from .intake import Catalog, create_catalog_entry

    fs, _, _ = fsspec.get_fs_token_paths(url)
    if not fs.exists(url):
        console.log("[bold red]file does not exist:[/]", url)
        raise SystemExit(1)

    console.log("creating intake catalog for kerchunk metadata file at:", url)
    entry,metadata = create_catalog_entry(name, description, url, freq, model, positive )
    print("entry")
    print(entry)
    print("metadata")
    print(metadata)

    catalog = Catalog.from_dict(
        metadata=metadata,name=catalog_name, description=catalog_description, entries={name: entry}
    )

    catalog.save(out)
    console.log("saved catalog to:", out)
    console.print("[green bold]successfully created the catalog[/]")
