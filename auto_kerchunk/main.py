from __future__ import annotations

import itertools
import pathlib
from glob import has_magic
from typing import Optional

import dask
import dask.diagnostics
import fsspec
import rich.console
import typer
from rich.table import Table

from .compression import CompressionAlgorithms

app = typer.Typer()
console = rich.console.Console()


def format_client_versions(versions) -> Table:
    def format_environment(env):
        table = Table(box=None)
        table.add_column("")
        table.add_column("value")
        for p, v in sorted(env.items()):
            table.add_row(str(p), str(v))
        return table

    def format_system(system):
        # reformat to a table of package → system
        table = Table.grid(padding=1)
        table.add_column("environment", justify="center", style="bold")
        table.add_column("info")

        for name, env in system.items():
            subtable = format_environment(env)
            table.add_row(name, subtable)

        return table

    table = Table.grid(padding=1, pad_edge=True)
    table.add_column("system", no_wrap=True, justify="center", style="bold red")
    table.add_column("values")
    for section in ["client", "scheduler"]:
        data = versions.get(section)
        if data is None:
            continue

        subtable = format_system(data)
        table.add_row(section, subtable)

    return table


@app.callback()
def cli_main_options(
    cluster_name: str = typer.Option(
        None, "--cluster", help="Run dask operations on this cluster"
    ),
    cluster_options: str = typer.Option("", help="Additional cluster settings"),
    workers: int = typer.Option(8, help="spawn N workers"),
    atleast_workers: int = typer.Option(
        4, help="wait for at least N workers to have spawned"
    ),
):
    if cluster_name is not None:
        import ifremer_clusters
        from distributed import Client

        options = dict(item.split("=") for item in cluster_options.split(";") if item)
        with console.status("[bold blue] Starting cluster", spinner="point") as status:
            status.update(
                status=f"[bold blue] Starting cluster:[/] [white]connecting to {cluster_name!r}"
            )
            cluster = ifremer_clusters.cluster(cluster_name, **options)
            console.log("connected to the cluster")

            status.update(
                status="[bold blue] Starting cluster:[/] [white]creating client"
            )
            client = Client(cluster)
            console.log(f"client: dashboard link: {client.dashboard_link}")

            status.update(
                status="[bold blue] Starting cluster:[/] [white]spawn workers"
            )
            cluster.scale(workers)
            client.wait_for_workers(n_workers=min(workers, atleast_workers))
            console.log(f"at least {atleast_workers} workers spawned")

            console.log(
                format_client_versions(client.get_versions()),
            )

        console.print(f"[green]cluster {cluster_name} started successfully")


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
):
    """extract the metadata from HDF5 files and write it to separate files"""
    from . import convert

    def glob_url(fs, url, default):
        if fs.isfile(url):
            return url

        if has_magic(url):
            globbed = url
        # TODO: handle archive urls
        else:
            globbed = url.rstrip("/") + "/" + default

        console.log("globbing:", globbed)
        return [f"{fs.protocol}://{p}" for p in fs.glob(globbed)]

    with console.status("[blue bold] extracting metadata", spinner="dots") as status:
        status.update(
            "[blue bold] extracting metadata: [/][white]collecting input files"
        )
        protocols = {convert.parse_url(url)[0] or "file" for url in urls}
        if len(protocols) != 1:
            console.log("[red bold] reading using multiple protocols is not supported")
            raise SystemExit(1)

        (protocol,) = protocols
        fs = fsspec.filesystem(protocol)
        all_urls = list(
            itertools.chain.from_iterable(glob_url(fs, url, glob) for url in urls)
        )
        console.log(f"collected {len(all_urls)} input files")

        status.update(
            "[blue bold] extracting metadata:[/] [white]preparing computation"
        )
        tasks = dict(
            convert.compute_outpath(u, root, relative_to=relative_to) for u in all_urls
        )
        console.log("constructed output paths")
        for parent in {p.parent for p in tasks.values()}:
            parent.mkdir(exist_ok=True, parents=True)
        console.log("created output folders")

        dsk = [dask.delayed(convert.gen_json_hdf5)(fs, u, p) for u, p in tasks.items()]
        console.log("done constructing the task graph")
        status.update("[blue bold] extracting metadata:[/] [white]computing ...")

    with dask.diagnostics.ProgressBar():
        _ = dask.compute(dsk)

    console.print("[green bold] metadata extraction successfully completed")


@app.command("multi-zarr-to-zarr")
def cli_multi_zarr_to_zarr(
    urls: list[str] = typer.Argument(..., help="input urls / paths"),
    outpath: pathlib.Path = typer.Argument(..., help="output file"),
    relative_to: Optional[pathlib.Path] = None,
    compression: Optional[CompressionAlgorithms] = typer.Option(
        None,
        help="compress the files using this algorithm. Don't compress by default or if an empty string was passed.",
    ),
    freq: Optional[str] = typer.Option(
        None,
        help="divide the files into groups (only for files divided by time for now). Can either be the size of the group or a frequency like '6M'.",
    ),
):
    from . import combine

    combine.combine_json(urls, outpath)
