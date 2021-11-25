from __future__ import annotations

import pathlib
from typing import Optional

import rich.console
import typer

from .compression import CompressionAlgorithms

app = typer.Typer()
console = rich.console.Console()


@app.callback()
def cli_main_options(
    cluster_name: str = typer.Option(
        None, "--cluster", help="Run dask operations on this cluster"
    ),
    cluster_options: str = typer.Option("", help="Additional cluster settings"),
    cluster_workers: int = typer.Option(8, help="spawn N workers"),
):
    if cluster_name is not None:
        import ifremer_clusters
        from distributed import Client

        options = dict(item.split("=") for item in cluster_options.split(";") if item)
        with console.status("[green] Starting cluster", spinner="dots") as status:
            status.update(
                status="[green] Starting cluster: connecting to {cluster_name!r}"
            )
            cluster = ifremer_clusters.cluster(cluster_name, **options)
            console.log("connected to the cluster")

            status.update(status="[green] Starting cluster: spawn workers")
            cluster.scale(cluster_workers)
            console.log("workers spawned")

            status.update(status="[green] Starting cluster: creating client")
            client = Client(cluster)
            console.log(f"client: dashboard link at {client.dashboard_like}")
            console.log(
                "client: using these packages:",
                client.get_versions(packages=["dask_jobqueue"]),
            )

        console.print(f"[bold blue]cluster {cluster_name} started successfully")


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
    import dask.array

    arr = dask.array.ones(shape=(10000, 10000, 100), chunks=(10, 10, 100))
    console.print("computing the sum of:", arr)
    console.print("result:", arr.sum().compute())
