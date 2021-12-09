from intake.catalog import Catalog  # noqa: F401
from intake.catalog.local import LocalCatalogEntry


def create_catalog_entry(name, description, url):
    """create a catalog entry

    Parameters
    ----------
    name : str
        name of the catalog entry
    description : str
        additional description of the catalog
    url : str
        url to the kerchunk metadata file
    """
    target_protocol = "file"  # hard-coded for now, extract in the future
    # hard-coded for now, try to detect using the filename
    compression = "zstd" if url.endswith(".zst") else None
    storage_options = {
        "fo": url,
        "target_protocol": target_protocol,
        "target_options": {
            "compression": compression,
        },
    }
    entry = LocalCatalogEntry(
        name=name,
        description=description,
        driver="zarr",
        args={
            "urlpath": "reference://",
            "storage_options": storage_options,
            "consolidated": False,
        },
    )
    return entry
