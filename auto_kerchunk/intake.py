from intake.catalog import Catalog  # noqa: F401
from intake.catalog.local import LocalCatalogEntry


def create_catalog_entry(name, description, url, storage_options={}):
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
    storage_options = {"fo": url} | storage_options
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
