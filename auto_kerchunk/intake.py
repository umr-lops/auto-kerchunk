from intake.catalog import Catalog  # noqa: F401
from intake.catalog.local import LocalCatalogEntry


def create_catalog_entry(name, description, url,freq,model,positive):
    """create a catalog entry

    Parameters
    ----------
    name : str
        name of the catalog entry
    description : str
        additional description of the catalog
    url : str
        url to the kerchunk metadata file
    freq : str
        freq of the each kerchunk metadata file
    model : str
        model parameter required for osdyn
    positive : str
        positive parameter requried for osdyn
    """
    target_protocol = "file"  # hard-coded for now, extract in the future
    # hard-coded for now, try to detect using the filename
    compression = "zstd" if url.endswith(".zst") else None
    #url = "\""+url+"/{{ year }}.json.zst\"" if freq == '1Y' else url
    url = url+"/{{ year }}.json.zst" if freq == '1Y' else url
    metadata={}
    #metadata={ }
    if freq == '1Y' :
        metadata["parameters"]= {
                 "year" : {
                 "default": "2010"
                 #"allowed": ['2010', '2011', '2012', '2013', '2014']
                 },
                 }
    if not model ==None :metadata["model"]= model
    if not positive ==None :metadata["positive"]= positive

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
    return entry, metadata
