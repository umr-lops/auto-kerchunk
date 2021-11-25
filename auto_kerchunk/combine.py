import blosc
from kerchunk.combine import MultiZarrToZarr


def combine_json(paths, outpath, compression=None):
    mzz = MultiZarrToZarr(
        paths,
        remote_protocol="file",
        xarray_open_kwargs={
            "decode_cf": False,
            "mask_and_scale": False,
            "decode_times": False,
            "decode_timedelta": False,
            "use_cftime": False,
            "decode_coords": False,
        },
        xarray_concat_args={
            "combine_attrs": "drop_conflicts",
            "data_vars": "minimal",
            "coords": "minimal",
            "compat": "override",
            # "concat_dim": "time",
            "dim": "time",
        },
    )
    # no templates because that is way too slow for a lot of files
    mzz.translate(outpath, template_count=None)

    if compression is not None:
        data = outpath.read_bytes()
        outpath.write_bytes(blosc.compress(data, typesize=8, cname=str(compression)))
