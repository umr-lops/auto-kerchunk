import itertools
import re

import fsspec.compression
import fsspec.utils
import ujson
from kerchunk.combine import MultiZarrToZarr


def extract_timestamp(url, regex):
    from dateutil.parser import parse

    match = regex.search(url)
    if not match:
        return None

    dt = parse(match.group(0))

    return dt.replace(tzinfo=None)


def sliced(seq, n):
    steps = range(0, len(seq), n)
    previous, next_ = itertools.tee(steps, 2)
    next_ = itertools.islice(next_, 1, None)
    slices_ = (
        slice(start, stop) for start, stop in itertools.zip_longest(previous, next_)
    )

    for slice_ in slices_:
        yield seq[slice_]


def compute_name(series, freq):
    min_ = series.min()
    max_ = series.max()

    if min_ == max_:
        return str(min_)

    return f"{min_}-{max_}"


def group_urls(urls, timestamp_regex, frequency):
    if frequency.isdigit():
        # group by numbers
        groups = (
            (f"group_{index}", group)
            for index, group in enumerate(sliced(urls, int(frequency)))
        )
    else:
        import pandas as pd

        regex = re.compile(timestamp_regex)

        # group by time
        df = pd.DataFrame(
            [(extract_timestamp(url, regex), url) for url in urls],
            columns=["timestamp", "urls"],
        ).assign(
            name=lambda df: df.timestamp.dt.to_period(frequency.replace("MS", "M"))
        )

        groups = (
            (compute_name(data.name, frequency), data.urls.to_list())
            for _, data in df.resample(frequency, on="timestamp")
        )

    return groups


def load_json(fs, url, **so):
    so = {"mode": "rb"} | so
    with fs.open(url, **so) as f:
        return ujson.load(f)


def infer_compression_extension(compression):
    compressions = {name: ext for ext, name in fsspec.utils.compressions.items()}

    ext = compressions.get(compression)
    if ext and isinstance(ext, list):
        return ext[0]

    return ext


def combine_json(paths, outpath, compression=None):
    if compression == "none":
        compression = None

    if compression:
        ext = infer_compression_extension(compression)
        if ext and not outpath.suffix.endswith(ext):
            outpath = outpath.with_suffix(f"{outpath.suffix}.{ext}")

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
        compress = fsspec.compression.compr[compression]
        with open(outpath, mode="wb") as f:
            f = compress(f, mode="wb")
            f.write(data)

    return outpath
