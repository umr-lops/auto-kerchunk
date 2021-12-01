import itertools
import re

import blosc
from kerchunk.combine import MultiZarrToZarr

from .utils import parse_url


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


def compute_url(url, name):
    """compute the output url of a group from a url and group data"""
    scheme, path = parse_url(url)
    return f"{scheme}://{path.rstrip('/')}/{name}"


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
