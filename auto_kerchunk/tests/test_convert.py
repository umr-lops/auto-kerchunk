import pytest

from .. import convert


@pytest.mark.parametrize(
    ["url", "scheme", "path"],
    (
        pytest.param("abc.nc", "", "abc.nc", id="local filename"),
        pytest.param("a/abc.nc", "", "a/abc.nc", id="local filepath"),
        pytest.param("file://a/abc.nc", "file", "a/abc.nc", id="relative local url"),
        pytest.param("file:///a/abc.nc", "file", "/a/abc.nc", id="absolute local url"),
    ),
)
def test_parse_url(url, scheme, path):
    assert convert.parse_url(url) == (scheme, path)
