import enum
import operator

import fsspec.compression

names = [str(n).lower() for n in fsspec.compression.compr.keys()]
CompressionAlgorithms = enum.Enum("CompressionAlgorithms", {n: n for n in names})
CompressionAlgorithms.__str__ = operator.attrgetter("name")
