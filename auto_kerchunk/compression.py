import enum

import blosc

CompressionAlgorithms = enum.Enum(
    "CompressionAlgorithms", {name: name for name in blosc.cnames}
)
CompressionAlgorithms.__str__ = lambda self: self.name
