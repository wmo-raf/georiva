"""One GRIB2 message, written by hand, for a store whose chunks are GRIB.

A virtual store's chunks are references to GRIB messages in a producer's
bucket, decoded by the codec the store names (``gribberish``). A test that
proves a tile is drawn through that codec needs a real message, and nothing in
the lock writes GRIB, so this writes the smallest honest one: a regular
latitude/longitude grid, scanned west to east and north to south from 0°E, its
values packed simply (template 5.0) with no bitmap.

The values must be whole numbers in 0…65535 above the smallest: they are
packed in 16 bits with no scaling, so they decode exactly and a test can
assert a pixel's value rather than a range.
"""

import struct

import numpy

#: Discipline 0 (meteorological), category 0 (temperature), number 0: what
#: gribberish calls ``TMP``.
TEMPERATURE = (0, 0, 0)


def _signed(value: int, octets: int) -> bytes:
    """GRIB's signed integers are sign and magnitude, not two's complement."""
    sign = 1 << (8 * octets - 1)
    return (abs(value) | (sign if value < 0 else 0)).to_bytes(octets, "big")


def _micro(degrees: float) -> int:
    return round(degrees * 1_000_000)


def message(values, *, latitudes, longitudes, parameter=TEMPERATURE) -> bytes:
    """A GRIB2 message holding ``values`` (rows north to south, columns west to
    east from ``longitudes[0]``) on the grid ``latitudes`` × ``longitudes``."""
    values = numpy.asarray(values)
    rows, columns = values.shape
    assert (rows, columns) == (len(latitudes), len(longitudes))
    discipline, category, number = parameter

    identification = struct.pack(">IBHHBBBHBBBBBBB", 21, 1, 7, 0, 2, 1, 1, 2026, 3, 23, 0, 0, 0, 0, 1)

    di = _micro(abs(longitudes[1] - longitudes[0]))
    dj = _micro(abs(latitudes[0] - latitudes[1]))
    grid_template = (
        bytes([6, 0])
        + (0).to_bytes(4, "big")
        + bytes([0])
        + (0).to_bytes(4, "big")
        + bytes([0])
        + (0).to_bytes(4, "big")
        + columns.to_bytes(4, "big")
        + rows.to_bytes(4, "big")
        + (0).to_bytes(4, "big")
        + (0xFFFFFFFF).to_bytes(4, "big")
        + _signed(_micro(latitudes[0]), 4)
        + _signed(_micro(longitudes[0]), 4)
        + bytes([48])
        + _signed(_micro(latitudes[-1]), 4)
        + _signed(_micro(longitudes[-1]), 4)
        + di.to_bytes(4, "big")
        + dj.to_bytes(4, "big")
        + bytes([0])  # scanning: west to east, north to south, rows consecutive
    )
    grid = struct.pack(">IBBIBBH", 14 + len(grid_template), 3, 0, rows * columns, 0, 0, 0) + grid_template

    product_template = bytes([category, number, 2, 0, 96]) + struct.pack(">HBBI", 0, 0, 1, 0)
    product_template += bytes([103, 0]) + (2).to_bytes(4, "big") + bytes([255, 0]) + (0).to_bytes(4, "big")
    product = struct.pack(">IBHH", 9 + len(product_template), 4, 0, 0) + product_template

    reference = float(values.min())
    packed = (values - reference).round().astype(">u2").tobytes()
    representation = struct.pack(">IBIH", 21, 5, rows * columns, 0) + struct.pack(">f", reference)
    representation += _signed(0, 2) + _signed(0, 2) + bytes([16, 0])

    bitmap = struct.pack(">IBB", 6, 6, 255)
    data = struct.pack(">IB", 5 + len(packed), 7) + packed

    body = identification + grid + product + representation + bitmap + data + b"7777"
    return b"GRIB" + bytes([0, 0, discipline, 2]) + (16 + len(body)).to_bytes(8, "big") + body
