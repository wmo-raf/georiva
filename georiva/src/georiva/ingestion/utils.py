from typing import Generator

import numpy as np

from georiva.core.unit_utils import ureg


def apply_unit_conversion(data: np.ndarray, source_unit=None, output_unit=None) -> np.ndarray:
    if not source_unit or not output_unit or source_unit == output_unit:
        return data
    quantity = ureg.Quantity(data, source_unit.pint_unit)
    return np.asarray(quantity.to(output_unit.pint_unit).magnitude, dtype=np.float32)


def iter_windows(
        width: int,
        height: int,
        block_size: int = 2048
) -> Generator[tuple[int, int, int, int], None, None]:
    """
    Yield (x_offset, y_offset, width, height) windows for chunked processing.
    """
    for y in range(0, height, block_size):
        h = min(block_size, height - y)
        for x in range(0, width, block_size):
            w = min(block_size, width - x)
            yield x, y, w, h
