from typing import Generator

import numpy as np

UNIT_CONVERSIONS = {
    'K_to_C': lambda x: x - 273.15,
    'Pa_to_hPa': lambda x: x * 0.01,
    'm_to_mm': lambda x: x * 1000.0,
    'ms_to_kmh': lambda x: x * 3.6,
    'kgm2s_to_mm': lambda x: x * 3600.0,
}


def apply_unit_conversion(data: np.ndarray, conversion: str) -> np.ndarray:
    """Apply unit conversion in-place where possible."""
    if not conversion or conversion not in UNIT_CONVERSIONS:
        return data
    return UNIT_CONVERSIONS[conversion](data)


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
