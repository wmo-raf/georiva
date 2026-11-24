from itertools import count


def free_slug(base: str, taken, *, stem: str | None = None) -> str:
    """``base`` when it is not ``taken``; else the first free of ``stem``,
    ``stem-2``, ``stem-3``….

    ``stem`` defaults to ``base`` itself, which is taken, so the first
    candidate is then ``base-2``.
    """
    if base not in taken:
        return base
    if stem is not None and stem not in taken:
        return stem
    stem = stem or base
    return next(candidate for n in count(2) if (candidate := f"{stem}-{n}") not in taken)
