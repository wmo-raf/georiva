"""Shared fixtures for tests that need styled variables.

The same two-style arrangement — a default and a non-default alternate whose
colormaps open on tellably different colors — recurs across every surface
that serves styling: the machine plane, STAC, EDR and the dataset pages. One
helper, in the pattern of ``organisations.testing``, so the arrangement
cannot drift between the suites guarding those surfaces.
"""

#: An alternate palette whose 256-entry colormap opens on a color no other
#: fixture here uses — tests tell styles apart by that first entry.
ANALYST_STOPS = [{"value": 0.0, "color": "#0000ff"},
                 {"value": 50.0, "color": "#00ff00"}]


def make_style(variable, slug="official", *, is_default=True, stops=None):
    """A named ``VariableStyle`` on ``variable``; black→red stops by default."""
    from georiva.core.models import VariableStyle

    return VariableStyle.objects.create(
        variable=variable, name=slug.title(), slug=slug, is_default=is_default,
        stops=stops or [{"value": 0.0, "color": "#000000"},
                        {"value": 50.0, "color": "#ff0000"}],
    )
