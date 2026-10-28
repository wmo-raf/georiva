"""Derived (vector) variables on a push feed: wind speed and direction from a U and a V component."""

from django.utils.text import slugify
from django.utils.translation import gettext as _
from django.utils.translation import gettext_lazy

from georiva.core.models import Variable
from georiva.core.provisioning import build_source_block, resolve_unit

TRANSFORMS = {
    Variable.TransformType.VECTOR_MAGNITUDE: gettext_lazy("wind speed"),
    Variable.TransformType.VECTOR_DIRECTION: gettext_lazy("wind direction"),
}


def derived_sources(u_source: str, v_source: str) -> list:
    """The ``sources`` value of a derived Variable: its U and V components."""
    return [build_source_block("u_component", u_source), build_source_block("v_component", v_source)]


def is_direction(transform: str) -> bool:
    return transform == Variable.TransformType.VECTOR_DIRECTION


def degree_unit():
    """Direction always comes out of the extractor in degrees, so it has one unit and nothing to convert."""
    return resolve_unit("degree")


def derived_slug(long_name: str) -> str:
    return slugify(long_name or "")


def validate_derived(spec: dict, label: str) -> list[str]:
    """Problems with a posted ``{transform, u_source, v_source}``, empty when it is sound."""
    errors = []
    if spec.get("transform") not in TRANSFORMS:
        errors.append(_("Variable '%s': choose wind speed or wind direction.") % label)
    u, v = (spec.get("u_source") or "").strip(), (spec.get("v_source") or "").strip()
    if not u or not v:
        errors.append(_("Variable '%s': choose both the U and the V component.") % label)
    elif u == v:
        errors.append(_("Variable '%s': the U and V components must be different variables.") % label)
    return errors


def read_derived(variable) -> dict | None:
    """``{transform, u_source, v_source}`` of a stored derived Variable, or None for a passthrough."""
    if variable.transform_type == Variable.TransformType.PASSTHROUGH:
        return None
    blocks = {block.block_type: block.value["source_name"] for block in variable.sources or []}
    return {
        "transform": variable.transform_type,
        "u_source": blocks.get("u_component", ""),
        "v_source": blocks.get("v_component", ""),
    }


def primary_source_name(variable) -> str:
    """The file variable a passthrough Variable reads, or ''."""
    for block in variable.sources or []:
        if block.block_type == "primary":
            return block.value["source_name"]
    return ""
