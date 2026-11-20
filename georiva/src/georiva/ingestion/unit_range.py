"""Re-expressing a value range in another unit (the push-feed setup surfaces)."""

from django.http import JsonResponse
from django.utils.translation import gettext as _

from georiva.core.unit_utils import Conversion, ConversionRefused, ureg


def same_unit(a: str, b: str) -> bool:
    """Whether two unit symbols name the same unit ('kelvin' and 'K' do)."""
    try:
        return ureg(a).units == ureg(b).units
    except Exception:
        return a.strip().lower() == b.strip().lower()


def convert_range(vmin: float, vmax: float, from_symbol: str, to_symbol: str, *, water: bool = False) -> tuple:
    """A range across a unit change, by the conversion a Variable would be
    served through (ADR 0046 §4, §6). :class:`ConversionRefused` when it would be refused."""
    conversion = Conversion.between(from_symbol, to_symbol, water=water)
    return conversion.forward(vmin), conversion.forward(vmax)


def _symbol(request, prefix: str) -> str:
    """A unit named either by ``<prefix>_id`` (a Unit pk) or ``<prefix>_symbol``."""
    from georiva.core.models import Unit

    unit_id = request.GET.get(f"{prefix}_id")
    if unit_id:
        unit = Unit.objects.filter(pk=unit_id).first()
        return unit.symbol if unit else ""
    return (request.GET.get(f"{prefix}_symbol") or "").strip()


def convert_range_view(request):
    """JSON: the posted min/max moved from one unit to another, for the setup tables."""
    try:
        vmin, vmax = float(request.GET["min"]), float(request.GET["max"])
    except (KeyError, ValueError):
        return JsonResponse({"error": str(_("Enter a numeric range first."))}, status=400)

    from_symbol, to_symbol = _symbol(request, "from"), _symbol(request, "to")
    if not from_symbol or not to_symbol:
        return JsonResponse({"error": str(_("Unknown unit."))}, status=400)
    try:
        low, high = convert_range(vmin, vmax, from_symbol, to_symbol, water=request.GET.get("water") == "1")
    except ConversionRefused as refused:
        return JsonResponse({"error": str(refused)}, status=400)
    return JsonResponse({"value_min": low, "value_max": high})
