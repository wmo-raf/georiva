"""Re-expressing a value range in another unit (the push-feed setup surfaces)."""

from django.http import JsonResponse
from django.utils.translation import gettext as _

from georiva.core.unit_utils import ureg


def convert_value(value: float, from_symbol: str, to_symbol: str) -> float:
    """``value`` in ``from_symbol`` expressed in ``to_symbol``. Offset units (K → °C) are handled."""
    if from_symbol == to_symbol:
        return value
    quantity = ureg.Quantity(value, ureg(from_symbol).units)
    return float(quantity.to(ureg(to_symbol).units).magnitude)


def same_unit(a: str, b: str) -> bool:
    """Whether two unit symbols name the same unit ('kelvin' and 'K' do)."""
    try:
        return ureg(a).units == ureg(b).units
    except Exception:
        return a.strip().lower() == b.strip().lower()


def units_convertible(from_symbol: str, to_symbol: str) -> bool:
    try:
        convert_value(1.0, from_symbol, to_symbol)
    except Exception:
        return False
    return True


def convert_range(vmin: float, vmax: float, from_symbol: str, to_symbol: str) -> tuple[float, float]:
    """A range across a unit change, ordered min < max (a decreasing conversion swaps the ends)."""
    low, high = convert_value(vmin, from_symbol, to_symbol), convert_value(vmax, from_symbol, to_symbol)
    low, high = sorted((low, high))
    return round(low, 6), round(high, 6)


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
        low, high = convert_range(vmin, vmax, from_symbol, to_symbol)
    except Exception:
        return JsonResponse(
            {"error": str(_("%(a)s cannot be converted to %(b)s.") % {"a": from_symbol, "b": to_symbol})},
            status=400,
        )
    return JsonResponse({"value_min": low, "value_max": high})
