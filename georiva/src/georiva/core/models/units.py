from django.db import models
from django.utils.translation import gettext_lazy as _
from wagtail.admin.panels import FieldPanel

from georiva.core.unit_utils import validate_unit, ureg


class Unit(models.Model):
    name = models.CharField(max_length=255, verbose_name=_("Name"), help_text=_("Name of the unit"), unique=True)
    symbol = models.CharField(max_length=255, verbose_name=_("Symbol"), help_text=_("Symbol of the unit"),
                              validators=[validate_unit], unique=True)
    description = models.TextField(verbose_name=_("Description"), blank=True, null=True,
                                   help_text=_("Description of the unit"))
    
    panels = [
        FieldPanel("name"),
        FieldPanel("symbol"),
        FieldPanel("description"),
    ]
    
    class Meta:
        verbose_name = _("Unit")
        verbose_name_plural = _("Units")
        ordering = ['name']
    
    def __str__(self):
        return self.name
    
    @property
    def pint_unit(self):
        return ureg(self.symbol)
    
    def get_registry_unit(self):
        unit = self.pint_unit.u
        return str(unit)
    
    get_registry_unit.short_description = _("Unit Registry")
