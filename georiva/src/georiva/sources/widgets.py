from django.forms import widgets

from .registry import data_source_registry


class DataSourceClassSelectWidget(widgets.Select):
    def __init__(self, attrs=None, choices=()):
        blank_choice = [("", "---------")]
        
        source_choices = data_source_registry.choices()
        
        super().__init__(attrs, blank_choice + source_choices)
