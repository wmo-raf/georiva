from django.forms import widgets


class ConditionalCheckbox(widgets.CheckboxInput):
    """
    A checkbox that shows/hides a target panel when toggled.
    
    Usage:
        FieldPanel('is_forecast', widget=ConditionalCheckbox(target_panel_id='panel-child-details-forecast_configuration-section'))
    """
    
    def __init__(self, target_panel_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_panel_id = target_panel_id
    
    def build_attrs(self, base_attrs, extra_attrs=None):
        attrs = super().build_attrs(base_attrs, extra_attrs)
        attrs['data-conditional-target'] = self.target_panel_id
        return attrs
    
    class Media:
        js = ('core/js/widgets/conditional_checkbox.js',)
