from wagtail.admin.panels import FieldPanel


class TransformTypePanel(FieldPanel):
    class BoundPanel(FieldPanel.BoundPanel):
        def get_context_data(self, parent_context=None):
            ctx = super().get_context_data(parent_context)
            ctx['transform_type_panel'] = True
            return ctx
        
        @property
        def media(self):
            media = super().media
            from django.forms import Media
            return media + Media(js=['core/js/transform_type_panel.js'])
