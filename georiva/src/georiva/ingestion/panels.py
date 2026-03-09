from django.urls import reverse
from wagtail.admin.ui.components import Component


class IngestionActivityPanel(Component):
    name = "ingestion_activity"
    template_name = "ingestion/dashboard_panel.html"
    order = 200
    
    def get_context_data(self, parent_context):
        context = super().get_context_data(parent_context)
        
        api_url = reverse("ingestion_dashboard_api")
        
        context.update({
            "api_url": api_url,
        })
        
        return context
