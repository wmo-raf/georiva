from django.urls import path
from wagtail import hooks


@hooks.register("register_admin_urls")
def register_harvest_wizard_urls():
    from .wizard_views import (
        harvest_recheck_upstream,
        harvest_wizard_step1,
        harvest_wizard_step2,
        harvest_wizard_step3,
        harvest_wizard_step4,
        harvest_wizard_step5,
    )

    return [
        path("harvest-feeds/wizard/address/", harvest_wizard_step1, name="harvest_wizard_step1"),
        path("harvest-feeds/wizard/collection/", harvest_wizard_step2, name="harvest_wizard_step2"),
        path("harvest-feeds/wizard/found/", harvest_wizard_step3, name="harvest_wizard_step3"),
        path("harvest-feeds/wizard/images/", harvest_wizard_step4, name="harvest_wizard_step4"),
        path("harvest-feeds/wizard/save/", harvest_wizard_step5, name="harvest_wizard_step5"),
        path("harvest-feeds/<int:pk>/recheck/", harvest_recheck_upstream, name="harvest_recheck_upstream"),
    ]
