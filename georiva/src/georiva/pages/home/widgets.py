from django import forms


class PortalAppSelect(forms.Select):
    """A choice of the apps this instance serves, worked out when it is drawn.

    Not field ``choices``: those would be read once, at import, and baked into a
    migration — and the set of installed apps is the operator's to change. A row
    whose plugin is no longer installed keeps its value, marked as such, so the
    editor can see what it was and delete it rather than have it silently
    rewritten to the first app in the list.
    """

    def optgroups(self, name, value, attrs=None):
        from georiva.core.web_apps import served_web_apps

        served = [(mount.plugin, str(mount.name)) for mount in served_web_apps()]
        known = {plugin for plugin, _ in served}
        orphans = [(v, f"{v} (not installed)") for v in value if v and v not in known]
        self.choices = [("", "---------"), *served, *orphans]
        return super().optgroups(name, value, attrs)
