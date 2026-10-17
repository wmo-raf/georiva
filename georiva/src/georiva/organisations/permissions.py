"""Permission policy for instance-admin-only admin surfaces.

Creating organisations is an instance-admin act, not an org-admin one: it hands
out a subdomain and a storage prefix. Rather than gate the views one by one, the
viewset carries this policy — Wagtail then hides the menu item, the listing
buttons and the views themselves from everyone but a superuser.
"""

from django.contrib.auth import get_user_model
from wagtail.permission_policies.base import BasePermissionPolicy


class SuperuserOnlyPermissionPolicy(BasePermissionPolicy):
    """Grants every action to active superusers, and to nobody else.

    ``denied_actions`` withholds an action from everyone, superusers included —
    the way to take a view off a viewset such that its buttons disappear too.
    """

    def __init__(self, model, denied_actions=()):
        super().__init__(model)
        self.denied_actions = frozenset(denied_actions)

    def user_has_permission(self, user, action):
        if action in self.denied_actions:
            return False
        return bool(user and user.is_authenticated and user.is_active and user.is_superuser)

    def user_has_any_permission(self, user, actions):
        return any(self.user_has_permission(user, action) for action in actions)

    def users_with_any_permission(self, actions):
        return get_user_model().objects.filter(is_active=True, is_superuser=True)

    def users_with_permission(self, action):
        return self.users_with_any_permission([action])
