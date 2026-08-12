"""Membership carries its own capabilities.

Tenancy answers *whose rows*; groups answer *what may be done to them*. Adding
somebody to an organisation and then separately remembering to add them to a
group would make the second step the real gate — and forgetting it would produce
the confusing failure where a member reaches their organisation's admin and can
see nothing. So joining an organisation grants the standard data-manager
capabilities, and row scoping decides where they apply.

Group membership is deliberately not removed when an organisation membership is:
users may belong to several organisations, and the choke point already denies a
non-member every row on the host they no longer belong to.
"""

import logging

from django.contrib.auth.models import Group
from django.db.models.signals import post_save
from django.dispatch import receiver

from georiva.core.groups import DATA_MANAGERS_GROUP

from .models import OrganisationMembership

logger = logging.getLogger(__name__)


@receiver(post_save, sender=OrganisationMembership, dispatch_uid="org_member_data_managers")
def grant_data_manager_capabilities(sender, instance, created, **kwargs):
    if not created:
        return
    group = Group.objects.filter(name=DATA_MANAGERS_GROUP).first()
    if group is None:
        # The group is created by a migration; its absence means a partially
        # migrated database, not a membership worth refusing.
        logger.warning(
            "Group %r does not exist; %s joined %s without data capabilities.",
            DATA_MANAGERS_GROUP,
            instance.user,
            instance.organisation.slug,
        )
        return
    instance.user.groups.add(group)
