# Create the "Data Managers" group: admin + wizard access, browse-only on the
# raw Catalog/Collection forms. Data managers define data through the guided
# flows (Add Data front door); raw model editing stays with advanced users.
from django.db import migrations

GROUP_NAME = "Data Managers"

# (app_label, model, codename) — permissions the group holds. Deliberately no
# add/change/delete on catalog or collection: Wagtail's ModelViewSet enforces
# these server-side, which is exactly the gate.
GROUP_PERMISSIONS = [
    ("wagtailadmin", "admin", "access_admin"),
    ("georivacore", "catalog", "view_catalog"),
    ("georivacore", "collection", "view_collection"),
]


def _permission(apps, app_label, model, codename):
    ContentType = apps.get_model("contenttypes", "ContentType")
    Permission = apps.get_model("auth", "Permission")
    # Permissions are normally created by the post_migrate signal, which runs
    # after all migrations — so get_or_create them here to be self-sufficient.
    content_type, _ = ContentType.objects.get_or_create(app_label=app_label, model=model)
    permission, _ = Permission.objects.get_or_create(
        content_type=content_type,
        codename=codename,
        defaults={"name": codename.replace("_", " ").capitalize()},
    )
    return permission


def create_group(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    group, _ = Group.objects.get_or_create(name=GROUP_NAME)
    group.permissions.add(*[
        _permission(apps, app_label, model, codename)
        for app_label, model, codename in GROUP_PERMISSIONS
    ])


def remove_group(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    Group.objects.filter(name=GROUP_NAME).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("georivacore", "0006_variable_unique_variable_slug_per_collection"),
        ("wagtailadmin", "0001_create_admin_access_permissions"),
    ]

    operations = [
        migrations.RunPython(create_group, remove_group),
    ]
