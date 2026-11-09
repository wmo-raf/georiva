from django.db import migrations


def create_datasets_index_page(apps, schema_editor):
    """Attach the datasets index to the first HomePage — and only that one.

    Every provisioned organisation has its own index (see
    ``organisations/provisioning._create_root_page``), so "does one exist?"
    asked instance-wide is a question about other tenants. This migration owns
    exactly one page: the index under the HomePage that
    ``home/0002_create_homepage`` built, which the central org later adopts
    along with the default Site. ``first()`` finds that one because treebeard
    orders by path and it is the earliest home page on the instance
    (``00010001``); organisation roots are added after it.
    """
    from django.contrib.contenttypes.models import ContentType
    from wagtail.models import Page

    from georiva.pages.datasets.models import DatasetsIndexPage

    # The home page is found as a plain ``Page``, by content type, and never
    # through the live ``HomePage`` model. A live model selects every column its
    # *current* code declares, and this migration runs before any later ``home``
    # migration has added them — so on a fresh database, a field added to
    # ``HomePage`` after this was written fails here ("column does not exist").
    # Wagtail's own ``Page`` is still needed live, for treebeard's ``add_child``.
    home_type = ContentType.objects.filter(app_label="home", model="homepage").first()
    if home_type is None:
        return

    home_page = Page.objects.filter(content_type=home_type).order_by("path").first()
    if not home_page:
        return

    if DatasetsIndexPage.objects.child_of(home_page).exists():
        return

    datasets_page = DatasetsIndexPage(
        title="Datasets",
        slug="datasets",
        intro_text="",
        collections_per_page=20,
        show_in_menus=False,
        live=True,
        draft_title="Datasets",
        # Given, not left to Wagtail: its default asks the parent's *specific*
        # model for the locale, which is the live ``HomePage`` query above all
        # over again.
        locale_id=home_page.locale_id,
    )

    home_page.add_child(instance=datasets_page)


class Migration(migrations.Migration):
    dependencies = [
        ("home", "0003_homepage_featured_heading_and_more"),
        ("datasets", "0001_initial"),
        ("wagtailsearch", "0010_add_text_fields"),
    ]

    operations = [
        # Reverse deletes nothing, deliberately. By the time anyone rolls back,
        # the index is editor-owned content — title, intro text, page size — and
        # the old `filter(slug="datasets")` matched every organisation's index,
        # not just this migration's. Even correctly scoped, destroying an edited
        # page to undo its creation is a worse trade than leaving it: rolling
        # forward again is harmless, because the guard above finds it still
        # there and returns.
        migrations.RunPython(
            create_datasets_index_page,
            migrations.RunPython.noop,
        ),
    ]
