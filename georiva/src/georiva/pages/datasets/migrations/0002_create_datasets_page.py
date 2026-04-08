from django.db import migrations


def create_datasets_index_page(apps, schema_editor):
    from georiva.pages.datasets.models import DatasetsIndexPage
    from georiva.pages.home.models import HomePage
    
    # Get the first home page
    home_page = HomePage.objects.first()
    if not home_page:
        return
    
    # Avoid creating duplicates on re-run
    if DatasetsIndexPage.objects.exists():
        return
    
    datasets_page = DatasetsIndexPage(
        title="Datasets",
        slug="datasets",
        intro_text="",
        collections_per_page=20,
        show_in_menus=False,
        live=True,
        draft_title="Datasets",
    )
    
    home_page.add_child(instance=datasets_page)


def remove_datasets_index_page(apps, schema_editor):
    from georiva.pages.datasets.models import DatasetsIndexPage
    DatasetsIndexPage.objects.filter(slug="datasets").delete()


class Migration(migrations.Migration):
    dependencies = [
        ("home", "0003_homepage_featured_heading_and_more"),
        ("datasets", "0001_initial"),
    ]
    
    operations = [
        migrations.RunPython(
            create_datasets_index_page,
            remove_datasets_index_page,
        ),
    ]
