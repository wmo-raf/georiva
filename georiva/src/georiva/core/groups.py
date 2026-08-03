"""Names of the groups the instance ships with.

The capability layer: what a user may *do* (add a catalog, run a wizard) is a
Wagtail/Django group permission, entirely separate from which organisation's
rows they may do it to. See ``organisations/access.py`` for the other half.
"""

#: Created by ``core.migrations.0007_data_managers_group``. Every organisation
#: member is added to it, so membership alone is enough to do data work.
DATA_MANAGERS_GROUP = "Data Managers"
