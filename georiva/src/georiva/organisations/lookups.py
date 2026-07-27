"""The vocabulary of tenancy declarations, importable from anywhere.

Split from ``access.py`` so a model can declare where it stands without
importing the enforcement machinery — ``access`` reads the organisation models,
and a model importing ``access`` would close that circle.

``access`` re-exports everything here, so enforcement code has one import.
"""

#: Class attribute a model declares to name the ORM path from itself to the
#: ``Organisation`` that owns it — ``"organisation"`` on the tenancy root,
#: ``"catalog__organisation"`` one step down, and so on.
LOOKUP_ATTR = "ORGANISATION_LOOKUP"

#: Declared instead of a path by a model no organisation owns and every
#: organisation reads — topics, units, the global tier of colour palettes.
#: Scoping passes these through untouched.
SHARED_REFERENCE_DATA = "shared-reference-data"

#: Declared by a model that *is* an organisation, or is keyed by one directly.
#: Scoping matches on identity rather than following a path.
ORGANISATION_SELF = "self"

#: Declared by a model that belongs to an organisation but has no ORM path to
#: it: pipeline bookkeeping keyed by a storage path (``FileIngestion`` and its
#: jobs), records reached only through an already-scoped parent
#: (``DerivationRun``), and Wagtail pages, which are org-owned through the
#: Site → root-page link rather than a field (decision #261). Scoping *refuses*
#: these — loudly — so putting one on a scoped surface is a decision somebody
#: has to make explicitly rather than a filter that quietly does nothing.
NOT_ORM_SCOPABLE = "not-orm-scopable"

#: Every declaration that is a decision rather than an ORM path.
SENTINELS = frozenset({SHARED_REFERENCE_DATA, ORGANISATION_SELF, NOT_ORM_SCOPABLE})

#: Models we did not write cannot declare anything — and cannot be org-owned
#: either, since nothing outside this codebase sits in the FK chain under a
#: Catalog. They are shared by construction, so the declaration requirement
#: applies to our own models only.
OWN_MODULE_PREFIX = "georiva."



def declared_lookup(model):
    """Whatever ``model`` declared: an ORM path, one of ``SENTINELS``, or none.

    Models from outside this codebase read as shared without declaring it — see
    ``OWN_MODULE_PREFIX``.
    """
    declared = getattr(model, LOOKUP_ATTR, None)
    if declared:
        return declared
    if not model.__module__.startswith(OWN_MODULE_PREFIX):
        return SHARED_REFERENCE_DATA
    return None
