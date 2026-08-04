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

#: Declared by a Wagtail page, or by anything else whose owner is decided by
#: where it sits in the page tree. Each organisation is provisioned with a Site
#: and a root page of its own, and everything it authors lives under that root
#: (ADR 0016) — so the ownership question for a page is a tree question, and the
#: dispatcher answers it by comparing treebeard paths rather than by walking a
#: field that does not exist.
PAGE_TREE = "page-tree"

#: Declared by a model that no ORM filter on an organisation can narrow, and
#: that no other declaration reaches either: pipeline bookkeeping keyed by a
#: storage path (``FileIngestion`` and its jobs), records reached only through an
#: already-scoped parent (``DerivationRun``), or a row belonging to a *person*
#: rather than to an institution (``ApiKey``), whose holder may be a member of
#: several and whose credential is nobody's tenant data. Scoping *refuses* these
#: — loudly — so putting one on a scoped surface is a decision somebody has to
#: make explicitly rather than a filter that quietly does nothing.
NOT_ORM_SCOPABLE = "not-orm-scopable"

#: Every unparameterised declaration that is a decision rather than an ORM path.
#: The two parameterised kinds below are decisions too; ask :func:`is_orm_path`
#: rather than testing membership here.
SENTINELS = frozenset({
    SHARED_REFERENCE_DATA,
    ORGANISATION_SELF,
    PAGE_TREE,
    NOT_ORM_SCOPABLE,
})

#: The parameterised kinds. Each names a *kind* of declaration rather than a
#: whole one — the declaration is this string, a colon and the arguments, which
#: is why they are recognised by prefix and not by membership of a set.
VIA_RELATED = "via-related"
VIA_CONTENT_OBJECT = "via-content-object"

VIA_RELATED_PREFIX = f"{VIA_RELATED}:"
VIA_CONTENT_OBJECT_PREFIX = f"{VIA_CONTENT_OBJECT}:"

#: The two kinds that are not spellings of a declaration: an ORM path is any
#: string that is none of the above, and no-route covers both a model that
#: declared :data:`NOT_ORM_SCOPABLE` and one that declared nothing at all. The
#: dispatcher refuses them identically, so they are one kind here.
ORM_PATH = "orm-path"
NO_ROUTE = "no-route"


def via_related(path):
    """Declare that the model at the end of ``path`` decides who owns this row.

    ``path`` is a forward relation — one field, or several joined by ``__`` — to
    another model that has a declaration of its own. The dispatcher reads *that*
    model's declaration and narrows this queryset to the rows whose related
    object survived it.

    This is the general form of "owned through the page tree": a page log entry
    declares ``via_related("page")``, a page-child orderable declares
    ``via_related("page")``, and both resolve to the tree test because ``Page``
    declares :data:`PAGE_TREE`. It is not a substitute for an ORM path to
    Organisation — write that where one exists, since it is one filter rather
    than two — but for a target that is itself unreachable by ORM path it is the
    only thing that works, and it composes: the target's own declaration may be
    another ``via_related``, or a generic content object.
    """
    return f"{VIA_RELATED_PREFIX}{path}"


def via_content_object(content_type_field, object_id_field):
    """Declare that this row's subject is polymorphic, named by a generic key.

    Workflow states, task states and model log entries identify their subject by
    a content type plus an object id, which no single ``.filter()`` can cross.
    The dispatcher splits the rows by content type, scopes each type by that
    type's own declaration, and recombines — which is why the two field names are
    part of the declaration rather than assumed: Wagtail's own two consumers
    disagree, ``WorkflowState`` keying on ``base_content_type`` and
    ``ModelLogEntry`` on ``content_type``.
    """
    return f"{VIA_CONTENT_OBJECT_PREFIX}{content_type_field}:{object_id_field}"


def related_path(declared):
    """The path in a :func:`via_related` declaration, or ``None``."""
    if isinstance(declared, str) and declared.startswith(VIA_RELATED_PREFIX):
        return declared[len(VIA_RELATED_PREFIX):]
    return None


def content_object_fields(declared):
    """The ``(content_type_field, object_id_field)`` pair, or ``None``."""
    if isinstance(declared, str) and declared.startswith(VIA_CONTENT_OBJECT_PREFIX):
        content_type_field, _, object_id_field = declared[
            len(VIA_CONTENT_OBJECT_PREFIX):
        ].partition(":")
        return content_type_field, object_id_field
    return None


def kind_of(declared):
    """Which kind of declaration ``declared`` is — the one question to ask of one.

    Every caller that would otherwise test the declaration string five ways in a
    row asks this instead and switches once on the answer. That is what keeps the
    dispatcher's two entry points and :func:`~.ownership.is_scopable` from
    drifting apart as kinds are added: a new kind is a new branch in one place,
    and a caller that does not handle it fails visibly rather than falling
    through to "ORM path" and filtering on a column called ``via-related:page``.

    The answer is one of :data:`SHARED_REFERENCE_DATA`,
    :data:`ORGANISATION_SELF`, :data:`PAGE_TREE`, :data:`VIA_RELATED`,
    :data:`VIA_CONTENT_OBJECT`, :data:`ORM_PATH` or :data:`NO_ROUTE` — the first
    three being their own declaration, since they carry no arguments.
    """
    if declared in (SHARED_REFERENCE_DATA, ORGANISATION_SELF, PAGE_TREE):
        return declared
    if not declared or declared == NOT_ORM_SCOPABLE:
        return NO_ROUTE
    if related_path(declared) is not None:
        return VIA_RELATED
    if content_object_fields(declared) is not None:
        return VIA_CONTENT_OBJECT
    return ORM_PATH


def is_orm_path(declared):
    """Whether ``declared`` is an ORM path to Organisation rather than a decision.

    A shorthand for the one question the helpers in ``access`` ask, which is
    whether they can do anything at all with a declaration.
    """
    return kind_of(declared) == ORM_PATH


#: Class attribute a model sets alongside a *path* to say that a null along that
#: path is not a broken route but a tier: a row no organisation owns, which every
#: organisation reads and only the instance admin writes. Colour palettes are the
#: case the declaration exists for — a shipped library everybody draws on, plus
#: each institution's own (decision #269).
#:
#: It is deliberately not a sentinel. A model declaring it is still org-owned,
#: still filtered by the path it declared; the global rows are added to what a
#: read may see, and are refused to every write that is not the instance admin's.
GLOBAL_TIER_ATTR = "ORGANISATION_GLOBAL_TIER"

#: Models we did not write cannot declare anything — and cannot be org-owned
#: either, since nothing outside this codebase sits in the FK chain under a
#: Catalog. They are shared by construction, so the declaration requirement
#: applies to our own models only.
OWN_MODULE_PREFIX = "georiva."



def has_global_tier(model):
    """Whether ``model`` declared that a null organisation means "everybody's"."""
    return bool(getattr(model, GLOBAL_TIER_ATTR, False))


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
