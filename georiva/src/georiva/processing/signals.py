"""
Engine-side signals (ADR-0020).

``unit_completed`` fires after a DerivationRun completes and its produced
Published item is registered. The engine knows nothing about who listens —
the feed layer (``sources``) subscribes to revive dependent products' parked
``not_ready`` runs, keeping ADR-0005's import direction intact: the engine
never imports DerivedProduct; the feed layer depends on the engine's signal.

Kwargs sent: ``item`` (the produced core Item), ``recipe_type``.
"""
import django.dispatch

unit_completed = django.dispatch.Signal()
