"""Shared test support for the sources admin pages."""


def ensure_base_datafeed_viewset():
    """Render data_feed_detail for a plain base DataFeed in tests.

    The detail page resolves edit/delete URLs through the viewset registry,
    which production populates per plugin child model; the base DataFeed used
    in tests needs the same registration.
    """
    from georiva.sources.models import DataFeed
    from georiva.sources.registry import data_feed_viewset_registry

    class _BaseDataFeedViewSet:
        type = "datafeed"
        model = DataFeed

        @staticmethod
        def get_url_name(action):
            # Any admin URL taking a pk works; panel tests only need the
            # page to render, not real edit/delete endpoints.
            return "data_feed_detail"

    if "datafeed" not in data_feed_viewset_registry._viewsets:
        data_feed_viewset_registry.register(_BaseDataFeedViewSet)
