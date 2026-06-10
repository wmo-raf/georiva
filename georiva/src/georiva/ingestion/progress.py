from task_ferry.progress import Progress


class PublishingProgress(Progress):
    """
    Progress subclass that publishes each increment to a Redis pub/sub channel.
    Redis wiring is added in slice 2; this slice gets the increment() calls
    in the right places with the right state strings.
    """

    def increment(self, by: float = 1.0, state: str = "") -> None:
        super().increment(by=by, state=state)
        self._publish(state)

    def _publish(self, state: str) -> None:
        pass  # wired to Redis in slice 2
