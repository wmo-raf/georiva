from task_ferry.progress import Progress


class PublishingProgress(Progress):
    """
    Progress subclass that publishes each increment to a Redis pub/sub channel.

    Pass job_id to tie events to a specific FileIngestionJob. Without job_id
    no events are published (safe for callers that don't need tracking).
    """

    def __init__(self, total: int, *, job_id: int = None, **kwargs) -> None:
        super().__init__(total, **kwargs)
        self._job_id = job_id

    def increment(self, by: float = 1.0, state: str = "") -> None:
        super().increment(by=by, state=state)
        self._publish(state)

    def _publish(self, state: str) -> None:
        if self._job_id is None:
            return
        from georiva.ingestion.events import publish_event
        publish_event({
            "type": "job.progress_updated",
            "job_id": self._job_id,
            "state": state,
            "percentage": self.percentage,
        })
