from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = (
        "Crash-recovery sweep: mark FetchRuns stuck in RUNNING past the "
        "staleness threshold as INTERRUPTED, reap wedged LoaderJobs, and "
        "auto-resume interrupted runs (capped). Same policy as the periodic "
        "beat — run this to recover immediately instead of waiting for it. "
        "Only sweep early (--older-than-hours / --run) when you know the "
        "worker is dead: sweeping a live run enqueues a duplicate racing it."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--older-than-hours",
            type=float,
            default=None,
            metavar="HOURS",
            help=(
                "Override the staleness threshold for this invocation "
                "(default: GEORIVA_FETCH_RUN_STALE_AFTER_HOURS). "
                "0 declares every running run dead — a hard sweep."
            ),
        )
        parser.add_argument(
            "--run",
            type=int,
            action="append",
            dest="run_ids",
            metavar="RUN_ID",
            help=(
                "Sweep exactly this FetchRun id, ignoring age — you are "
                "asserting the run is dead. Repeatable."
            ),
        )
        parser.add_argument(
            "--no-resume",
            action="store_true",
            help="Mark runs INTERRUPTED without enqueuing auto-resumes.",
        )

    def handle(self, *args, **options):
        from georiva.sources.acquisition_recovery import (
            stale_after_hours,
            sweep_stale_fetch_runs,
        )
        from georiva.sources.models import FetchRun

        stale_hours = options["older_than_hours"]
        run_ids = options["run_ids"]

        if run_ids is not None and stale_hours is not None:
            raise CommandError(
                "--run and --older-than-hours are mutually exclusive: "
                "targeted runs are swept regardless of age."
            )

        if run_ids is not None:
            running = set(
                FetchRun.objects
                .filter(pk__in=run_ids, status=FetchRun.Status.RUNNING)
                .values_list("pk", flat=True)
            )
            for missed in sorted(set(run_ids) - running):
                self.stdout.write(self.style.WARNING(
                    f"FetchRun {missed}: not found or not in RUNNING — skipped."
                ))
            self.stdout.write(
                f"Sweeping {len(running)} targeted run(s), ignoring age..."
            )
        else:
            hours = stale_after_hours() if stale_hours is None else stale_hours
            self.stdout.write(
                f"Sweeping fetch runs stuck in RUNNING for over {hours}h..."
            )

        result = sweep_stale_fetch_runs(
            stale_hours=stale_hours,
            run_ids=run_ids,
            resume=not options["no_resume"],
        )
        self.stdout.write(self.style.SUCCESS(
            f"Sweep complete: {result['swept']} run(s) interrupted, "
            f"{result['resumed']} resume(s) enqueued, "
            f"{result['jobs_reaped']} zombie job(s) reaped."
        ))
