"""A duplicated IntervalSchedule must not unregister periodic tasks (#411)."""

from django.test import TestCase
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from georiva.utils.periodic import PeriodicSpec, register_periodic_tasks


class RegisterPeriodicTasksTests(TestCase):
    def test_duplicate_schedules_are_reused(self):
        IntervalSchedule.objects.create(every=5, period=IntervalSchedule.MINUTES)
        IntervalSchedule.objects.create(every=5, period=IntervalSchedule.MINUTES)

        register_periodic_tasks([PeriodicSpec("t.a", "x.a", 5, IntervalSchedule.MINUTES)])

        pt = PeriodicTask.objects.get(name="t.a")
        self.assertEqual((pt.interval.every, pt.interval.period), (5, IntervalSchedule.MINUTES))
        self.assertEqual(IntervalSchedule.objects.filter(every=5, period=IntervalSchedule.MINUTES).count(), 2)

    def test_creates_schedule_when_absent(self):
        register_periodic_tasks([PeriodicSpec("t.a", "x.a", 7, IntervalSchedule.DAYS)])
        self.assertEqual(PeriodicTask.objects.get(name="t.a").interval.every, 7)

    def test_one_failure_does_not_take_the_others(self):
        logged = []
        register_periodic_tasks(
            [
                PeriodicSpec("t.bad", "x.bad", None, IntervalSchedule.MINUTES),
                PeriodicSpec("t.good", "x.good", 1, IntervalSchedule.DAYS),
            ],
            log=lambda *a: logged.append(a),
        )
        self.assertTrue(PeriodicTask.objects.filter(name="t.good").exists())
        self.assertEqual(len(logged), 1)
