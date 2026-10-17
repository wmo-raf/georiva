"""The guard on CI's split of the suite.

Sharding divides the Django suite across parallel CI jobs by test label. Its
failure mode is silent and it is the worst kind: add an app that no shard names
and its tests simply stop running, while CI stays green and reports success on a
suite that no longer includes them. Nothing goes red. Nothing is logged.

So the partition is held to Django's own discovery rather than to a list someone
maintains alongside it. What is asserted is the property CI actually depends on
— every test the runner can find runs in exactly one shard — which stays true
without anyone remembering it, because the left-hand side of the comparison is
built by the same machinery that would have run the missing tests.
"""

import json
from pathlib import Path

from django.test import SimpleTestCase
from django.test.runner import DiscoverRunner

SHARDS_FILE = Path(__file__).resolve().parent.parent.parent / "config" / "test_shards.json"


def load_shards():
    return json.loads(SHARDS_FILE.read_text())["shards"]


#: The directory holding the ``georiva`` package. Discovery resolves dotted
#: labels against the working directory, which in the container is the package
#: itself — one level too deep for "georiva.sources" to mean anything. Pinning
#: top_level makes these assertions independent of where the runner was invoked.
TOP_LEVEL = str(Path(__file__).resolve().parents[3])


def _test_ids(labels):
    """Every test id Django's runner would execute for these labels."""
    suite = DiscoverRunner(verbosity=0, top_level=TOP_LEVEL).build_suite(labels)
    return {t.id() for t in iter_tests(suite)}


def iter_tests(suite):
    for item in suite:
        if hasattr(item, "__iter__"):
            yield from iter_tests(item)
        else:
            yield item


class ShardPartitionTests(SimpleTestCase):
    def test_the_shards_file_is_readable_and_non_empty(self):
        shards = load_shards()
        self.assertGreater(len(shards), 1, "a single shard is not a partition")
        for labels in shards:
            self.assertTrue(labels, "an empty shard runs nothing and wastes a job")

    def test_no_label_appears_in_two_shards(self):
        """Cheap to check, and the likeliest way to hand-edit this file wrong."""
        shards = load_shards()
        flat = [label for labels in shards for label in labels]
        duplicates = {label for label in flat if flat.count(label) > 1}
        self.assertEqual(duplicates, set(), f"labels in more than one shard: {sorted(duplicates)}")

    def test_every_test_the_runner_finds_runs_in_some_shard(self):
        """The assertion this module exists to make.

        Discovery over no labels is the whole suite; discovery over the shards is
        what CI will actually run. Anything in the first and not the second is a
        test that has silently stopped being run.
        """
        everything = _test_ids([])
        sharded = set()
        for labels in load_shards():
            sharded |= _test_ids(labels)

        missed = everything - sharded
        self.assertEqual(
            missed,
            set(),
            f"these tests are in no shard, so CI would stop running them without going red: {sorted(missed)[:10]}",
        )

    def test_no_test_runs_in_more_than_one_shard(self):
        """Duplicated work is only wasted time, but it also skews the balance."""
        seen, overlapping = set(), set()
        for labels in load_shards():
            ids = _test_ids(labels)
            overlapping |= seen & ids
            seen |= ids

        self.assertEqual(overlapping, set(), f"tests in more than one shard: {sorted(overlapping)[:10]}")
