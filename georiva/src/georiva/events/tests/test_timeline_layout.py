"""The Timeline's rail layout: lanes, folds, gaps and focus, without a database."""

import datetime
import uuid

from django.test import SimpleTestCase

from georiva.events.timeline import Entry, build_layout, elapsed

T0 = datetime.datetime(2026, 10, 27, 10, 2, 11, tzinfo=datetime.UTC)


def at(seconds):
    return T0 + datetime.timedelta(seconds=seconds)


def entry(seconds, *, cause=None, event_type="ingestion.fileingestionfinished", outcome="ok"):
    return Entry(
        id=uuid.uuid4(),
        event_type=event_type,
        occurred_at=at(seconds),
        outcome_class=outcome,
        caused_by_id=cause.id if isinstance(cause, Entry) else cause,
    )


def drawn(layout):
    """What each rail row shows: an entry, a group, a repeat, or a gap."""
    return [row.entry or row.group or row.repeat or "gap" for row in layout.rows]


def segments(cell):
    return "".join(side for side in "nsew" if getattr(cell, side))


class LinearChainTests(SimpleTestCase):
    def test_a_chain_without_forks_is_one_lane_oldest_first(self):
        first = entry(0)
        second = entry(4, cause=first)
        third = entry(41, cause=second)

        layout = build_layout([first, second, third])

        self.assertEqual(drawn(layout), [first, second, third])
        self.assertEqual(layout.lanes, 1)
        self.assertEqual([segments(row.cells[0]) for row in layout.rows], ["s", "ns", "n"])
        self.assertTrue(all(row.cells[0].dot for row in layout.rows))

    def test_offsets_count_from_the_first_event_shown(self):
        first = entry(0)
        second = entry(125, cause=first)

        layout = build_layout([first, second])

        self.assertEqual(layout.started, T0)
        self.assertEqual(layout.duration, datetime.timedelta(seconds=125))
        self.assertEqual(layout.rows[1].offset, datetime.timedelta(seconds=125))

    def test_an_effect_recorded_before_its_cause_is_still_drawn_below_it(self):
        # Worker clocks drift; a child stamped a moment early must not sit above its parent.
        parent = entry(10)
        child = entry(9, cause=parent)

        self.assertEqual(drawn(build_layout([child, parent])), [parent, child])


class ForkTests(SimpleTestCase):
    def test_the_last_effect_continues_the_lane_and_earlier_ones_branch(self):
        root = entry(0)
        early = entry(38, cause=root, event_type="processing.derivationattemptfinished")
        late = entry(41, cause=root, event_type="processing.derivationattemptfinished")

        layout = build_layout([root, early, late])

        self.assertEqual(drawn(layout), [root, early, late])
        self.assertEqual(layout.lanes, 2)
        branch = layout.rows[1]
        # The root's lane runs on past the branch point to the effect that continues it.
        self.assertEqual(segments(branch.cells[0]), "nse")
        self.assertFalse(branch.cells[0].dot)
        self.assertEqual(segments(branch.cells[1]), "w")
        self.assertTrue(branch.cells[1].dot)
        self.assertEqual(segments(layout.rows[2].cells[0]), "n")
        self.assertTrue(layout.rows[2].cells[0].dot)

    def test_a_branch_lane_is_reused_once_it_ends(self):
        root = entry(0)
        a = entry(1, cause=root, event_type="x.a")
        b = entry(2, cause=root, event_type="x.b")
        c = entry(3, cause=b, event_type="x.c")
        d = entry(4, cause=b, event_type="x.d")

        layout = build_layout([root, a, b, c, d])

        self.assertEqual(layout.lanes, 2)

    def test_a_long_branch_crossing_another_lane_draws_through_it(self):
        root = entry(0)
        side = entry(1, cause=root, event_type="x.side")
        side_child = entry(5, cause=side, event_type="x.side_child")
        other = entry(2, cause=root, event_type="x.other")
        last = entry(6, cause=root, event_type="x.last")

        layout = build_layout([root, side, side_child, other, last])

        rows = {row.entry: row for row in layout.rows}
        self.assertEqual(layout.lanes, 3)
        # ``other`` branches from lane 0 to lane 2 across lane 1's vertical.
        self.assertEqual(segments(rows[other].cells[1]), "nsew")


class FoldTests(SimpleTestCase):
    def test_three_effects_of_one_type_stay_as_lanes(self):
        root = entry(0, event_type="sources.fetchrunfinished")
        files = [entry(i + 1, cause=root) for i in range(3)]

        layout = build_layout([root, *files])

        self.assertEqual(drawn(layout), [root, *files])

    def test_four_effects_of_one_type_fold_into_one_group(self):
        root = entry(0, event_type="sources.fetchrunfinished")
        files = [entry(i + 2, cause=root) for i in range(3)] + [entry(238, cause=root, outcome="failed")]

        layout = build_layout([root, *files])

        self.assertEqual(len(layout.rows), 2)
        group = layout.rows[1].group
        self.assertEqual(group.members, files)
        self.assertEqual(group.counts, {"ok": 3, "failed": 1, "refused": 0})
        self.assertEqual((group.start, group.end), (datetime.timedelta(seconds=2), datetime.timedelta(seconds=238)))
        self.assertEqual(group.outcome_class, "failed")

    def test_failures_inside_a_group_are_pulled_out(self):
        root = entry(0, event_type="sources.fetchrunfinished")
        ok = [entry(i + 1, cause=root) for i in range(3)]
        failed = entry(5, cause=root, outcome="failed")
        refused = entry(6, cause=root, outcome="refused")

        group = build_layout([root, *ok, failed, refused]).rows[1].group

        self.assertEqual(group.pulled_out, [failed, refused])
        self.assertEqual(group.outcome_class, "failed")

    def test_effects_of_a_folded_group_fold_level_by_level_whatever_their_count(self):
        root = entry(0, event_type="sources.fetchrunfinished")
        files = [entry(i + 1, cause=root) for i in range(4)]
        derived = [entry(40 + i, cause=files[i], event_type="processing.derivationattemptfinished") for i in range(2)]

        layout = build_layout([root, *files, *derived])

        self.assertEqual(len(layout.rows), 3)
        self.assertEqual(layout.rows[2].group.members, derived)
        self.assertEqual(layout.lanes, 1)

    def test_a_group_knows_which_members_have_effects_to_focus_on(self):
        root = entry(0, event_type="sources.fetchrunfinished")
        files = [entry(i + 1, cause=root) for i in range(4)]
        derived = entry(40, cause=files[2], event_type="processing.derivationattemptfinished")

        layout = build_layout([root, *files, derived])

        self.assertEqual(layout.rows[1].group.focusable, {files[2].id})

    def test_many_roots_of_one_type_fold_too(self):
        # Files that land while their fetch is still running have no cause to hang from.
        files = [entry(i) for i in range(5)]

        layout = build_layout(files)

        self.assertEqual(len(layout.rows), 1)
        self.assertEqual(layout.rows[0].group.members, files)

    def test_the_span_bar_is_placed_on_the_scale_of_the_whole_chain(self):
        root = entry(0, event_type="sources.fetchrunfinished")
        files = [entry(s, cause=root) for s in (100, 150, 200, 300)]
        tail = entry(400, cause=files[0], event_type="x.tail")

        group = build_layout([root, *files, tail]).rows[1].group

        self.assertEqual((group.bar_left, group.bar_width), (25.0, 50.0))

    def test_a_group_has_a_stable_anchor(self):
        files = [entry(i) for i in range(4)]

        group = build_layout(files).rows[0].group

        self.assertEqual(group.anchor, f"group-{files[0].id}")


class CauseTests(SimpleTestCase):
    def test_a_cause_outside_the_view_starts_a_root_with_a_stub(self):
        orphan = entry(0, cause=uuid.uuid4())

        row = build_layout([orphan]).rows[0]

        self.assertTrue(row.stub)
        self.assertTrue(row.cells[0].dashed_n)

    def test_an_event_with_no_cause_is_a_plain_root(self):
        row = build_layout([entry(0)]).rows[0]

        self.assertFalse(row.stub)
        self.assertFalse(row.cells[0].dashed_n)

    def test_events_caused_by_an_unreadable_event_still_hang_from_it(self):
        unreadable = Entry(
            id=uuid.uuid4(), event_type="gone_plugin.thing", occurred_at=T0, outcome_class="ok", caused_by_id=None
        )
        child = entry(3, cause=unreadable)

        layout = build_layout([unreadable, child])

        self.assertEqual(layout.lanes, 1)
        self.assertFalse(layout.rows[1].stub)


class GapTests(SimpleTestCase):
    def test_a_long_quiet_spell_breaks_the_rail(self):
        first = entry(0)
        parked = entry(41, cause=first)
        woken = entry(41 + 3 * 3600 + 12 * 60, cause=parked)

        layout = build_layout([first, parked, woken])

        self.assertEqual(drawn(layout), [first, parked, "gap", woken])
        gap = layout.rows[2]
        self.assertEqual(gap.gap, datetime.timedelta(hours=3, minutes=12))
        self.assertEqual(segments(gap.cells[0]), "ns")
        self.assertTrue(gap.cells[0].dashed)

    def test_same_type_events_either_side_of_a_quiet_spell_do_not_fold_together(self):
        # A chain lasts days; folding across a break would lose the order of what happened.
        early = [entry(s) for s in (0, 1)]
        late = [entry(s) for s in (3600, 3601)]

        layout = build_layout([*early, *late])

        self.assertEqual(drawn(layout), [*early, "gap", *late])

    def test_no_break_while_a_burst_is_still_busy(self):
        steady = [entry(s * 300, event_type=f"x.e{s}") for s in range(6)]

        self.assertNotIn("gap", drawn(build_layout(steady)))


def cycle(hour, *, outcome="ok"):
    """One sweep-revives-a-file cycle, ``hour`` hours into the chain."""
    base = hour * 3600
    return [
        entry(base, event_type="georivaevents.sweepintervened"),
        entry(base + 2, event_type="ingestion.fileingestionfinished", outcome=outcome),
        entry(base + 3, event_type="core.productpublished"),
    ]


class RepeatTests(SimpleTestCase):
    def test_bursts_of_the_same_shape_collapse_after_the_first(self):
        bursts = [cycle(hour) for hour in range(5)]

        layout = build_layout([event for burst in bursts for event in burst])

        self.assertEqual(drawn(layout)[:3], bursts[0])
        self.assertEqual(drawn(layout)[3], "gap")
        repeat = layout.rows[4].repeat
        self.assertEqual(len(layout.rows), 5)
        self.assertEqual(repeat.count, 4)
        self.assertEqual(repeat.members, [event for burst in bursts[1:] for event in burst])
        self.assertEqual(repeat.every, datetime.timedelta(hours=1))
        self.assertEqual(repeat.start, datetime.timedelta(hours=1))
        self.assertEqual(repeat.end, datetime.timedelta(hours=4, seconds=3))
        self.assertTrue(layout.rows[4].cells[0].dot)

    def test_a_single_repeat_is_drawn_in_full(self):
        bursts = [cycle(hour) for hour in range(2)]

        layout = build_layout([event for burst in bursts for event in burst])

        self.assertFalse([row for row in layout.rows if row.repeat])

    def test_a_failure_breaks_the_repetition(self):
        bursts = [cycle(0), cycle(1), cycle(2, outcome="failed"), cycle(3), cycle(4)]

        layout = build_layout([event for burst in bursts for event in burst])

        repeats = [row.repeat for row in layout.rows if row.repeat]
        self.assertEqual([repeat.count for repeat in repeats], [])
        self.assertIn(bursts[2][1], drawn(layout))

    def test_a_burst_with_a_cause_across_its_edge_is_not_collapsed(self):
        bursts = [cycle(hour) for hour in range(4)]
        bursts[2][0].caused_by_id = bursts[0][0].id

        layout = build_layout([event for burst in bursts for event in burst])

        self.assertFalse([row for row in layout.rows if row.repeat])

    def test_a_target_inside_a_repeat_opens_it(self):
        bursts = [cycle(hour) for hour in range(4)]
        wanted = bursts[2][1]

        layout = build_layout([event for burst in bursts for event in burst], target=wanted.id)

        repeat = next(row.repeat for row in layout.rows if row.repeat)
        self.assertTrue(repeat.open)
        self.assertEqual(repeat.target, wanted.id)

    def test_it_names_what_repeats(self):
        bursts = [cycle(hour) for hour in range(3)]

        repeat = next(row.repeat for row in build_layout([e for b in bursts for e in b]).rows if row.repeat)

        # In the order one burst runs, read from the events (their types are not installed in this test).
        self.assertEqual(
            repeat.shape,
            ["georivaevents.sweepintervened", "ingestion.fileingestionfinished", "core.productpublished"],
        )

    def test_a_type_that_occurs_more_than_once_a_burst_says_how_often(self):
        bursts = [[*cycle(hour), entry(hour * 3600 + 4, event_type="core.productpublished")] for hour in range(3)]

        repeat = next(row.repeat for row in build_layout([e for b in bursts for e in b]).rows if row.repeat)

        self.assertEqual(repeat.shape[-1], "2 × core.productpublished")


class TargetTests(SimpleTestCase):
    def test_the_target_event_is_marked(self):
        first = entry(0)
        second = entry(4, cause=first)

        layout = build_layout([first, second], target=second.id)

        self.assertEqual([row.target for row in layout.rows], [False, True])

    def test_a_target_inside_a_group_opens_it(self):
        root = entry(0, event_type="sources.fetchrunfinished")
        files = [entry(i + 1, cause=root) for i in range(4)]

        group = build_layout([root, *files], target=files[2].id).rows[1].group

        self.assertTrue(group.open)
        self.assertEqual(group.target, files[2].id)

    def test_the_first_failure_is_where_the_header_points(self):
        first = entry(0)
        refused = entry(1, cause=first, outcome="refused")
        failed = entry(2, cause=first, outcome="failed")

        layout = build_layout([first, refused, failed])

        self.assertEqual(layout.first_failure, refused.id)
        self.assertEqual(layout.counts, {"ok": 1, "failed": 1, "refused": 1})


class FocusTests(SimpleTestCase):
    def test_focus_draws_one_branch_from_its_root(self):
        root = entry(0, event_type="sources.fetchrunfinished")
        files = [entry(i + 1, cause=root) for i in range(4)]
        derived = [entry(40 + i, cause=files[1], event_type=f"x.d{i}") for i in range(2)]

        layout = build_layout([root, *files, *derived], focus=files[1].id)

        self.assertEqual(drawn(layout), [files[1], *derived])
        self.assertFalse(layout.rows[0].stub)
        self.assertEqual(layout.started, files[1].occurred_at)

    def test_focus_on_an_event_not_in_the_chain_is_none(self):
        self.assertIsNone(build_layout([entry(0)], focus=uuid.uuid4()))


class ElapsedTests(SimpleTestCase):
    def test_it_reads_like_a_stopwatch(self):
        cases = {
            0: "0s",
            4: "4s",
            123: "2m 03s",
            3 * 3600 + 12 * 60 + 55: "3h 12m 55s",
            2 * 86400 + 3 * 3600 + 5: "2d 3h",
        }
        for seconds, text in cases.items():
            self.assertEqual(elapsed(datetime.timedelta(seconds=seconds, microseconds=400)), text)
