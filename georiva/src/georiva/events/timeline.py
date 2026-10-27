"""The Timeline page for one chain (#429): where it lives, and how its rail is laid out.

A chain is a tree — every event may name the event that caused it — so the page
draws a rail in the manner of ``git log --graph``: rows strictly oldest first,
one lane per branch. :func:`build_layout` does all of that in Python, so the
template only paints cells and the layout can be tested without a browser.

A chain can be large two ways, and each has its own fold:

- *Wide*: a fetch run causes every file it brings in. Siblings of one type
  fold into a group once there are :data:`FOLD_AT` of them, and everything a
  folded group causes folds again by type, level by level. Failed and refused
  members are pulled out of a group, so a fold never hides one.
- *Long*: a run's chain lasts days, and a quiet spell longer than :data:`GAP`
  breaks the rail into bursts. Folding never reaches across a break, so the
  order of what happened survives; instead, consecutive bursts of one shape —
  the same types the same number of times, nothing failed — collapse into one
  repeat row after the first of them is drawn.
"""

import itertools
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta

from django.urls import NoReverseMatch, reverse

from .models import OutcomeClass

#: How many same-type siblings in one burst fold into a group.
FOLD_AT = 4
#: A quiet spell longer than this breaks the rail into bursts.
GAP = timedelta(minutes=10)
#: How many bursts must repeat the first of their shape before they collapse.
REPEAT_AT = 2


def timeline_url(correlation_id, event_id=None):
    """The chain's Timeline, landing on ``event_id`` when one is given."""
    try:
        url = reverse("events_timeline", args=[correlation_id])
    except NoReverseMatch:
        return None
    # The fragment scrolls; the query tells the server which fold to open.
    return f"{url}?event={event_id}#event-{event_id}" if event_id else url


def elapsed(delta) -> str:
    """``4s``, ``2m 03s``, ``3h 12m 55s``, ``2d 3h`` — a stopwatch, not a clock."""
    seconds = int(delta.total_seconds())
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes:02d}m {seconds:02d}s"
    if minutes:
        return f"{minutes}m {seconds:02d}s"
    return f"{seconds}s"


@dataclass(eq=False)
class Entry:
    """One indexed event of the chain. ``row`` is ``None`` when its type is not installed here."""

    id: object
    event_type: str
    occurred_at: object
    outcome_class: str
    caused_by_id: object = None
    row: object = None
    offset: timedelta | None = None
    burst: int = 0


def _label(members, event_type):
    """What a type is called, read from any of its events that can be read."""
    readable = next((member.row for member in members if member.row is not None), None)
    return readable.type_label if readable else event_type


@dataclass
class Cell:
    """One lane of one rail row: which edges a line touches, and whether a dot sits in it."""

    n: bool = False
    s: bool = False
    e: bool = False
    w: bool = False
    dot: str = ""  # an outcome class, "group", "repeat", or "" for a line passing through
    dashed: bool = False  # a gap row: the whole cell is a dashed line
    dashed_n: bool = False  # a root whose cause is outside this view


@dataclass(eq=False)
class Group:
    """Siblings of one type in one burst, folded into one node."""

    event_type: str
    members: list
    start: timedelta = None
    end: timedelta = None
    counts: dict = field(default_factory=dict)
    pulled_out: list = field(default_factory=list)
    focusable: set = field(default_factory=set)
    bar_left: float = 0.0
    bar_width: float = 100.0
    open: bool = False
    target: object = None

    @property
    def anchor(self):
        return f"group-{self.members[0].id}"

    @property
    def label(self):
        return _label(self.members, self.event_type)

    @property
    def outcome_class(self):
        for outcome in (OutcomeClass.FAILED, OutcomeClass.REFUSED):
            if self.counts.get(outcome):
                return outcome
        return OutcomeClass.OK


@dataclass(eq=False)
class Repeat:
    """Consecutive bursts with the shape of the burst drawn just above them."""

    members: list
    count: int
    every: timedelta
    shape: list
    start: timedelta = None
    end: timedelta = None
    open: bool = False
    target: object = None

    @property
    def anchor(self):
        return f"repeat-{self.members[0].id}"


@dataclass
class RailRow:
    cells: list
    entry: Entry | None = None
    group: Group | None = None
    repeat: Repeat | None = None
    gap: timedelta | None = None
    offset: timedelta | None = None
    stub: bool = False
    target: bool = False


@dataclass
class Layout:
    rows: list
    lanes: int
    started: object
    duration: timedelta
    counts: dict
    first_failure: object = None

    @property
    def size(self):
        """How many events are shown, folded or not."""
        return sum(self.counts.values())


@dataclass(eq=False)
class _Node:
    entry: Entry | None = None
    group: Group | None = None
    repeat: Repeat | None = None
    children: list = field(default_factory=list)
    parent: "_Node | None" = None
    stub: bool = False
    time: object = None
    depth: int = 0
    seq: int = 0
    row: int = 0
    lane: int = 0
    end: int = 0  # the last row this node's lane is busy to

    @property
    def span(self):
        """When the first and last event this node draws occurred."""
        if self.entry:
            return self.entry.occurred_at, self.entry.occurred_at
        members = (self.group or self.repeat).members
        return members[0].occurred_at, members[-1].occurred_at


def _counts(entries):
    tally = Counter(entry.outcome_class for entry in entries)
    return {outcome: tally.get(outcome, 0) for outcome in OutcomeClass.values}


def _subtree(entries, root_id, children_of):
    keep, stack = set(), [root_id]
    while stack:
        current = stack.pop()
        keep.add(current)
        stack.extend(child.id for child in children_of.get(current, ()))
    return [entry for entry in entries if entry.id in keep]


def _bursts(entries):
    """``entries`` (oldest first) split wherever nothing happened for longer than :data:`GAP`."""
    bursts, previous = [], None
    for entry in entries:
        if previous is None or entry.occurred_at - previous > GAP:
            bursts.append([])
        bursts[-1].append(entry)
        entry.burst = len(bursts) - 1
        previous = entry.occurred_at
    return bursts


def _repeats(bursts, children_of, target):
    """Runs of bursts that repeat the shape of the burst before them."""

    def shape(burst):
        # Only a burst no causal line crosses can leave the rail without breaking a line.
        ids = {entry.id for entry in burst}
        if any(entry.outcome_class != OutcomeClass.OK for entry in burst):
            return None
        if any(entry.caused_by_id is not None and entry.caused_by_id not in ids for entry in burst):
            return None
        if any(child.id not in ids for entry in burst for child in children_of.get(entry.id, ())):
            return None
        return tuple(sorted(Counter(entry.event_type for entry in burst).items()))

    shapes = [shape(burst) for burst in bursts]
    repeats, first = [], 0
    while first < len(bursts):
        last = first
        while shapes[first] is not None and last + 1 < len(bursts) and shapes[last + 1] == shapes[first]:
            last += 1
        if last - first >= REPEAT_AT:
            repeats.append(_repeat(bursts[first + 1 : last + 1], target))
        first = last + 1
    return repeats


def _repeat(bursts, target):
    members = [entry for burst in bursts for entry in burst]
    starts = [burst[0].occurred_at for burst in bursts]
    every = statistics.median((later - earlier).total_seconds() for earlier, later in itertools.pairwise(starts))
    times = Counter(entry.event_type for entry in bursts[0])
    shape = []
    for event_type in dict.fromkeys(entry.event_type for entry in bursts[0]):
        label = _label([entry for entry in members if entry.event_type == event_type], event_type)
        shape.append(label if times[event_type] == 1 else f"{times[event_type]} × {label}")
    repeat = Repeat(
        members=members,
        count=len(bursts),
        every=timedelta(seconds=round(every)),
        shape=shape,
        start=members[0].offset,
        end=members[-1].offset,
    )
    if target is not None and any(member.id == target for member in members):
        repeat.open, repeat.target = True, target
    return repeat


def build_layout(entries, *, target=None, focus=None) -> Layout | None:
    """Lay ``entries`` (one chain, any order) out as rail rows.

    ``focus`` draws only that event and what it caused; ``None`` comes back
    when it is not among ``entries``. ``target`` marks one event and opens the
    group or repeat that holds it.
    """
    entries = sorted(entries, key=lambda entry: (entry.occurred_at, str(entry.id)))
    children_of = defaultdict(list)
    for entry in entries:
        if entry.caused_by_id is not None:
            children_of[entry.caused_by_id].append(entry)
    if focus is not None:
        if not any(entry.id == focus for entry in entries):
            return None
        entries = _subtree(entries, focus, children_of)

    started = entries[0].occurred_at
    finished = max(entry.occurred_at for entry in entries)
    total = (finished - started).total_seconds()
    for entry in entries:
        entry.offset = entry.occurred_at - started
    repeats = _repeats(_bursts(entries), children_of, target)
    collapsed = {member.id for repeat in repeats for member in repeat.members}
    shown = {entry.id for entry in entries if entry.id not in collapsed}

    nodes = []

    def add(node, parent):
        node.parent = parent
        node.depth = parent.depth + 1 if parent else 0
        node.seq = len(nodes)
        # A cause is always drawn above its effect, even when clocks disagree.
        node.time = max(node.span[0], parent.time) if parent else node.span[0]
        nodes.append(node)
        if parent:
            parent.children.append(node)
        return node

    def fold(event_type, members, parent):
        group = Group(event_type=event_type, members=members)
        group.start, group.end = members[0].offset, members[-1].offset
        group.counts = _counts(members)
        group.pulled_out = [member for member in members if member.outcome_class != OutcomeClass.OK]
        group.focusable = {member.id for member in members if children_of.get(member.id)}
        if total:
            group.bar_left = round(group.start.total_seconds() / total * 100, 1)
            group.bar_width = max(round((group.end - group.start).total_seconds() / total * 100, 1), 1.0)
        if target is not None and any(member.id == target for member in members):
            group.open, group.target = True, target
        node = add(_Node(group=group), parent)
        effects = [child for member in members for child in children_of.get(member.id, ()) if child.id in shown]
        place(sorted(effects, key=lambda entry: (entry.occurred_at, str(entry.id))), node, folded=True)

    def place(siblings, parent, *, folded=False):
        # Siblings fold by type within their burst, never across a break.
        by_kind = defaultdict(list)
        for sibling in siblings:
            by_kind[sibling.event_type, sibling.burst].append(sibling)
        for sibling in siblings:
            kind = sibling.event_type, sibling.burst
            members = by_kind.get(kind)
            if members is None:
                continue  # already placed as part of its kind's group
            if folded or len(members) >= FOLD_AT:
                del by_kind[kind]
                fold(sibling.event_type, members, parent)
                continue
            node = add(_Node(entry=sibling), parent)
            node.stub = parent is None and sibling.caused_by_id is not None and sibling.id != focus
            place(children_of.get(sibling.id, ()), node)

    place([entry for entry in entries if entry.id in shown and entry.caused_by_id not in shown], None)
    for repeat in repeats:
        add(_Node(repeat=repeat), None)

    nodes.sort(key=lambda node: (node.time, node.depth, node.seq))
    for index, node in enumerate(nodes):
        node.row = index

    # The last effect is the heir: it carries its cause's lane on, and earlier
    # effects branch off before it, so every junction sits on the cause's line.
    heirs = {}
    for node in reversed(nodes):
        heir = max(node.children, key=lambda child: child.row, default=None)
        heirs[node] = heir
        node.end = heir.end if heir else node.row
    busy = defaultdict(list)  # lane → [(first row, last row)]
    for node in nodes:
        if node.parent is not None and heirs[node.parent] is node:
            node.lane = node.parent.lane
        else:
            node.lane = next(
                lane
                for lane in range(len(nodes) + 1)
                if all(node.end < first or node.row > last for first, last in busy[lane])
            )
            busy[node.lane].append((node.row, node.end))

    lanes = max(node.lane for node in nodes) + 1
    grid = [[Cell() for _ in range(lanes)] for _ in nodes]
    for node in nodes:
        here = grid[node.row][node.lane]
        here.dot = node.entry.outcome_class if node.entry else "group" if node.group else "repeat"
        if node.stub:
            here.n = here.dashed_n = True
        if not node.children:
            continue
        last = heirs[node].row
        here.s = True
        for row in range(node.row + 1, last):
            grid[row][node.lane].n = grid[row][node.lane].s = True
        for child in node.children:
            junction = grid[child.row][node.lane]
            junction.n = True
            if child.lane == node.lane:
                continue
            junction.s = True
            towards, back = ("e", "w") if child.lane > node.lane else ("w", "e")
            setattr(junction, towards, True)
            step = 1 if child.lane > node.lane else -1
            for lane in range(node.lane + step, child.lane, step):
                grid[child.row][lane].e = grid[child.row][lane].w = True
            setattr(grid[child.row][child.lane], back, True)

    rows = []
    busy_until = None
    for node, cells in zip(nodes, grid, strict=True):
        start, end = node.span
        if busy_until is not None and start - busy_until > GAP:
            rows.append(
                RailRow(
                    cells=[Cell(n=cell.s, s=cell.s, dashed=cell.s) for cell in rows[-1].cells],
                    gap=start - busy_until,
                )
            )
        busy_until = end if busy_until is None else max(busy_until, end)
        rows.append(
            RailRow(
                cells=cells,
                entry=node.entry,
                group=node.group,
                repeat=node.repeat,
                offset=node.entry.offset if node.entry else (node.group or node.repeat).start,
                stub=node.stub,
                target=node.entry is not None and node.entry.id == target,
            )
        )

    failures = [entry for entry in entries if entry.outcome_class != OutcomeClass.OK]
    return Layout(
        rows=rows,
        lanes=lanes,
        started=started,
        duration=finished - started,
        counts=_counts(entries),
        first_failure=failures[0].id if failures else None,
    )
