"""The guard on tests existing but never running.

`georiva/edr/` had no ``__init__.py`` — the only package in the tree without
one. Django's test discovery skips namespace packages, because it reads
``module.__file__`` and that is ``None`` for them, so ``edr/tests.py`` was never
imported. Five tests sat in the repository, were never collected by any run
local or CI, and nobody found out. The suite reported 1780 tests and passed; it
simply never mentioned the five it could not see.

Nothing in the project could have caught that. Its sibling
``test_shard_partition`` compares the shards against Django's discovery, so
tests invisible to discovery are invisible to it too — both sides of that
comparison were missing the same five. A green suite is only evidence about the
tests it ran, and the number it prints is not a claim about the tests it didn't.

So this asserts the thing discovery needs in order to see a test at all: an
unbroken chain of real packages from the package root down to the file. It is a
filesystem check, deliberately, because importing is the thing that was broken.
"""

from pathlib import Path

from django.test import SimpleTestCase

import georiva

#: The importable root. Every test file must be reachable from here by a chain
#: of directories that each carry an __init__.py.
PACKAGE_ROOT = Path(georiva.__file__).resolve().parent

#: Django's default discovery pattern. Matching it matters: a file this finds
#: and discovery does not is exactly the bug, and a file discovery finds and
#: this does not would be a hole in the guard.
TEST_FILE_GLOB = "test*.py"

IGNORED_DIRS = {"__pycache__", "migrations"}


def _test_files():
    for path in sorted(PACKAGE_ROOT.rglob(TEST_FILE_GLOB)):
        if IGNORED_DIRS.intersection(path.parts):
            continue
        yield path


def _broken_package_chain(path):
    """The directories between PACKAGE_ROOT and ``path`` that are not packages.

    Returned rather than asserted on directly so a failure can name the missing
    ``__init__.py`` instead of only the file that went unrun because of it.
    """
    missing = []
    directory = path.parent
    while True:
        if not (directory / "__init__.py").exists():
            missing.append(directory)
        if directory == PACKAGE_ROOT:
            break
        directory = directory.parent
    return missing


class TestDiscoveryReachabilityTests(SimpleTestCase):
    def test_the_guard_is_looking_at_something(self):
        """A guard that silently matches nothing passes for the wrong reason."""
        found = list(_test_files())
        self.assertGreater(
            len(found), 50, f"only {len(found)} test files found under {PACKAGE_ROOT} — is the glob still right?"
        )

    def test_every_test_file_sits_in_an_importable_package(self):
        """The assertion this module exists to make.

        A directory without ``__init__.py`` is a namespace package, which
        discovery walks straight past. Any test file underneath one is dead
        weight: present, passing when run by hand, and absent from every count
        the suite reports.
        """
        broken = {}
        for path in _test_files():
            missing = _broken_package_chain(path)
            if missing:
                broken[str(path.relative_to(PACKAGE_ROOT))] = [str(d.relative_to(PACKAGE_ROOT)) or "." for d in missing]

        self.assertEqual(
            broken,
            {},
            "these test files cannot be reached by test discovery, so they never "
            f"run and nothing says so — add __init__.py to the directories listed: {broken}",
        )
