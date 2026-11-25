"""What the Style template catalogue generators share (#789, #811).

Each generator (``earthkit_styles/generate.py``, ``magics_styles/generate.py``)
reads one upstream, pinned to a GitHub commit in the ``catalogue.yml`` beside
it, and writes one vendored catalogue into ``core/style_catalogue/``. This is
the part that does not depend on which upstream it is: fetching the pin, the
catalogue's envelope and source block, and the command line around it. A
generator imports it by path (``scripts/`` is not a package):

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import style_sources
"""

import argparse
import io
import itertools
import json
import tarfile
import tempfile
import urllib.request
from pathlib import Path

import yaml

#: Where the vendored catalogues live.
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "src" / "georiva" / "core" / "style_catalogue"

#: Both upstreams are ECMWF's, under Apache-2.0 (see NOTICE).
LICENSE = "Apache-2.0"


class Skip(Exception):
    """A style (or one use of it) that cannot be taken faithfully; ``str()`` is why."""


def strictly_ascending(levels) -> bool:
    return all(b > a for a, b in itertools.pairwise(levels))


def fetch(source, into):
    """Download and unpack ``source``'s pinned commit from GitHub; return its root."""
    repository = source["repository"].removeprefix("https://github.com/")
    url = f"https://codeload.github.com/{repository}/tar.gz/{source['commit']}"
    with urllib.request.urlopen(url, timeout=120) as response:  # noqa: S310 - a fixed https URL
        data = response.read()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        archive.extractall(into, filter="data")
    (root,) = Path(into).iterdir()
    return root


def catalogue(source, *, tag, copyright_line, modified, generator, units, templates):
    """A catalogue as ``load_catalogue`` reads it: where it came from, the units its
    templates are declared in, and the templates."""
    return {
        "source": {
            "name": source["name"],
            "repository": source["repository"],
            "tag": tag,
            "commit": source["commit"],
            "license": LICENSE,
            "copyright": copyright_line,
            "modified": modified,
        },
        "generator": generator,
        "units": units,
        "templates": templates,
    }


def run(doc, config_path, *, generate, output_name, report):
    """A generator's command line: read ``config_path``, fetch the pin (or take
    ``--source``, a checkout of it), write the catalogue and report.

    ``generate(root, config)`` returns ``(catalogue, skipped)``; ``output_name(source)``
    names the file; ``report(catalogue, skipped, output)`` prints what was done.
    """
    parser = argparse.ArgumentParser(description=doc.split("\n\n")[0])
    parser.add_argument("--source", type=Path, help="a checkout at the pinned commit (default: fetch it)")
    parser.add_argument("--output", type=Path, help="where to write the catalogue JSON")
    args = parser.parse_args()

    config = yaml.safe_load(Path(config_path).read_text())
    output = args.output or OUTPUT_DIR / output_name(config["source"])
    if args.source:
        built, skipped = generate(args.source, config)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            built, skipped = generate(fetch(config["source"], tmp), config)

    output.write_text(json.dumps(built, indent=1, ensure_ascii=False) + "\n")
    report(built, skipped, output)
    return 0
