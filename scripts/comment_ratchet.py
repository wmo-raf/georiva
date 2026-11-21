import argparse
import ast
import io
import json
import subprocess
import sys
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASELINE = ROOT / "scripts" / "comment-baseline.json"
EXCLUDED_PARTS = ("/migrations/", "source-plugin-boilerplate/", "dev-plugins/")
DIRECTIVES = ("# noqa", "# type:", "# pragma:", "# fmt:", "# ruff:", "# -*-")


def tracked_python_files():
    command = ["git", "ls-files", "--cached", "--others", "--exclude-standard", "*.py"]
    listing = subprocess.check_output(command, cwd=ROOT, text=True)
    return sorted(path for path in listing.splitlines() if not any(part in "/" + path for part in EXCLUDED_PARTS))


def is_directive(comment, row):
    text = comment.strip()
    if row == 1 and text.startswith("#!"):
        return True
    return text.startswith(DIRECTIVES)


def comment_rows(source):
    rows = set()
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT and not is_directive(token.string, token.start[0]):
            rows.add(token.start[0])
    return rows


def docstring_rows(source):
    rows = set()
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        if not node.body:
            continue
        first = node.body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            rows.update(range(first.lineno, first.end_lineno + 1))
    return rows


def count(path):
    source = (ROOT / path).read_text(encoding="utf-8")
    return len(comment_rows(source) | docstring_rows(source))


def current_counts():
    counts = {}
    for path in tracked_python_files():
        if not (ROOT / path).exists():
            continue
        lines = count(path)
        if lines:
            counts[path] = lines
    return counts


def load_baseline():
    if not BASELINE.exists():
        return {}
    return json.loads(BASELINE.read_text(encoding="utf-8"))


def write_baseline(counts):
    BASELINE.write_text(json.dumps(dict(sorted(counts.items())), indent=1) + "\n", encoding="utf-8")


def check(counts, baseline):
    grown = {path: (lines, baseline.get(path, 0)) for path, lines in counts.items() if lines > baseline.get(path, 0)}
    slack = {
        path: (counts.get(path, 0), allowed) for path, allowed in baseline.items() if counts.get(path, 0) < allowed
    }
    return grown, slack


def main():
    parser = argparse.ArgumentParser(description="Comment ratchet: comment and docstring lines may only go down.")
    parser.add_argument("--update", action="store_true", help="lower the baseline to the current counts")
    parser.add_argument("--init", action="store_true", help="write the baseline from scratch")
    args = parser.parse_args()

    counts = current_counts()
    if args.init:
        write_baseline(counts)
        print(f"comment ratchet: baseline written, {sum(counts.values())} lines in {len(counts)} files")
        return 0

    baseline = load_baseline()
    grown, slack = check(counts, baseline)

    if grown:
        print("comment ratchet: comments or docstrings were added. Comments are banned; delete them.")
        print("Record a non-obvious decision in docs/adr/ instead (see .claude/skills/delete-comment).")
        for path, (lines, allowed) in sorted(grown.items()):
            print(f"  {path}: {lines} comment lines, {allowed} allowed")
        return 1

    if args.update:
        write_baseline(counts)
        shed = sum(baseline.values()) - sum(counts.values())
        print(f"comment ratchet: baseline lowered by {shed} lines, {sum(counts.values())} remain")
        return 0

    if slack:
        print("comment ratchet: comments were deleted but the baseline still allows them.")
        print("Run `just format` (or `just comment-ratchet-update`) and commit scripts/comment-baseline.json.")
        for path, (lines, allowed) in sorted(slack.items()):
            print(f"  {path}: {lines} comment lines, baseline {allowed}")
        return 1

    print(f"comment ratchet: ok, {sum(counts.values())} lines in {len(counts)} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
