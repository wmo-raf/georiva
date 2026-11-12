#!/usr/bin/env bash
# Reads changed paths on stdin, one per line. Prints "true" when any of them
# could change a test result, "false" when every one is documentation:
# anything under docs/, or a Markdown file anywhere.
#
# No paths at all prints "true": if we could not work out what changed, test.
set -euo pipefail

seen=0
while IFS= read -r path; do
  [ -n "$path" ] || continue
  seen=1
  case "$path" in
    docs/* | *.md) ;;
    *) echo true; exit 0 ;;
  esac
done

[ "$seen" -eq 1 ] && echo false || echo true
