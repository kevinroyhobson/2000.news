#!/usr/bin/env bash
# Copies Times New Roman and Courier New into the Lambda package. Their license
# lets a machine install them but not us commit them, so each deploy copies them
# in from the machine doing the deploy: a Mac, or Linux with
# ttf-mscorefonts-installer.

set -euo pipefail

FONTS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fonts"

stage() {
    local name="$1"
    shift
    for candidate in "$@"; do
        if [[ -f "$candidate" ]]; then
            cp "$candidate" "$FONTS/$name"
            return
        fi
    done
    echo "stage_fonts: no $name found. Install ttf-mscorefonts-installer, or deploy from a Mac." >&2
    exit 1
}

stage times.ttf \
    "/System/Library/Fonts/Supplemental/Times New Roman.ttf" \
    /usr/share/fonts/truetype/msttcorefonts/Times_New_Roman.ttf
stage cour.ttf \
    "/System/Library/Fonts/Supplemental/Courier New.ttf" \
    /usr/share/fonts/truetype/msttcorefonts/Courier_New.ttf
