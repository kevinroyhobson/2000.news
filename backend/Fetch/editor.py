"""
The assignment editor: decides which fetched stories make the paper.

Fetch gathers a wide pool of candidates from every source, and one model call
reads them all and picks the ones most worth satirizing, each with a note on
why. The note is saved on the story and handed to the brainstorm prompt, so
the writers start from the editor's read on the joke.
"""

import os
import random
from dataclasses import dataclass, replace

import anthropic

from lib.anthropic_batches import extract_text
from lib.llm_json import parse_json_response
from lib.ssm_secrets import get_secret

EDITOR_MODEL = os.getenv("EDITOR_MODEL", "claude-opus-5-5")
EDITOR_EFFORT = os.getenv("EDITOR_EFFORT", "medium")
DESCRIPTION_CHARS = 300

EDITOR_SYSTEM_PROMPT = """You are the assignment editor at a satirical newspaper (The Onion meets SimCity 2000). A few times a day the wires hand you a pile of real stories, and you decide which ones go to the headline writers. Each story you pick becomes raw material for deadpan, darkly funny fake headlines, so you are choosing what to satirize, not the news of record.

WHAT MAKES A STORY WORTH ASSIGNING:
- PARODYABLE: the story already contains the setup. Powerful people or institutions behaving absurdly or hypocritically, bureaucratic madness, a premise that sounds made up, built-in irony, concrete specifics a writer can twist, names and phrases ripe for wordplay.
- IMPORTANT: the stories everyone is talking about today. Satire of the big story lands because readers already feel it, so a major story with an obvious target belongs on the list even when its jokes take more work.
- The best picks are both. Fill out the list with stories that are strongly one or the other.

WHAT TO PASS ON:
- Stories whose only joke punches down at victims, the grieving, or people with no power. Tragedy with a powerful party to blame is fair game; tragedy alone is not.
- Service pieces and filler: how-to-watch guides, deals and product roundups, live blogs, recaps with nothing to hook onto, puzzle and newsletter promos.
- A second story about an event you already picked. Different outlets covering the same news count as one story; keep the version with the richest details.

Balance the list across topics (politics, business, tech, world, sports, culture, the weird), and don't let one source dominate.

RESPONSE FORMAT:
Return a JSON array ordered from strongest pick to weakest:
[{"id": 12, "why": "..."}]
- id: the candidate's number
- why: one sentence for the headline writers naming what makes this story ripe: the absurdity, the hypocrisy, the wordplay hook. Be specific; it goes straight to the writer."""

_client = None


@dataclass(frozen=True)
class Candidate:
    story: dict
    label: str
    note: str = ""


def pick_stories(candidates: list, count: int) -> list:
    """The editor's picks, strongest first, each carrying its note. Raises if
    the call fails; an answer naming no real candidates comes back empty."""
    shuffled = random.sample(candidates, len(candidates))
    message = _anthropic_client().messages.create(
        model=EDITOR_MODEL,
        # Opus 5.5 always thinks, and thinking counts against max_tokens.
        max_tokens=16000,
        output_config={"effort": EDITOR_EFFORT},
        system=EDITOR_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_prompt(shuffled, count)}],
    )
    return parse_picks(extract_text(message), shuffled, count)


def build_prompt(candidates: list, count: int) -> str:
    listing = "\n\n".join(_describe(i, c) for i, c in enumerate(candidates, start=1))
    return f"Pick the {count} stories to assign.\n\n{listing}"


def parse_picks(response_text: str, candidates: list, count: int) -> list:
    """Map the editor's ids back to candidates, skipping ids that don't name
    one and repeats of an id already picked."""
    picks = []
    picked_ids = set()
    for choice in parse_json_response(response_text):
        if len(picks) >= count:
            break
        candidate_id = choice.get("id") if isinstance(choice, dict) else None
        if not isinstance(candidate_id, int) or candidate_id in picked_ids:
            continue
        if not 1 <= candidate_id <= len(candidates):
            continue
        picked_ids.add(candidate_id)
        note = str(choice.get("why") or "").strip()
        picks.append(replace(candidates[candidate_id - 1], note=note))
    return picks


def _describe(candidate_id: int, candidate: Candidate) -> str:
    story = candidate.story
    description = " ".join((story.get("description") or "").split())
    if len(description) > DESCRIPTION_CHARS:
        description = description[:DESCRIPTION_CHARS].rstrip() + "…"
    source = story.get("source_id") or "unknown"
    return f"[{candidate_id}] {story['title']}\n({source}) {description}".rstrip()


def _anthropic_client():
    global _client
    if _client is None:
        # Bounded so a slow call plus its one retry still finishes inside the
        # Fetch Lambda's timeout, leaving time to save the fallback picks.
        _client = anthropic.Anthropic(
            api_key=get_secret("ANTHROPIC_API_KEY"), timeout=180, max_retries=1,
        )
    return _client
