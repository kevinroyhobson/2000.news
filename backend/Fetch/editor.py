"""
The assignment editor: decides which fetched stories make the paper.

Fetch gathers a wide pool of candidates from every source, and one model call
reads them all and picks the ones most worth satirizing, each with a note on
why. A pick is usually one story, but a few can be mashups: 2-3 stories that
collide into a joke none of them makes alone. The note is saved on the story
and handed to the brainstorm prompt, so the writers start from the editor's
read on the joke.
"""

import os
import random
from dataclasses import dataclass

import anthropic

from lib.anthropic_batches import extract_text
from lib.llm_json import parse_json_response
from lib.ssm_secrets import get_secret

EDITOR_MODEL = os.getenv("EDITOR_MODEL", "claude-opus-5-5")
EDITOR_EFFORT = os.getenv("EDITOR_EFFORT", "medium")
DESCRIPTION_CHARS = 300
MAX_STORIES_PER_MASHUP = 3

EDITOR_SYSTEM_PROMPT = """You are the assignment editor at a satirical newspaper (The Onion meets SimCity 2000). A few times a day the wires hand you a pile of real stories, and you decide which ones go to the headline writers. Each story you pick becomes raw material for deadpan, darkly funny fake headlines, so you are choosing what to satirize, not the news of record.

WHAT MAKES A STORY WORTH ASSIGNING (every pick needs at least one; the best have several):
- PARODYABLE: the story already contains the setup. Powerful people or institutions behaving absurdly or hypocritically, bureaucratic madness, built-in irony, concrete specifics a writer can twist, names and phrases ripe for wordplay.
- IMPORTANT: the stories everyone is talking about today. Satire of the big story lands because readers already feel it, so a major story with an obvious target belongs on the list even when its jokes take more work.
- ALREADY SOUNDS FAKE: a real headline a reader would swear came from a satire site. The paper sometimes runs the real headline among the fakes, and the moment a reader can't tell which is which is the point. These are often small stories buried deep in the pile that nobody is talking about yet; dig for them.
- UNFAIR AND ABSURD: stories that show how random and unjust the world is. The paper exists to say that out loud, so tragedy belongs on the list: random suffering, cruelty, dumb luck, and the strange machinery that rolls on around them. The writers aim the joke at the world, not at the people it happened to.

WHAT TO PASS ON:
- Service pieces and filler: how-to-watch guides, deals and product roundups, live blogs, recaps with nothing to hook onto, puzzle and newsletter promos.
- A second story about an event you already picked. Different outlets covering the same news count as one story; keep the version with the richest details.

MASHUPS:
A few picks can combine 2 or 3 stories into one assignment, when they collide into a joke none of them makes alone: a billionaire christening his third yacht the same week his company's warehouse workers unionize, say. The collision is the point; stories that merely share a topic aren't a mashup. Zero mashups is a fine answer, and the assignment says how many you may use at most. A story can run alone and in one mashup if both earn their slot, but never in two mashups. Candidates marked as already running are the paper's regular features and run on their own no matter what; use them only inside mashups.

BUILD A FRONT PAGE, NOT A RANKING:
Your picks run together, so think about the mix. Swing between the world-historic and the stupid; a war next to a raccoon stuck in a vending machine is funnier than either alone. Spread across topics (politics, business, tech, world, sports, culture, the weird), and don't let one source dominate.

RESPONSE FORMAT:
Return a JSON array ordered from strongest pick to weakest:
[{"ids": [12], "why": "..."}, {"ids": [3, 40], "why": "..."}]
- ids: the candidate's number for a single story, or 2-3 numbers for a mashup. List the story whose photo should run first.
- why: one sentence for the headline writers naming what makes this pick worth it: the absurdity, the hypocrisy, the unfairness, the wordplay hook, or for a mashup, the collision. Be specific; it goes straight to the writer."""

_client = None


@dataclass(frozen=True)
class Candidate:
    story: dict
    label: str
    pinned: bool = False


@dataclass(frozen=True)
class Pick:
    sources: tuple
    note: str = ""

    @property
    def is_mashup(self) -> bool:
        return len(self.sources) > 1


def pick_stories(candidates: list, count: int, max_mashups: int) -> list:
    """The editor's picks, strongest first, each carrying its note. Raises if
    the call fails; an answer naming no real candidates comes back empty."""
    shuffled = random.sample(candidates, len(candidates))
    message = _anthropic_client().messages.create(
        model=EDITOR_MODEL,
        # Opus 5.5 always thinks, and thinking counts against max_tokens.
        max_tokens=16000,
        output_config={"effort": EDITOR_EFFORT},
        system=EDITOR_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_prompt(shuffled, count, max_mashups)}],
    )
    return parse_picks(extract_text(message), shuffled, count, max_mashups)


def build_prompt(candidates: list, count: int, max_mashups: int) -> str:
    listing = "\n\n".join(_describe(i, c) for i, c in enumerate(candidates, start=1))
    instruction = f"Pick the {count} stories to assign, up to {max_mashups} of them mashups."
    return f"{instruction}\n\n{listing}\n\n{instruction}"


def parse_picks(response_text: str, candidates: list, count: int, max_mashups: int) -> list:
    """Map the editor's ids back to candidates. Skips picks that name a
    nonexistent candidate, run a story alone twice, run a pinned story alone,
    put a story in a second mashup, or go past the mashup cap."""
    picks = []
    solo_ids = set()
    mashup_ids = set()
    mashups = 0
    for choice in parse_json_response(response_text):
        if len(picks) >= count:
            break
        ids = _candidate_ids(choice, len(candidates))
        if not ids:
            continue
        sources = tuple(candidates[i - 1] for i in ids)
        if len(sources) == 1:
            if sources[0].pinned or ids[0] in solo_ids:
                continue
            solo_ids.add(ids[0])
        else:
            if mashups >= max_mashups or not mashup_ids.isdisjoint(ids):
                continue
            mashups += 1
            mashup_ids.update(ids)
        picks.append(Pick(sources=sources, note=str(choice.get("why") or "").strip()))
    return picks


def _candidate_ids(choice, candidate_count: int):
    """The pick's candidate numbers, or None if they don't name 1 to
    MAX_STORIES_PER_MASHUP distinct candidates."""
    ids = choice.get("ids") if isinstance(choice, dict) else None
    if not isinstance(ids, list) or not 1 <= len(ids) <= MAX_STORIES_PER_MASHUP:
        return None
    if not all(isinstance(i, int) and 1 <= i <= candidate_count for i in ids):
        return None
    if len(set(ids)) != len(ids):
        return None
    return ids


def _describe(candidate_id: int, candidate: Candidate) -> str:
    story = candidate.story
    description = " ".join((story.get("description") or "").split())
    if len(description) > DESCRIPTION_CHARS:
        description = description[:DESCRIPTION_CHARS].rstrip() + "…"
    source = story.get("source_id") or "unknown"
    if candidate.pinned:
        source += ", already running: mashups only"
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
