"""The scoop editor: turns a /scoop request into stories worth writing up.

The request can be a name, a topic or a sentence ("find something about Haley
Stevens endorsing Mike Rogers"). Claude searches newsdata.io a few ways,
headlines first and then article text, and picks the results that are about
what was asked, each with a note for the writers. Stories the paper filed in
the last couple of days but never wrote up are on the table too.
"""

import datetime
import json
import os
from dataclasses import dataclass

from lib.anthropic_batches import extract_text
from lib.llm_json import parse_json_response

EDITOR_MODEL = os.getenv("SCOOP_EDITOR_MODEL", "claude-opus-5-5")
EDITOR_EFFORT = os.getenv("SCOOP_EDITOR_EFFORT", "medium")
FALLBACK_BETA = "server-side-fallback-2026-07-01"
MAX_SEARCHES = 4
MAX_TURNS = MAX_SEARCHES + 2
MAX_CLOSE_CALLS = 3
DESCRIPTION_CHARS = 300

SYSTEM_PROMPT = f"""You are the editor on duty at a satirical newspaper (The Onion meets SimCity 2000). Someone on staff has posted a request to the paper's channel: a name, a topic, or a sentence describing the story they want the writers to take on. Find real news stories that are about what they asked for. Each story you pick goes to the headline writers, who write about twenty fake headlines from its title and opening paragraphs, so a story that mentions the subject in passing gives them nothing to work with.

SEARCHING
search_news queries newsdata.io: recent articles (about the last two days) from major US outlets, ten results a search. You have {MAX_SEARCHES} searches; stop once you have what you need.
- Start narrow: the key name or phrase with title_only, so every result has the subject in its headline.
- Widen when that comes up short: search article text instead, or reword: the other people involved, the event's usual name, a shorter phrase.
- Query syntax: "quotes" for an exact phrase, AND between words that must all appear, OR for alternatives, NOT to exclude. Words with no operator between them match independently, anywhere.

PICKING
- Pick only stories about the request: the subject is the story, not a name in the ninth paragraph. If neither the title nor the description mentions the subject, the story isn't about it.
- When the request describes a specific event, pick coverage of that event. Another story about the same people is a close call, not a pick.
- Different outlets covering the same news count as one story; keep the version with the richest details.
- A story marked as unable to run can't be picked.
- Picking nothing is a fine answer. The requester would rather hear that nothing fits than get headlines about a story they didn't ask for.

ANSWER
When you're done searching, answer with JSON only, no prose:
{{"picks": [{{"id": 3, "why": "..."}}], "close_calls": [7, 12]}}
- picks: strongest first. why: one sentence for the headline writers naming what makes the story worth satirizing, aimed at what the requester asked for.
- close_calls: up to {MAX_CLOSE_CALLS} ids of the nearest misses, best first, for the requester to look over when picks come up short. Empty when nothing came close."""

SEARCH_TOOL = {
    "name": "search_news",
    "description": "Search recent news from major US outlets. Returns up to ten articles, each numbered for picking.",
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "newsdata.io query, using the syntax in your instructions."},
            "title_only": {"type": "boolean", "description": "Match the query against headlines only."},
        },
        "required": ["query", "title_only"],
        "additionalProperties": False,
    },
}


class EditorError(Exception):
    """Why the editor came back with nothing, worded for the channel."""


@dataclass(frozen=True)
class Candidate:
    title: str
    source: str
    description: str
    url: str
    result: dict = None  # a newsdata result the paper doesn't have yet
    filed: dict = None  # a Stories item the paper filed but never wrote up
    blocker: str = ""  # why it can't run, when it can't

    @classmethod
    def from_result(cls, result: dict, blocker: str = ""):
        return cls(title=result["title"], source=result.get("source_id") or "unknown",
                   description=result.get("description") or "", url=result.get("link") or "",
                   result=result, blocker=blocker)

    @classmethod
    def from_filed(cls, story: dict):
        return cls(title=story["Title"], source=story.get("Source") or "unknown",
                   description=story.get("Description") or "", url=story.get("Url") or "",
                   filed=story)


@dataclass(frozen=True)
class Pick:
    candidate: Candidate
    note: str


@dataclass(frozen=True)
class Assignment:
    picks: list
    close_calls: list


class Desk:
    """Every story the editor has seen on this request, numbered from 1 in
    the order it turned up. A title seen twice keeps its first number."""

    def __init__(self, unwritten: list, unavailable: dict):
        self.candidates = []
        self._ids_by_title = {}
        self._unavailable = unavailable
        for story in unwritten:
            self._add(Candidate.from_filed(story))

    def add_result(self, result: dict) -> int:
        if result["title"] in self._ids_by_title:
            return self._ids_by_title[result["title"]]
        return self._add(Candidate.from_result(result, self._blocker(result)))

    def get(self, candidate_id):
        if isinstance(candidate_id, int) and 1 <= candidate_id <= len(self.candidates):
            return self.candidates[candidate_id - 1]
        return None

    def describe(self, candidate_id: int) -> str:
        candidate = self.get(candidate_id)
        description = " ".join(candidate.description.split())
        if len(description) > DESCRIPTION_CHARS:
            description = description[:DESCRIPTION_CHARS].rstrip() + "…"
        lines = [f"[{candidate_id}] {candidate.title}", f"({candidate.source}) {description}".rstrip()]
        if candidate.blocker:
            lines.append(f"Can't run: {candidate.blocker}.")
        return "\n".join(lines)

    def _add(self, candidate: Candidate) -> int:
        self.candidates.append(candidate)
        self._ids_by_title[candidate.title] = len(self.candidates)
        return len(self.candidates)

    def _blocker(self, result: dict) -> str:
        if result["title"] in self._unavailable:
            return self._unavailable[result["title"]]
        if not result.get("image_url"):
            return "it has no photo"
        return ""


def assign(request: str, client, newsdata, unwritten: list, unavailable: dict,
           max_picks: int, today: datetime.date) -> Assignment:
    """Search for stories about the request and pick up to max_picks.

    unwritten: Stories items filed recently that no one has written up.
    unavailable: titles the paper has filed but can't hand over, each with
    the reason, worded to finish "Can't run: ...". """
    desk = Desk(unwritten, unavailable)
    messages = [{"role": "user", "content": brief(request, desk, max_picks, today)}]
    searches = 0
    for _ in range(MAX_TURNS):
        response = client.beta.messages.create(
            model=EDITOR_MODEL,
            max_tokens=16000,
            output_config={"effort": EDITOR_EFFORT},
            betas=[FALLBACK_BETA],
            fallbacks="default",
            system=SYSTEM_PROMPT,
            tools=[SEARCH_TOOL],
            messages=messages,
        )
        if response.stop_reason == "refusal":
            raise EditorError("the editor wouldn't take that one")
        messages.append({"role": "assistant", "content": response.content})

        tool_uses = [block for block in response.content if block.type == "tool_use"]
        if not tool_uses:
            return parse_assignment(extract_text(response), desk, max_picks)

        results = []
        for tool_use in tool_uses:
            searches += 1
            results.append(_run_search(tool_use, desk, newsdata, searches))
        messages.append({"role": "user", "content": results})
    raise EditorError("the editor kept searching and never made a call")


def brief(request: str, desk: Desk, max_picks: int, today: datetime.date) -> str:
    if desk.candidates:
        listing = "\n\n".join(desk.describe(i) for i in range(1, len(desk.candidates) + 1))
        filed = f"Filed in the last two days but never written up:\n\n{listing}"
    else:
        filed = "Nothing filed in the last two days is waiting to be written up."
    return (f"Request: {request}\n\nToday is {today:%A, %B} {today.day}, {today.year}.\n\n"
            f"{filed}\n\nPick up to {max_picks}.")


def parse_assignment(response_text: str, desk: Desk, max_picks: int) -> Assignment:
    """Map the editor's ids back to candidates. Skips ids that name nothing,
    repeat an earlier one, or name a story that can't run."""
    answer = parse_json_response(response_text)
    if not isinstance(answer, dict):
        answer = {}

    picks, picked_ids = [], set()
    for choice in answer.get("picks") or []:
        candidate_id = choice.get("id") if isinstance(choice, dict) else None
        candidate = desk.get(candidate_id)
        if len(picks) >= max_picks or not candidate or candidate.blocker or candidate_id in picked_ids:
            continue
        picked_ids.add(candidate_id)
        picks.append(Pick(candidate=candidate, note=str(choice.get("why") or "").strip()))

    close_calls = []
    for candidate_id in answer.get("close_calls") or []:
        candidate = desk.get(candidate_id)
        if len(close_calls) >= MAX_CLOSE_CALLS or not candidate or candidate_id in picked_ids:
            continue
        picked_ids.add(candidate_id)
        close_calls.append(candidate)
    return Assignment(picks=picks, close_calls=close_calls)


def _run_search(tool_use, desk: Desk, newsdata, search_number: int) -> dict:
    if search_number > MAX_SEARCHES:
        return _tool_result(tool_use, "Out of searches. Make your picks from what you have.", is_error=True)
    query, title_only = tool_use.input.get("query", ""), bool(tool_use.input.get("title_only"))
    print(f"Scoop search {search_number}: {json.dumps(query)} (title_only={title_only})")
    try:
        response = newsdata.fetch_by_query(query, title_only=title_only)
    except Exception as e:
        return _tool_result(tool_use, f"Search failed: {e}", is_error=True)

    ids = [desk.add_result(result) for result in response.get("results") or [] if result.get("title")]
    for candidate_id in ids:
        print(f"  [{candidate_id}] {desk.get(candidate_id).title}")
    listing = "\n\n".join(desk.describe(i) for i in ids)
    return _tool_result(tool_use, listing or "No results.")


def _tool_result(tool_use, text: str, is_error: bool = False) -> dict:
    result = {"type": "tool_result", "tool_use_id": tool_use.id, "content": text}
    if is_error:
        result["is_error"] = True
    return result

