"""Subvert a handful of stories right now, without the batch pipeline.

The scheduled path runs through Anthropic's Batch API because nobody is
waiting on it. A story sent in from the Telegram channel has someone waiting
for a reply, so its brainstorm and generate calls run as parallel
messages.create calls instead: about a minute end to end at standard pricing
on a few calls. The prompts, parsing and writes are the pipeline's own.

Headlines are written with Deferred=True so the tournament stream trigger
leaves them for the next scheduled run instead of starting one of its own.
"""

from lib.anthropic_batches import run_synchronously
from Subvert.pipeline import (
    build_brainstorm_request,
    build_generate_requests,
    angles_from_brainstorm,
    get_anthropic_client,
    get_random_words,
    save_generated_headlines,
)

DEFERRED_HEADLINE = {"Deferred": True}


def subvert_synchronously(stories: list) -> list:
    """stories: pipeline-shaped dicts (see Subvert.subvert.pipeline_story).
    Returns the SubvertedHeadlines items written."""
    client = get_anthropic_client()
    stories = [{**story, "random_words": get_random_words(8)} for story in stories]

    brainstorm_requests = [build_brainstorm_request(s, i) for i, s in enumerate(stories)]
    brainstormed = run_synchronously(client, brainstorm_requests)
    angles_per_story = [
        angles_from_brainstorm(story, brainstorm_requests[i], brainstormed.get(f"story-{i}", {}))
        for i, story in enumerate(stories)
    ]

    generate_requests = build_generate_requests(stories, angles_per_story)
    generated = run_synchronously(client, generate_requests)
    return save_generated_headlines(stories, angles_per_story, generate_requests, generated,
                                    extra_attributes=DEFERRED_HEADLINE)
