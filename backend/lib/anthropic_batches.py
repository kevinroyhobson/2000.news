"""
Helpers for running LLM calls through Anthropic's Message Batches API.

No LLM interaction in this codebase sits on a live serving path, so all calls
go through batches (50% of standard token prices), orchestrated by Step
Functions. The pattern is always:

    1. A pipeline Lambda builds requests and calls submit_batch()
    2. The state machine polls check_batch_state() until the batch has ended,
       or gives up after MAX_POLLS and marks the stage timed_out
    3. The next pipeline Lambda calls resolve_batch(), which returns results
       keyed by custom_id and falls back to synchronous messages.create()
       calls for anything the batch didn't successfully complete — individual
       errored requests, or the whole batch on timeout

Because of step 3, callers must be able to rebuild the request list they
submitted. Requests are rebuilt rather than carried through Step Functions
state to stay well under the 256KB execution-state limit (the cached system
prompts alone would blow it).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def submit_batch(client, requests) -> dict:
    """Submit requests and return a fresh batch-tracking dict for SFN state.

    requests: [{"custom_id": str, "params": {...messages.create kwargs...}}]
    """
    batch = client.messages.batches.create(requests=requests)
    print(f"[batches] Submitted {batch.id} ({len(requests)} requests)")
    return {
        "batch_id": batch.id,
        "status": batch.processing_status,
        "polls": 0,
        "timed_out": False,
    }


def check_batch_state(client, state) -> dict:
    """The check_batch pipeline action: refresh status, bump the poll counter."""
    batch = dict(state["batch"])
    remote = client.messages.batches.retrieve(batch["batch_id"])
    counts = remote.request_counts
    batch["status"] = remote.processing_status
    batch["polls"] = int(batch.get("polls", 0)) + 1
    print(
        f"[batches] {batch['batch_id']} poll #{batch['polls']}: {batch['status']} "
        f"(processing={counts.processing}, succeeded={counts.succeeded}, "
        f"errored={counts.errored})"
    )
    return {**state, "batch": batch}


def _cancel_and_drain(client, batch_id, timeout_seconds=180) -> bool:
    """Cancel a batch and wait for it to settle so partial results are readable."""
    try:
        client.messages.batches.cancel(batch_id)
    except Exception as e:
        print(f"[batches] Cancel of {batch_id} failed, continuing: {e}")
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if client.messages.batches.retrieve(batch_id).processing_status == "ended":
            return True
        time.sleep(5)
    print(f"[batches] {batch_id} did not settle after cancel")
    return False


def extract_text(message) -> str:
    """First text block of a Message. With thinking enabled, content[0] can be
    a (possibly empty) thinking block, so filter by type instead of indexing."""
    for block in message.content:
        if getattr(block, "type", None) == "text":
            return block.text
    raise RuntimeError(f"No text block in response (stop_reason={message.stop_reason})")


def _usage_dict(usage) -> dict:
    d = {"input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens}
    for field in ("cache_creation_input_tokens", "cache_read_input_tokens"):
        value = getattr(usage, field, None)
        if value is not None:
            d[field] = value
    return d


def resolve_batch(client, batch, requests, sync_max_workers=8) -> dict:
    """
    Collect a result for every request of a submitted batch.

    batch:    the batch-tracking dict from SFN state ({"batch_id", "timed_out", ...})
    requests: the same request list that was submitted (rebuilt by the caller)

    Returns {custom_id: {"text": str, "usage": dict, "via": "batch"|"sync"}}.
    Results stream back in arbitrary order, so everything is keyed by
    custom_id. Requests whose batch result is missing, errored, canceled, or
    expired are retried synchronously at standard pricing; if the sync call
    also fails, the entry is {"error": str} and the caller decides how to
    degrade.
    """
    batch_id = batch["batch_id"]
    if batch.get("timed_out"):
        print(f"[batches] {batch_id} timed out — canceling, then sync fallback")
        _cancel_and_drain(client, batch_id)

    resolved = {}
    try:
        for result in client.messages.batches.results(batch_id):
            if result.result.type == "succeeded":
                message = result.result.message
                try:
                    resolved[result.custom_id] = {
                        "text": extract_text(message),
                        "usage": _usage_dict(message.usage),
                        "via": "batch",
                    }
                except RuntimeError as e:
                    print(f"[batches] {result.custom_id}: {e}")
            else:
                print(f"[batches] {result.custom_id}: {result.result.type}")
    except Exception as e:
        # Results unreadable (e.g. cancel never settled) — sync everything.
        print(f"[batches] Could not read results for {batch_id}: {e}")

    stragglers = [r for r in requests if r["custom_id"] not in resolved]
    if stragglers:
        print(f"[batches] Sync fallback for {len(stragglers)}/{len(requests)} requests")
        with ThreadPoolExecutor(max_workers=min(sync_max_workers, len(stragglers))) as pool:
            futures = {
                pool.submit(client.messages.create, **r["params"]): r["custom_id"]
                for r in stragglers
            }
            for future in as_completed(futures):
                custom_id = futures[future]
                try:
                    message = future.result()
                    resolved[custom_id] = {
                        "text": extract_text(message),
                        "usage": _usage_dict(message.usage),
                        "via": "sync",
                    }
                except Exception as e:
                    print(f"[batches] Sync fallback failed for {custom_id}: {e}")
                    resolved[custom_id] = {"error": str(e)}

    return resolved
