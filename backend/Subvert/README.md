# Subvert

Takes real news headlines and rewrites them as funny SimCity 2000-style newspaper headlines.

## Architecture

All LLM calls run through Anthropic's Message Batches API (50% of standard
token pricing), orchestrated by the `SubvertPipeline` Step Functions state
machine:

```
Stories stream -> subvert.py (starter) -> SubvertPipeline state machine
                                             |
                     submit_brainstorm  (one batch: 1 request/story)
                          poll ~2min intervals until the batch ends
                     submit_generate    (one batch: 1 request/angle, ~5/story)
                          poll
                     save_headlines     -> SubvertedHeadlines table
```

- `subvert.py` — thin Stories-stream trigger. Dedupes stories that already
  have headlines and starts one execution per stream batch. Never calls a model.
- `pipeline.py` — the state machine's task handler (dispatch on `action`).

The two LLM stages:

1. **Brainstorm** - Analyzes the headline and generates 4-5 comedic angles (puns, rhymes, pop culture refs, absurdist twists). Also pulls random words from the Words table for inspiration.

2. **Generate** - For each angle, generates 3-4 polished headlines. Aims for SimCity 2000 newspaper vibe: zany, pithy, satirical.

Cross-story ranking happens later in the separate Tournament pipeline (same
batch + state machine pattern), triggered off the SubvertedHeadlines stream.

Batches usually finish well under an hour; if one is still running after ~3
hours the state machine cancels it and the next stage re-runs the missing
requests as synchronous API calls, so latency is bounded. Headline IDs are
deterministic per (story, angle, position), which makes every pipeline step
safe to retry — replays overwrite instead of duplicating.

## Configuration

Each stage can use a different Anthropic model. Set via environment variables
(on `SubvertPipelineFunction` in template.yaml, or `.env` locally):

```
ANTHROPIC_API_KEY=sk-ant-...

BRAINSTORM_MODEL=claude-opus-4-8
GENERATE_MODEL=claude-haiku-4-5-20251001
```

### Model options

- `claude-haiku-4-5` - Fast, cheap, good at wordplay
- `claude-sonnet-5` - Smarter, more expensive
- `claude-opus-4-8` - Best, most expensive

## Local testing

```bash
cd backend
python -m Subvert.pipeline
```

Drives the full batch flow (submit → poll → save) inline for one test story,
writing to day key `TEST`. Make sure your `.env` has `ANTHROPIC_API_KEY` and
the Langfuse keys, and that your shell has AWS credentials for DynamoDB.
Expect a few minutes of batch-poll waiting.
