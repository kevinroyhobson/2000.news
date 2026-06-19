# Subvert

Takes real news headlines and rewrites them as funny SimCity 2000-style newspaper headlines.

## Pipeline

Two-stage agentic pipeline:

1. **Brainstorm** - Analyzes the headline and generates 4-5 comedic angles (puns, rhymes, pop culture refs, absurdist twists). Also pulls random words from the Words table for inspiration.

2. **Generate** - For each angle, generates 3-4 polished headlines. Aims for SimCity 2000 newspaper vibe: zany, pithy, satirical.

Cross-story ranking happens later in the separate Tournament lambda, which is triggered off the SubvertedHeadlines stream.

## Configuration

Each stage can use a different model. Set via environment variables:

```
# API keys
ANTHROPIC_API_KEY=sk-ant-...
GEMINI_API_KEY=...

# Stage 1: Brainstorm
BRAINSTORM_PROVIDER=anthropic
BRAINSTORM_MODEL=claude-opus-4-8

# Stage 2: Generate headlines
GENERATE_PROVIDER=anthropic
GENERATE_MODEL=claude-haiku-4-5-20251001
```

### Providers

- `anthropic` - Claude models
- `google` - Gemini models

### Model options

**Anthropic:**
- `claude-haiku-4-5` - Fast, cheap, good at wordplay
- `claude-sonnet-4-6` - Smarter, more expensive
- `claude-opus-4-8` - Best, most expensive

**Google:**
- `gemini-2.5-flash-lite` - Cheapest
- `gemini-2.5-flash` - Better quality

## Local testing

```bash
cd backend/Subvert
python subvert.py
```

Runs with a test headline. Make sure your `.env` has the API keys and stage config.
