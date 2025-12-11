# Subvert

Takes real news headlines and rewrites them as funny SimCity 2000-style newspaper headlines.

## Pipeline

Three-stage agentic pipeline:

1. **Brainstorm** - Analyzes the headline and generates 4-5 comedic angles (puns, rhymes, pop culture refs, absurdist twists). Also pulls random words from the Words table for inspiration.

2. **Generate** - For each angle, generates 2-3 polished headlines. Aims for SimCity 2000 newspaper vibe: zany, pithy, satirical.

3. **Tournament** - Pairwise comparisons ("which is funnier, A or B?") to select the best 4 headlines. More reliable than self-scoring.

## Configuration

Each stage can use a different model. Set via environment variables:

```
# API keys
ANTHROPIC_API_KEY=sk-ant-...
GEMINI_API_KEY=...

# Stage 1: Brainstorm
BRAINSTORM_PROVIDER=anthropic
BRAINSTORM_MODEL=claude-haiku-4-5

# Stage 2: Generate headlines
GENERATE_PROVIDER=anthropic
GENERATE_MODEL=claude-haiku-4-5

# Stage 3: Tournament selection
TOURNAMENT_PROVIDER=google
TOURNAMENT_MODEL=gemini-2.5-flash-lite
```

### Providers

- `anthropic` - Claude models
- `google` - Gemini models

### Model options

**Anthropic:**
- `claude-haiku-4-5` - Fast, cheap, good at wordplay
- `claude-sonnet-4-5` - Smarter, more expensive
- `claude-opus-4-5` - Best, most expensive

**Google:**
- `gemini-2.5-flash-lite` - Cheapest
- `gemini-2.5-flash` - Better quality

## Why cross-model judging?

Models are bad at rating their own jokes. Having Gemini judge Claude's headlines (or vice versa) removes self-rating bias and produces better selections.

## Local testing

```bash
cd backend/Subvert
python subvert.py
```

Runs with a test headline. Make sure your `.env` has the API keys and stage config.
