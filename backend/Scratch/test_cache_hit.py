"""
Verify Opus 4.7 prompt caching works against the actual Tournament prompt.

Reads the materialized exemplar cache from DDB, constructs the same system
prompt the Tournament Lambda would build at module load, and makes two
Opus calls with cache_control. Reports cache_creation_input_tokens on
call 1 (should be ~the full prompt size) and cache_read_input_tokens on
call 2 (should match — confirming cache hit).
"""

import os
import sys
import time

import anthropic
import boto3

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from lib.ssm_secrets import get_secret  # noqa: E402

# Pull the static system prompt from tournament.py
import re
tourn_src = open(os.path.join(os.path.dirname(__file__), '..', 'Tournament', 'tournament.py')).read()
m = re.search(r'TOURNAMENT_SYSTEM_PROMPT = """(.*?)"""', tourn_src, re.DOTALL)
STATIC_PROMPT = m.group(1)

# Pull cached exemplars from DDB (same logic Tournament uses at module load)
dynamo = boto3.resource('dynamodb', region_name='us-east-2')
t = dynamo.Table('SubvertedHeadlines')
resp = t.get_item(Key={'YearMonthDay': 'META', 'HeadlineId': 'outstanding_exemplars'})
item = resp.get('Item') or {}
headlines = item.get('Headlines') or []
print(f"Loaded {len(headlines)} cached exemplars from META.")

# Build the appended exemplar block
if headlines:
    lines = ['', 'ADDITIONAL EXEMPLARS (recent headlines marked outstanding by the editor):', '']
    for h in headlines:
        lines.append(f'- "{h.get("Headline", "")}"')
        if h.get('Rationale'):
            lines.append(f'  Why it works: {h["Rationale"]}')
    appendix = '\n'.join(lines)
else:
    appendix = ''

SYSTEM_PROMPT = STATIC_PROMPT + appendix
print(f"System prompt: {len(SYSTEM_PROMPT)} chars (~{len(SYSTEM_PROMPT)//4} tokens)")
print()

# Build a tiny user prompt — minimal ranking task to keep output cost negligible
user_prompt = """Rank these satirical headlines from best to worst.
Reply ONLY with the letters separated by commas (e.g. "B, A"). No preamble.

A: "Local Man Discovers His Reflection Has Been Lying To Him For Years"
B: "Pentagon Unveils $4 Trillion Anti-Stupidity Missile, Targeting Itself"
"""

api_key = os.getenv('CURATION_ANTHROPIC_API_KEY') or get_secret('ANTHROPIC_API_KEY')
client = anthropic.Anthropic(api_key=api_key)


def call(label: str):
    r = client.messages.create(
        model='claude-opus-4-7',
        max_tokens=50,
        system=[{
            'type': 'text',
            'text': SYSTEM_PROMPT,
            'cache_control': {'type': 'ephemeral'},
        }],
        messages=[{'role': 'user', 'content': user_prompt}],
    )
    print(f"== {label} ==")
    print(f"  input_tokens:               {r.usage.input_tokens}")
    print(f"  cache_creation_input_tokens: {getattr(r.usage, 'cache_creation_input_tokens', 'n/a')}")
    print(f"  cache_read_input_tokens:     {getattr(r.usage, 'cache_read_input_tokens', 'n/a')}")
    print(f"  output_tokens:              {r.usage.output_tokens}")
    print()


call('Call 1 (expect cache_creation > 0, cache_read = 0)')
time.sleep(2)
call('Call 2 (expect cache_creation = 0, cache_read ≈ Call 1 creation)')
