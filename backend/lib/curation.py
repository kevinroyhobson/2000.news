"""Curation core: grade writes, rationale generation, exemplar cache.

Shared by the interactive CLI (Scratch/curate_headlines.py) and the Telegram
reaction grader (TelegramReaction/grade.py) so a star tapped in the channel
does exactly what pressing `o` at the prompt does.
"""

import datetime
import os
import re
from zoneinfo import ZoneInfo

import anthropic
from boto3.dynamodb.conditions import Attr

from lib.ssm_secrets import get_secret


REGION = 'us-east-2'
TABLE_NAME = 'SubvertedHeadlines'

GRADES = ('outstanding', 'solid', 'meh', 'bad')

RATIONALE_MODEL = 'claude-opus-4-8'
RATIONALE_FALLBACK_MODEL = 'claude-sonnet-5'

# A single synthetic item in SubvertedHeadlines holds the materialized top-20
# "outstanding" exemplars for the Tournament prompt. Keeps Tournament's load
# path to a single GetItem (no scan, no time window) so curation can be sparse.
EXEMPLAR_CACHE_KEY = {'YearMonthDay': 'META', 'HeadlineId': 'outstanding_exemplars'}
# Target total system-prompt tokens after exemplars are appended. Picked to
# clear Opus 4.7's 4,096-token cache threshold with ~20% buffer, while
# staying small enough that the judge isn't drowning in pattern-match anchors.
EXEMPLAR_TOKEN_TARGET = int(os.getenv('EXEMPLAR_TOKEN_TARGET', '5000'))
# Hard upper bound on exemplars cached, regardless of token budget. Floor on
# overfit risk if the rationales are unusually short.
EXEMPLAR_HARD_CAP = 50
# The judge's system prompt lives in the Step Functions task handler.
TOURNAMENT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'Tournament', 'pipeline.py'
)

RATIONALE_SYSTEM = """You explain why satirical news headlines work, in the style of a comedy editor's brief annotation.

Given a satirical headline (and the original news headline it riffs on), write ONE sentence (<=35 words) explaining why the headline works. Focus on the comic device — wordplay, format-borrowing, literal-reading absurdism, deadpan-institutional framing, surprise misdirection — and what specifically makes it land.

Examples of the style:
- "Hit on" means both flirtation and literally being punched. The advice column framing sells the misdirection — you read it one way, then the other meaning clicks.
- Format-borrowing: missing-persons flyer + sports box score in the same breath. The deadpan-realism details sell the format; the pivot ("defeat search party") is the punchline.

Reply with ONLY the explanation. No preamble, no quotes around it."""


_anthropic_client = None


def get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        api_key = os.getenv('CURATION_ANTHROPIC_API_KEY')
        source = 'env CURATION_ANTHROPIC_API_KEY'
        if not api_key:
            api_key = get_secret('ANTHROPIC_API_KEY')
            source = 'SSM /2000news/ANTHROPIC_API_KEY (production key)'
        print(f"[curate] using API key from: {source}")
        _anthropic_client = anthropic.Anthropic(api_key=api_key)
    return _anthropic_client


class RefusalError(RuntimeError):
    """Raised when the model returns stop_reason=refusal."""


def _now_iso() -> str:
    return datetime.datetime.now(ZoneInfo('UTC')).isoformat()


def _gen_rationale_once(headline: str, original: str, model: str) -> str:
    client = get_anthropic_client()
    msg = client.messages.create(
        model=model,
        # Adaptive thinking on both the Opus 4.8 primary and Sonnet 5 fallback.
        # The rationale is one sentence, but thinking tokens count against
        # max_tokens, so this is bumped well above the answer size to leave room
        # for the think. The text-block filter below already skips the thinking
        # block.
        max_tokens=4096,
        thinking={'type': 'adaptive'},
        system=RATIONALE_SYSTEM,
        messages=[{
            'role': 'user',
            'content': f'SATIRICAL: "{headline}"\nORIGINAL: "{original}"',
        }],
    )
    text_blocks = [b for b in msg.content if getattr(b, 'type', None) == 'text']
    if text_blocks:
        return text_blocks[0].text.strip()
    if msg.stop_reason == 'refusal':
        raise RefusalError(f"{model} refused (likely safety system flagging the topic)")
    block_types = [getattr(b, 'type', '?') for b in msg.content]
    raise RuntimeError(
        f"No text block in response (stop_reason={msg.stop_reason}, "
        f"blocks={block_types}, len={len(msg.content)})"
    )


def gen_rationale(headline: str, original: str, on_refusal=None) -> str:
    """Generate a rationale, falling back to Sonnet if Opus refuses."""
    try:
        return _gen_rationale_once(headline, original, RATIONALE_MODEL)
    except RefusalError as e:
        if on_refusal:
            on_refusal(e)
        return _gen_rationale_once(headline, original, RATIONALE_FALLBACK_MODEL)


def apply_grade(table, year_month_day: str, headline_id: str, grade: str,
                rationale: str = '', source: str = '') -> None:
    """Write a grade (plus optional rationale) onto a headline."""
    if grade not in GRADES:
        raise ValueError(f"unknown grade {grade!r}")
    update = 'SET #g = :g, GradedAt = :ts'
    values = {':g': grade, ':ts': _now_iso()}
    if rationale:
        update += ', Rationale = :r'
        values[':r'] = rationale
    if source:
        update += ', GradeSource = :src'
        values[':src'] = source
    table.update_item(
        Key={'YearMonthDay': year_month_day, 'HeadlineId': headline_id},
        UpdateExpression=update,
        ExpressionAttributeNames={'#g': 'Grade'},
        ExpressionAttributeValues=values,
    )


def clear_grade(table, year_month_day: str, headline_id: str, only_if_source: str = '') -> bool:
    """Remove a grade. With only_if_source set, leaves grades from other
    sources (e.g. the CLI) untouched and returns False."""
    kwargs = {
        'Key': {'YearMonthDay': year_month_day, 'HeadlineId': headline_id},
        'UpdateExpression': 'REMOVE #g, GradedAt, Rationale, GradeSource',
        'ExpressionAttributeNames': {'#g': 'Grade'},
    }
    if only_if_source:
        kwargs['ConditionExpression'] = Attr('GradeSource').eq(only_if_source)
    try:
        table.update_item(**kwargs)
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return False
    return True


def _read_static_system_prompt() -> str:
    """Pull the TOURNAMENT_SYSTEM_PROMPT base text from the judge's source."""
    src = open(TOURNAMENT_PATH).read()
    m = re.search(r'TOURNAMENT_SYSTEM_PROMPT = """(.*?)"""', src, re.DOTALL)
    if not m:
        raise RuntimeError(f"Could not locate TOURNAMENT_SYSTEM_PROMPT in {TOURNAMENT_PATH}")
    return m.group(1)


def _build_appendix(headlines: list) -> str:
    if not headlines:
        return ''
    lines = ['', 'ADDITIONAL EXEMPLARS (recent headlines marked outstanding by the editor):', '']
    for h in headlines:
        lines.append(f'- "{h.get("Headline", "")}"')
        if h.get('Rationale'):
            lines.append(f'  Why it works: {h["Rationale"]}')
    return '\n'.join(lines)


def _count_prompt_tokens(client, system_text: str) -> int:
    """Count tokens for a system prompt as Tournament would send it."""
    r = client.messages.count_tokens(
        model='claude-opus-4-8',
        system=[{'type': 'text', 'text': system_text}],
        messages=[{'role': 'user', 'content': 'placeholder'}],
    )
    return r.input_tokens


def rebuild_exemplar_cache(table) -> int:
    """
    Scan SubvertedHeadlines for Grade='outstanding' items, then bin-pack as many
    as fit under the EXEMPLAR_TOKEN_TARGET budget (newest first by GradedAt).
    Writes them as a materialized item Tournament reads at module load.
    Returns count of exemplars written.
    """
    items = []
    kwargs = {
        'FilterExpression': Attr('Grade').eq('outstanding'),
        'ProjectionExpression': '#h, OriginalHeadline, Rationale, GradedAt, #r, CrossDayRank',
        'ExpressionAttributeNames': {'#h': 'Headline', '#r': 'Rank'},
    }
    resp = table.scan(**kwargs)
    items.extend(resp.get('Items', []))
    while 'LastEvaluatedKey' in resp:
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
        resp = table.scan(**kwargs)
        items.extend(resp.get('Items', []))

    items.sort(key=lambda x: x.get('GradedAt', ''), reverse=True)
    candidates = items[:EXEMPLAR_HARD_CAP]

    # Estimate via Anthropic's count_tokens API: compute base size, then how many
    # exemplars we can add before exceeding target.
    static_prompt = _read_static_system_prompt()
    client = get_anthropic_client()
    base_tokens = _count_prompt_tokens(client, static_prompt)

    if base_tokens >= EXEMPLAR_TOKEN_TARGET:
        # Static prompt alone already over budget; cache nothing.
        chosen = []
    elif not candidates:
        chosen = []
    else:
        # One measurement with all candidates → derive avg per exemplar, then
        # pick a count and verify.
        full_appendix = _build_appendix([
            {'Headline': c.get('Headline', ''), 'Rationale': c.get('Rationale', '')}
            for c in candidates
        ])
        full_tokens = _count_prompt_tokens(client, static_prompt + full_appendix)
        added = max(1, full_tokens - base_tokens)
        avg_per = added / len(candidates)
        budget_for_exemplars = EXEMPLAR_TOKEN_TARGET - base_tokens
        target_n = max(1, min(len(candidates), int(budget_for_exemplars / avg_per)))

        # Verify with one more count_tokens; nudge ±1 if we're off
        chosen = candidates[:target_n]
        verify_tokens = _count_prompt_tokens(client, static_prompt + _build_appendix([
            {'Headline': c.get('Headline', ''), 'Rationale': c.get('Rationale', '')}
            for c in chosen
        ]))
        # Trim if over, expand if under and have headroom
        while verify_tokens > EXEMPLAR_TOKEN_TARGET and len(chosen) > 1:
            chosen = chosen[:-1]
            verify_tokens = _count_prompt_tokens(client, static_prompt + _build_appendix([
                {'Headline': c.get('Headline', ''), 'Rationale': c.get('Rationale', '')}
                for c in chosen
            ]))
        while (
            len(chosen) < len(candidates)
            and (verify_tokens + avg_per) <= EXEMPLAR_TOKEN_TARGET
        ):
            chosen = candidates[:len(chosen) + 1]
            verify_tokens = _count_prompt_tokens(client, static_prompt + _build_appendix([
                {'Headline': c.get('Headline', ''), 'Rationale': c.get('Rationale', '')}
                for c in chosen
            ]))
            if verify_tokens > EXEMPLAR_TOKEN_TARGET:
                chosen = chosen[:-1]
                break

    headlines = [
        {
            'Headline': c.get('Headline', ''),
            'OriginalHeadline': c.get('OriginalHeadline', ''),
            'Rationale': c.get('Rationale', ''),
            'GradedAt': c.get('GradedAt', ''),
        }
        for c in chosen
    ]
    final_tokens = base_tokens
    if headlines:
        final_tokens = _count_prompt_tokens(
            client, static_prompt + _build_appendix(headlines)
        )

    table.put_item(Item={
        **EXEMPLAR_CACHE_KEY,
        'Headlines': headlines,
        'UpdatedAt': _now_iso(),
        'TotalOutstanding': len(items),
        'PromptTokens': final_tokens,
        'TokenTarget': EXEMPLAR_TOKEN_TARGET,
    })
    print(f"  cache: {len(headlines)} exemplars / {len(items)} total outstanding "
          f"→ {final_tokens} tokens (target {EXEMPLAR_TOKEN_TARGET})")
    return len(headlines)
