"""Parse the JSON a model was asked to return, tolerating prose and code fences around it."""

import json

_CLOSING_BRACKETS = {"[": "]", "{": "}"}


def parse_json_response(response_text: str) -> list:
    """Parse JSON from a model response, handling markdown code blocks.
    The JSON runs from the first bracket to the last matching one, so an
    object holding arrays parses as the object."""
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        try:
            starts = [i for i in (response_text.find("["), response_text.find("{")) if i != -1]
            if not starts:
                raise ValueError("No valid JSON found in response")
            json_start = min(starts)
            json_end = response_text.rfind(_CLOSING_BRACKETS[response_text[json_start]]) + 1
            if json_end == 0:
                raise ValueError("No valid JSON found in response")
            return json.loads(response_text[json_start:json_end])
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing response: {e}")
            print(f"Response text: {response_text}")
            return []
