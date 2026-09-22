"""Parse the JSON a model was asked to return, tolerating prose and code fences around it."""

import json


def parse_json_response(response_text: str) -> list:
    """Parse JSON from a model response, handling markdown code blocks."""
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        try:
            json_start = response_text.find("[")
            if json_start == -1:
                json_start = response_text.find("{")
            json_end = response_text.rfind("]") + 1
            if json_end == 0:
                json_end = response_text.rfind("}") + 1

            if json_start != -1 and json_end != 0:
                json_str = response_text[json_start:json_end]
                return json.loads(json_str)
            else:
                raise ValueError("No valid JSON found in response")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing response: {e}")
            print(f"Response text: {response_text}")
            return []
