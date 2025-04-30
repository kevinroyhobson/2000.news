import os
import string
import random
import json
import boto3
from decimal import Decimal
from dynamodb_json import json_util as dynamodb_json
from google import genai
from google.genai import types
from dotenv import load_dotenv

_dynamo_resource = boto3.resource("dynamodb")
load_dotenv()


def subvert(event, context):

    for record in event["Records"]:

        if record["eventName"] != "INSERT" and record["eventName"] != "MODIFY":
            print(
                f"Skipped record {record['eventID']} because it's not an INSERT or MODIFY event."
            )
            continue

        story = dynamodb_json.loads(record["dynamodb"]["NewImage"])
        print(f"story: {story}")
        if "SubvertedTitles" in story and story["SubvertedTitles"] is not None:
            print(f"Skipped {story['Title']} because it has already been subverted.")
            continue

        story["SubvertedTitles"] = compute_subverted_titles(story["Title"], story.get("Description", ""))
        update_story(story)


def compute_subverted_titles(title: str, subtitle: str):

    system_instruction = """
    You are a copywriter who writes short and zany headlines in a pithy, funny, satirical style. Your headlines end up 
    in the SimCIty 2000 newspaper -- like the old Llama Drama headlines of yore.
    """

    prompt = f"""
    Create one or more alternative, more creative headlines for the following headline: "{title}". some other context: "{subtitle}".

    The best headlines are headlines that rhyme, or headlines that have a good pun, or headlines with a lot of alliteration 
    or assonance, some funny format like a haiku, or just something kinda sarcastic. But also if there are things you think 
    are really clever or funny then don't be afraid to use that too. If the headline is only a single phrase or sentence, it 
    shouldn't end with a period.

    Only choose headlines that you think are really clever and funny -- don't send me any garbage because I'm going to 
    randomly choose the responses to show in the paper. You should also feel free to alter the headlines in some crazy ways 
    too, even if it changes the meaning. But where the headline came from should still make sense. Use some of the following 
    words/people/concepts if you want to alter things: "{', '.join(get_random_words(10))}".
    
    Return your answer as a JSON array of headlines. For each headline, include the rewritten headline content itself, a 
    score from 0.0 to 1.0 of how funny you think this particular headline is, and an explanation of why it's funny. 
    [{{"headline":"example headline", "funny_score": 0.75, "explanation": "why it's funny"}}]
    """

    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

    response = client.models.generate_content(
        model=os.getenv("GEMINI_MODEL"),
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            response_modalities=["TEXT"],
            temperature=2.0,
            thinking_config=types.ThinkingConfig(thinking_budget=1024)
        ),
    )

    response_text = response.candidates[0].content.parts[0].text.strip()    
    subverted_titles = parse_subverted_titles_json_response(response_text)
    subverted_titles = [format_subverted_title(title, prompt, response) for title in subverted_titles]

    print(f"Parsed subverted titles: {subverted_titles}")
    return subverted_titles


def parse_subverted_titles_json_response(response_text: str) -> list:
    try:
        # First try parsing the entire response as JSON
        return json.loads(response_text)
    except json.JSONDecodeError:
        try:
            # If that fails, try to find JSON array/object in the text
            json_start = response_text.find('[')
            if json_start == -1:
                json_start = response_text.find('{')
            json_end = response_text.rfind(']') + 1
            if json_end == 0:
                json_end = response_text.rfind('}') + 1
            
            if json_start != -1 and json_end != 0:
                json_str = response_text[json_start:json_end]
                return json.loads(json_str)
            else:
                raise ValueError("No valid JSON found in response")
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing response: {e}")
            print(f"Response text: {response_text}")
            raise


def format_subverted_title(subverted_title, prompt, response):
    print(f"Subverted headline: {subverted_title['headline']}")
    
    return {
        "SubvertedTitle": subverted_title['headline'],
        "FunnyScore": Decimal(str(subverted_title['funny_score'])),
        "Explanation": subverted_title['explanation'],
        "Prompt": prompt,
        "ModelVersion": response.model_version,
        "UsageMetadata": {
            "prompt_token_count": response.usage_metadata.prompt_token_count,
            "candidates_token_count": response.usage_metadata.candidates_token_count,
            "total_token_count": response.usage_metadata.total_token_count
        },
        "SubvertedTitleId": "".join(
            random.choices(string.ascii_lowercase + string.digits, k=5)
        ),
    }


def get_random_words(num_words: int):

    words_table = _dynamo_resource.Table("Words")
    response = words_table.scan()
    words = response["Items"]
    return random.sample([word["Word"] for word in words], num_words)


def update_story(story):
    stories_table = _dynamo_resource.Table("Stories")
    stories_table.update_item(
        Key={"YearMonthDay": story["YearMonthDay"], "Title": story["Title"]},
        UpdateExpression="set SubvertedTitles = :s",
        ExpressionAttributeValues={":s": story["SubvertedTitles"]},
    )


if __name__ == "__main__":
    compute_subverted_titles("test headline", "test subtitle")
