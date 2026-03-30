import json
import os
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

load_dotenv()

# =========================
# Config
# =========================
DEFAULT_INPUT_JSON = "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/validation_poc/questions_with_answers.json"
DEFAULT_OUTPUT_TXT = "/home/ankit/smartsense_code/fraudx_timeline_poc/validation_poc/patient_profile.txt"
DEFAULT_MODEL = "gpt-4.1-mini"


# =========================
# Prompt Builder
# =========================
def build_prompt(questions_json: dict) -> str:
    return f"""You are a medical-legal case analyst for an insurance company.

You will receive a JSON containing questions and their answers extracted from
medical, legal, and attorney documents related to an insurance claim.

Your task is to convert this Q&A JSON into a clean, structured patient profile
in plain text format.

## RULES:
- Use ONLY the information present in the JSON answers
- If an answer is "NOT FOUND", keep it as "NOT FOUND" in the profile
- Do not infer, assume, or add any information not present in the answers
- Keep the profile clean, readable, and consistent
- Group related information under the correct section
- For injuries, break them down by body part clearly
- Return only the profile text, no preamble or explanation

OUTPUT FORMAT TO FOLLOW EXACTLY:
## PATIENT PROFILE
===============

## IDENTITY
Patient Name: <value>
Alternate Names: <value>
Date of Birth: <value>
Gender: <value>
Known Addresses: <value>

## INCIDENT
Incident Date: <value>
Location: <value>
Activity at Time of Incident: <value>
Employer at Time of Incident: <value>
Reported to Employer: <value>
Witnesses: <value>

## CLAIMED INJURIES
Body Parts Affected: <value>

Nature of Injuries:
<list each body part with its injury details on a new line, as: "- Body Part: details">

Pre-Existing / Degenerative Flags: <value>
---

Q&A JSON:
{json.dumps(questions_json, indent=2)}
"""


# =========================
# Profile Builder
# =========================
def build_patient_profile(questions_json: dict, model_name: str = DEFAULT_MODEL) -> str:
    llm = ChatOpenAI(
        model=model_name,
        api_key=os.getenv("OPENAI_API_KEY"),
        temperature=0.1,
    )

    prompt = build_prompt(questions_json)
    response = llm.invoke([HumanMessage(content=prompt)])
    return response.content.strip()


# =========================
# IO Helpers
# =========================
def load_answers(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def save_profile(path: str, profile_text: str) -> None:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(profile_text, encoding="utf-8")


# =========================
# Entry Point
# =========================
if __name__ == "__main__":
    questions_json = load_answers(DEFAULT_INPUT_JSON)
    profile = build_patient_profile(questions_json)

    print(profile)

    save_profile(DEFAULT_OUTPUT_TXT, profile)
    print(f"\nProfile saved to: {DEFAULT_OUTPUT_TXT}")