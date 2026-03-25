import os
from dotenv import load_dotenv
import requests

load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("Set OPENAI_API_KEY first")

url = "https://api.openai.com/v1/chat/completions"
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}

payload = {
    "model": "gpt-4.1-nano",
    "messages": [{"role": "user", "content": "ping"}],
    "max_tokens": 1,
    "temperature": 0,
}

resp = requests.post(url, headers=headers, json=payload, timeout=60)

print("Status:", resp.status_code)
if resp.status_code >= 400:
    print("Error:", resp.text)

# Print all rate-limit related headers
for k, v in resp.headers.items():
    if "ratelimit" in k.lower():
        print(f"{k}: {v}")
