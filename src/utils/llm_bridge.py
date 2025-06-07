"""
Async OpenAI chat call for batches of prompts.
Replace OPENAI_KEY with env-var in production.
"""

import os, httpx, asyncio
from typing import List, Tuple

OPENAI_KEY = os.getenv("OPENAI_API_KEY", "YOUR_FAKE_KEY")

SYSTEM_PROMPT = (
    "You are a procurement matching assistant.\n"
    "Given one delivery line and up to five price-list candidates, "
    "return JSON: {is_match:bool, best_material_number:str, confidence:float}."
)

async def _post(payload):
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_KEY}"},
            json=payload,
        )
    return r.json()

async def classify_batches(batches: List[Tuple[str, str]]):
    """
    batches = list of (delivery_prompt, prices_prompt) strings.
    """
    tasks = []
    for dp, pp in batches:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": dp + "\n\n" + pp},
        ]
        tasks.append(_post({"model": "gpt-4o-mini",
                            "messages": messages,
                            "temperature": 0}))
    return await asyncio.gather(*tasks)
