"""
AI Advisor — OpenAI GPT-powered portfolio Q&A.

Feeds fund definitions, market context, and user risk profile to ChatGPT
and streams the answer back.
"""

from __future__ import annotations

import os
from typing import Generator

from backend.risk_profiler.risk_model import FUND_PROFILES, RISK_LABELS

SYSTEM_PROMPT = """You are AssetEra AI Advisor, an expert financial assistant built into the AssetEra portfolio management platform.

## Your capabilities
- Answer questions about AssetEra's five model funds (F1-F5)
- Explain risk profiles, asset allocation, and diversification
- Help users understand their portfolio fit based on demographics
- Provide general financial education (NOT personalised investment advice)

## Available funds
{fund_context}

## Risk profile scale
1 = Very Conservative, 2 = Conservative, 3 = Moderate, 4 = Aggressive, 5 = Very Aggressive

## Rules
- Always clarify you are NOT a licensed financial advisor and this is NOT investment advice
- Be concise but thorough — use bullet points and tables where helpful
- If the user shares their risk profile, reference the matching funds
- If asked about specific stocks, explain what role they play in the fund allocations
- Use plain English; avoid jargon unless the user is clearly experienced
"""


def _build_fund_context() -> str:
    lines = []
    for fid, info in FUND_PROFILES.items():
        allocs = ", ".join(f"{t} {w:.0%}" for t, w in list(info["allocations"].items())[:8])
        if len(info["allocations"]) > 8:
            allocs += f" ... +{len(info['allocations']) - 8} more"
        lines.append(
            f"- **{fid}: {info['name']}** (risk {info['risk_range'][0]}-{info['risk_range'][1]}): "
            f"{info['description']}. Top holdings: {allocs}. "
            f"Expected: {info['expected_return']}."
        )
    return "\n".join(lines)


def get_system_prompt(user_risk: int | None = None) -> str:
    prompt = SYSTEM_PROMPT.format(fund_context=_build_fund_context())
    if user_risk:
        prompt += (
            f"\n\n## Current user's risk profile\n"
            f"Risk score: {user_risk} ({RISK_LABELS.get(user_risk, 'Unknown')})\n"
            f"Reference this when making fund suggestions."
        )
    return prompt


def stream_response(
    messages: list[dict],
    system: str,
    api_key: str,
) -> Generator[str, None, None]:
    """Stream a response from OpenAI GPT. Yields text chunks."""
    from openai import OpenAI

    client = OpenAI(api_key=api_key)

    api_messages = [{"role": "system", "content": system}] + messages

    stream = client.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=1024,
        messages=api_messages,
        stream=True,
    )

    for chunk in stream:
        delta = chunk.choices[0].delta
        if delta.content:
            yield delta.content
