Example Prompt:
"""
Act as: Senior Quant Analyst.
Universe: NOW, U
Input: "NKE Discussion" | "Repost

I don’t know why nobody’s talking about this, but Nike is going to post quarterly earnings today after the bell. The stock is currently at it’s 10 year lows and I do believe it’s the rock bottom. Previous management fucked up and the new CEO is trying to turn the ship around.

Expectations are so low that Nike is going to beat and it’s going to post decent earnings and the market is gonna like it and catch a momentum.

My thesis is that having decent/good news is going to make the market believe it was a bottom and people are going to buy back since the stock is almost -70% since it’s all-time high. Markets are going to start thinking the ship is actually turning around and I expect it to pump at least 8% and even double that.

2016 revenue was $32 billion and the stock price was at $50-55.

2021 revenue was $44 billion and the stock price reached its all time high at $179

2026 expected revenue $47 billion and the stock price is sitting at it’s 10 year lows.

I am aware that profit margins have shrunk but still the stock price is super heavily over sold and it is due for a run up.

Positions 55c and 59c expiring in 2 days.

TLDR; nke stock is extremely oversold, wsb is super bearish which means its gonna pump and im just a regard holding calls. 

What do you guys think? "
    Task: Score Relevance (0-10), Sentiment (-1.0 to 1.0), and Confidence in your scoring.
    Relevance Rubric:
    - 10: Direct idiosyncratic event (Earnings, M&A).
    - 8: Significant business news (New product, contract).
    - 7: Indirect impact (Competitor/Sector news).
    - <7: Ignore.
    Sentiment Rubric (Score from -1.0 to 1.0 in 0.1 increments):
    - +1.0: Transformational/Systemic positive (M&A, massive earnings beat).
    - +0.7 to +0.9: Strong positive (Major product launch, key contract win).
    - +0.3 to +0.6: Moderate positive (Positive analyst upgrade, steady growth).
    - +0.1 to +0.2: Slight positive (Minor positive mention, general market lift).
    - 0.0: Neutral, administrative, or purely factual noise.
    - -0.1 to -0.2: Slight negative (Minor litigation, general market drag).
    - -0.3 to -0.6: Moderate negative (Missed estimates, management turnover).
    - -0.7 to -0.9: Strong negative (Regulatory investigation, product recall).
    - -1.0: Catastrophic (Bankruptcy, fraud, massive data breach).
    Confidence Scoring:
    - High: Clear signal. Direct quotes, confirmed by multiple sources, or explicit event.
    - Medium: Reasonable inference but some ambiguity. Analyst opinion or sector trend.
    - Low: Speculative or based on rumor. Requires corroboration.
    Output Format (JSON list):
    [{
      "t": "TICKER",
      "r": score,
      "s": score,
      "c": "High|Medium|Low",
      "why": "one sentence justification"
    }]
"""


Example Output:
"""
ChatCompletion(id='chatcmpl-DPVbJ6d2RIrZFY0pKW6EgwYScD2FZ', choices=[Choice(finish_reason='stop', index=0, logprobs=None, message=ChatCompletionMessage(content='```json\n[{\n  "t": "NKE",\n  "r": 10,\n  "s": 0.7,\n  "c": "Medium",\n  "why": "The upcoming earnings report is a direct event with potentially significant implications for the stock\'s price action, though the expectations seem to be based on speculative beliefs rather than concrete evidence."\n}]\n```', refusal=None, role='assistant', annotations=[], audio=None, function_call=None, tool_calls=None))], created=1774972045, model='gpt-4o-mini-2024-07-18', object='chat.completion', service_tier='default', system_fingerprint='fp_e738e3044b', usage=CompletionUsage(completion_tokens=77, prompt_tokens=749, total_tokens=826, completion_tokens_details=CompletionTokensDetails(accepted_prediction_tokens=0, audio_tokens=0, reasoning_tokens=0, rejected_prediction_tokens=0), prompt_tokens_details=PromptTokensDetails(audio_tokens=0, cached_tokens=0)))
[31/Mar/2026 16:47:28] INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
"""

Total prompt tokens: 749
Total output tokens: 77
Total sub-reddit's searched: 12
Total posts current fetched per subreddit: 25
Total runs per day: 32
Typically proportion of analysed posts: 1/3
gpt-4o-mini pricing
    input: $0.15 per million tokens
    output: $0.60 per million tokens

Max total cost: $1.52
Avg total cost: $0.51