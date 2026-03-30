Model: gpt-4o-mini

Cost: $0.15/1M token inputs

Example prompt:
"""
    Act as: Senior Quant Analyst.
    Universe: NOW, U
    Input: "Beginner here with zero knowledge of stock trading. Where should I start, what resources should I follow, and how do I avoid costly mistakes in the beginning?" | "I’m a complete beginner with no knowledge of stock trading and want to start my journey in the right way. I’m looking for guidance on where to begin, what fundamental concepts I should learn first, and which reliable resources or platforms I should follow. I also want to understand common mistakes that beginners make so I can avoid losing money early on. Any advice on building a strong foundation, managing risk, and developing the right mindset for long-term success in trading would be really helpful."
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
Tokens: 561

Max Total prompts per day per subreddit: 10

Total subreddits: 12

Time Frame 01/01/2024-today 819 days

Total cost = 819 * 12 * 100 * 561 * 0.15 / 1000000 = $8.27