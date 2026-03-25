import os
from dotenv import load_dotenv
from openai import OpenAI


# 1. Load variables from the .env file into the system environment
load_dotenv()

# 2. Initialize the client using the environment variable
# If the key isn't found, os.getenv returns None and OpenAI will raise an error
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def analyze_sentiment(text):
    """
    Analyzes text and returns a sentiment score from -1.0 to 1.0.
    """
    if not text.strip():
        return 0.0

    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a precise sentiment analysis tool. "
                        "Return a single float between -1.0 (very negative) and 1.0 (very positive). "
                        "Return ONLY the number. No words, no punctuation."
                    )
                },
                {"role": "user", "content": text}
            ],
            temperature=0,
        )

        # Clean the output and convert to float
        result = response.choices[0].message.content.strip()
        return float(result)

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


if __name__ == "__main__":
    print("--- AI Sentiment Analyzer (v2.0) ---")
    print("Type 'exit' to quit.")

    while True:
        user_input = input("\nEnter text to analyze: ")

        if user_input.lower() == 'exit':
            break

        score = analyze_sentiment(user_input)

        if score is not None:
            # Formatting the output for readability
            color = "🟢" if score > 0.25 else "🔴" if score < -0.25 else "⚪"
            print(f"Score: {score} {color}")
