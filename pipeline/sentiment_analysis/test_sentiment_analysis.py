"""Unit tests for sentiment_analysis.py."""

import pytest
from unittest.mock import MagicMock, patch


# Patch the OpenAI client at import time so no real API calls are made
with patch("sentiment_analysis.OpenAI"):
    from sentiment_analysis import analyze_sentiment


def make_mock_response(content: str) -> MagicMock:
    """Build a mock OpenAI chat completion response with the given content string."""
    message = MagicMock()
    message.content = content
    choice = MagicMock()
    choice.message = message
    response = MagicMock()
    response.choices = [choice]
    return response


# === Empty / whitespace input ===

def test_empty_string_returns_zero():
    assert analyze_sentiment("") == 0.0


def test_whitespace_only_returns_zero():
    assert analyze_sentiment("   ") == 0.0


# === Valid API responses ===

@patch("sentiment_analysis.client")
def test_positive_sentiment(mock_client):
    mock_client.chat.completions.create.return_value = make_mock_response(
        "0.9")
    result = analyze_sentiment("Apple stock surges to record high!")
    assert result == pytest.approx(0.9)


@patch("sentiment_analysis.client")
def test_negative_sentiment(mock_client):
    mock_client.chat.completions.create.return_value = make_mock_response(
        "-0.8")
    result = analyze_sentiment("Markets crash amid recession fears.")
    assert result == pytest.approx(-0.8)


@patch("sentiment_analysis.client")
def test_neutral_sentiment(mock_client):
    mock_client.chat.completions.create.return_value = make_mock_response(
        "0.0")
    result = analyze_sentiment("The market closed today.")
    assert result == pytest.approx(0.0)


@patch("sentiment_analysis.client")
def test_returns_float_type(mock_client):
    mock_client.chat.completions.create.return_value = make_mock_response(
        "0.5")
    result = analyze_sentiment("Stocks rose slightly.")
    assert isinstance(result, float)


# === Error handling ===

@patch("sentiment_analysis.client")
def test_api_exception_returns_none(mock_client):
    mock_client.chat.completions.create.side_effect = Exception("API error")
    result = analyze_sentiment("Some text.")
    assert result is None


@patch("sentiment_analysis.client")
def test_non_numeric_response_returns_none(mock_client):
    mock_client.chat.completions.create.return_value = make_mock_response(
        "very positive")
    result = analyze_sentiment("Some text.")
    assert result is None
