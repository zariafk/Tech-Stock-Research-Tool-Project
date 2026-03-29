import altair as alt

# 1. The Price Line
price_line = alt.Chart(df_history).mark_line(color='white').encode(
    x='bar_date:T',
    y='close:Q'
)

# 2. The Reddit Sentiment Points
sentiment_points = alt.Chart(df_reddit).mark_circle().encode(
    x='created_at:T',  # Join from reddit_post
    y='close:Q',      # Positioned on the price line
    size='relevance_score:Q',
    color=alt.Color('sentiment_score:Q',
                    scale=alt.Scale(scheme='redyellowgreen')),
    tooltip=['title', 'sentiment_score', 'relevance_score']
)

# Layer them together
st.altair_chart(price_line + sentiment_points, use_container_width=True)
