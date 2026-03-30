FROM python:3.13-slim

COPY summary/requirements.txt .

RUN pip3 install -r requirements.txt

COPY summary/dashboard.py .
COPY summary/queries.py .

# listen on port 8501 for Streamlit
EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]