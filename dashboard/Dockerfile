FROM python:3.13-slim

COPY requirements.txt .

RUN pip3 install -r requirements.txt 

COPY dashboard.py .
COPY chatbot.py .
COPY siphon_logo.png .

COPY summary/__init__.py ./summary/
COPY trends/__init__.py ./trends/

COPY summary/charts.py ./summary/
COPY summary/dashboard.py ./summary/
COPY summary/helpers.py ./summary/
COPY summary/queries.py ./summary/

COPY trends/charts.py ./trends/
COPY trends/dashboard.py ./trends/
COPY trends/helpers.py ./trends/
COPY trends/queries.py ./trends/


# listen on port 8501 for Streamlit
EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]