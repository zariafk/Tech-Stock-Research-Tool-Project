FROM python:3.13-slim

COPY requirements.txt .

RUN pip3 install -r requirements.txt 

COPY dashboard.py .

COPY live/dashboard_graphs.py ./live/
COPY live/dashboard_queries.py ./live/
COPY live/live_dashboard.py ./live/

COPY historical/dashboard_graphs.py ./historical/
COPY historical/dashboard_queries.py ./historical/
COPY historical/hist_dashboard.py ./historical/

# listen on port 8501 for Streamlit
EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]