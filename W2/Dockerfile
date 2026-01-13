FROM quay.io/jupyter/base-notebook:latest

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY notebooks/ /home/jovyan/work/

EXPOSE 8888

CMD ["start-notebook.py", "--ServerApp.ip=0.0.0.0", "--ServerApp.port=8888", "--ServerApp.token=my-token"]
