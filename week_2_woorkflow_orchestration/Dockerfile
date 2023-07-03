FROM prefecthq/prefect:2.10-python3.9-conda

COPY docker-requirements.txt .
RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org

COPY flows /opt/prefect/flows
COPY data /opt/prefect/data
RUN mkdir -p /opt/prefect/data/yellow