# FROM apache/apache/spark:3.5.2
FROM bitnami/spark:3.5.2-debian-12-r0

COPY main.py /opt/spark/work-dir/
COPY requirements.txt /opt/spark/work-dir/

RUN pip install -r /opt/spark/work-dir/requirements.txt

ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit"]