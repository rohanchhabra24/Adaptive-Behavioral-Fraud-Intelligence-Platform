FROM flink:1.17.1-scala_2.12
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    ln -s /usr/bin/python3 /usr/bin/python
COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt
WORKDIR /opt/flink/usrlib
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.17.1/flink-json-1.17.1.jar
COPY src/ ./src/
COPY start.sh ./start.sh
RUN chmod +x start.sh
CMD ["./start.sh"]
