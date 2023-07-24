FROM ubuntu:22.04

# shanghai zoneinfo
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo '$TZ' > /etc/timezone

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64
ENV PATH=$PATH:/home/graphar/.local/bin/:/opt/spark-3.2.2-bin-hadoop3.2/bin


ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/opt/spark-3.2.2-bin-hadoop3.2

RUN apt-get update && \
    apt-get install -y sudo vim git curl build-essential libssl-dev default-jdk maven cmake libcurl4-openssl-dev libboost-graph-dev && \
    apt-get clean -y && \
    rm -rf /var/lib/apt/lists/*

RUN curl https://archive.apache.org/dist/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz | sudo tar -xz -C /opt/

RUN useradd -m graphar -u 1001 \
    && echo 'graphar ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

USER graphar
WORKDIR /home/graphar
