FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Java + 필수 유틸 설치
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    procps \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

#  Hadoop 다운로드/설치
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop

RUN curl -fsSL https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    | tar -xz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME}

# PATH
ENV PATH="${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin"

#  Hadoop 설정 파일 복사
COPY config/core-site.xml ${HADOOP_HOME}/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
COPY config/mapred-site.xml ${HADOOP_HOME}/etc/hadoop/mapred-site.xml

#  HDFS 데이터 디렉토리(영속성 대상)
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data \
    && chmod -R 777 /hadoop/dfs

VOLUME ["/hadoop/dfs"]

#  포트(웹 UI + RPC)
EXPOSE 9870 9864 9000

# 시작 스크립트
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
