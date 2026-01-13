#!/bin/bash
set -e

export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# JAVA_HOME 자동 감지: java 실행파일 위치를 기준으로 상위 디렉토리 계산
export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"
export PATH=$PATH:$JAVA_HOME/bin

echo "[INFO] JAVA_HOME=$JAVA_HOME"
java -version || true

# 최초 1회 NameNode 포맷 (볼륨 사용 시 재시작해도 current가 남아있음)
if [ ! -d "/hadoop/dfs/name/current" ]; then
  echo "[INIT] Formatting NameNode (first run only)..."
  hdfs namenode -format -force
else
  echo "[INIT] NameNode already formatted."
fi

echo "[START] Starting NameNode..."
hdfs --daemon start namenode

echo "[START] Starting DataNode..."
hdfs --daemon start datanode

echo "[OK] Hadoop(HDFS) is running."
echo " - NameNode UI: http://localhost:9870"

# 컨테이너 유지
tail -f /dev/null
