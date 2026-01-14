# Hadoop MapReduce WordCount (Python Streaming)

## 1. Project Overview
This project demonstrates how to run a Hadoop MapReduce job on a Docker-based multi-node Hadoop cluster using **Hadoop Streaming** with **Python**.
The job reads a large English e-book from **HDFS**, counts the occurrences of each word, and writes the results back to **HDFS**.

## 2. Environment
- Hadoop Version: 3.3.6
- Cluster Type: Docker-based Multi-node Cluster
- Containers:
  - NameNode: `namenode`
  - DataNodes: `datanode1`, `datanode2`
- MapReduce Framework: YARN (MRv2)
- Programming Language: Python (Hadoop Streaming)

## 3. Input Data
- Title: *Moby-Dick; or, The Whale*
- Author: Herman Melville
- Source: Project Gutenberg
- Download Link: https://www.gutenberg.org/ebooks/2701.txt.utf-8
- Local File Path (Host): `~/Desktop/pg2701.txt`

## 4. Prerequisites
- Docker and Docker Compose installed
- Hadoop multi-node cluster containers running
- Python 3 available inside Hadoop containers
- HDFS and YARN services running normally

## 5. Step-by-Step Execution

### 5.1 Verify Containers
```bash
docker ps
```

### 5.2 Copy Input File to NameNode
```bash
docker cp ~/Desktop/pg2701.txt namenode:/root/ebook.txt
```

### 5.3 Enter NameNode Container
```bash
docker exec -it namenode bash
```

### 5.4 Upload File to HDFS
```bash
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -put -f /root/ebook.txt /user/hadoop/input/pg2701.txt
hdfs dfs -ls /user/hadoop/input
```

### 5.5 Run MapReduce Job
Check Safe Mode:
```bash
hdfs dfsadmin -safemode get
```

If ON:
```bash
hdfs dfsadmin -safemode leave
```

Remove old output:
```bash
hdfs dfs -rm -r -f /user/hadoop/output_wordcount
```

Run Hadoop Streaming:
```bash
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input /user/hadoop/input/pg2701.txt \
  -output /user/hadoop/output_wordcount
```

### 5.6 Retrieve Output
```bash
hdfs dfs -ls /user/hadoop/output_wordcount
hdfs dfs -cat /user/hadoop/output_wordcount/part-* | head
```

Top 20 words:
```bash
hdfs dfs -cat /user/hadoop/output_wordcount/part-* | sort -k2,2nr | head -n 20
```

## 6. Output Format
Each line of output:
```
word    count
```

## 7. Monitoring
- YARN ResourceManager: http://localhost:8088
- HDFS NameNode UI: http://localhost:9870
- JobHistory UI: http://localhost:19888

## 8. Result
The MapReduce job successfully processes the entire e-book and produces accurate word counts stored in HDFS.


the	14727
of	6746
and	6514
a	4805
to	4709
in	4244
that	3100
it	2537
his	2532
i	2127
he	1900
s	1827
but	1822
with	1770
as	1753
is	1748
was	1647
for	1644
all	1544
this	1441
at	1333
whale	1243
