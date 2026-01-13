# Single-Node Hadoop Cluster using Docker

## 1. Project Overview

This project demonstrates how to set up a **single-node Apache Hadoop (HDFS) cluster using Docker**.  
The objective is to gain hands-on experience with Hadoop configuration, Docker containerization, HDFS operations, and data persistence.

The Hadoop cluster runs inside a Docker container and automatically starts the required services (NameNode and DataNode).  
HDFS can be accessed both via CLI and via the Hadoop Web UI from the host machine.

---

## 2. Environment

- Host OS: macOS  
- Container OS: Ubuntu 22.04  
- Hadoop Version: 3.3.6  
- Java Version: OpenJDK 11  
- Docker: Docker Desktop for macOS  

---

## 3. Project Structure

```text
hadoop-docker-single-node/
├── Dockerfile
├── entrypoint.sh
├── config/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   └── mapred-site.xml
└── README.md
```

---

## 4. Docker Image Build

Build the Docker image using the Dockerfile.

```bash
docker build -t my-hadoop-single:1.0 .
```

Verify the image:

```bash
docker images
```

---

## 5. Run the Hadoop Container

Run the container with port mapping and a Docker volume for HDFS persistence.

```bash
docker run -d --name hadoop1 \
  -p 9870:9870 -p 9000:9000 -p 9864:9864 \
  -v hadoop_data:/hadoop/dfs \
  my-hadoop-single:1.0
```

Check container status:

```bash
docker ps
```

---

## 6. Hadoop Services

When the container starts, the following services are automatically launched:

- NameNode: Manages HDFS metadata  
- DataNode: Stores actual HDFS data blocks  

Verify processes inside the container:

```bash
docker exec -it hadoop1 bash
jps
```

Expected output:

```text
NameNode
DataNode
Jps
```

---

## 7. Hadoop Web UI

The Hadoop NameNode Web UI is accessible from the host machine:

```text
http://localhost:9870
```

The Web UI allows monitoring of:

- Cluster status  
- Live DataNodes  
- HDFS file system structure  

---

## 8. HDFS Operations

### 8.1 Create Directory in HDFS

```bash
hdfs dfs -mkdir -p /user/testdir
hdfs dfs -ls /user
```

---

### 8.2 Create and Upload a File to HDFS

Create a local file inside the container:

```bash
echo "Hello Hadoop from Docker by Minsu" > hello.txt
```

Upload the file to HDFS:

```bash
hdfs dfs -put -f hello.txt /user/testdir/
hdfs dfs -ls /user/testdir
```

---

### 8.3 Verify File Content from HDFS

```bash
hdfs dfs -cat /user/testdir/hello.txt
```

Output:

```text
Hello Hadoop from Docker by Minsu
```

---

## 9. Data Persistence Verification

HDFS data is stored in a Docker volume to ensure persistence across container restarts.

### 9.1 Stop and Restart the Container (Host)

```bash
docker stop hadoop1
docker start hadoop1
```

### 9.2 Verify Data After Restart

```bash
docker exec -it hadoop1 bash
hdfs dfs -ls /user/testdir
hdfs dfs -cat /user/testdir/hello.txt
```
