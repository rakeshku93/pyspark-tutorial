

On Linux ( Ubuntu 22.04 LTS ) :
--------------------------------------------

Install Java 8 or 11

```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

set the JAVA_HOME environment variable
```bash
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
source ~/.bashrc
```

verify the installation

```bash
java -version
```

---

Install Python 3.8 or later

```bash
sudo apt update
sudo apt install python3.10
```

verify the installation

```bash
python3 --version
```

---

download spark from the official website

```bash
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xvzf spark-3.5.1-bin-hadoop3.tgz
```

---

set the SPARK_HOME environment variable

```bash
echo "export SPARK_HOME=/path/to/spark-3.5.1-bin-hadoop3" >> ~/.bashrc
source ~/.bashrc
```

---

Optional:

set PYSPARK_PYTHON environment variable

```bash
echo "export PYSPARK_PYTHON=python3" >> ~/.bashrc
source ~/.bashrc
```

---

verify the installation

```bash
$SPARK_HOME/bin/pyspark
```

---

Summary:

- Install Java 8 or 11
- Install Python 3.8 or later
- Download Spark
- Set SPARK_HOME environment variable
- Set PYSPARK_PYTHON environment variable (optional)
- Verify the installation

---

For Windows one more additional setup s required:

download hadoop for windows 
https://github.com/steveloughran/winutils

set the HADOOP_HOME environment variable

---