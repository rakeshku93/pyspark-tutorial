



1. python spark-shell

```bash
$SPARK_HOME/bin/pyspark
```

2. Notebook
    ...

3. IDE ( vs-code | PyCharms)
    ...


------------------------------------------

Create Python Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

on Windows

```bash
$ python -m venv venv
$ venv\Scripts\activate
```

-------------------------------------------

Install pyspark

```bash
$ pip install pyspark
```

-------------------------------------------

Create Python File
    
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkApp").getOrCreate()
print(spark.version)
spark.stop()
```

-------------------------------------------
    