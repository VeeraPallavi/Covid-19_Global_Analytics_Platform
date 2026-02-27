#!/bin/bash

echo "COVID PROJECT ENV SETUP STARTED "

# Step 1: Activate Virtual Environment

source spark_env/bin/activate

# Step 2: Check & Set Java

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin
echo "JAVA_HOME set to $JAVA_HOME"

# Step 3: Check & Set Hadoop

export HADOOP_HOME=$HOME/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/Hadoop

# Step 4: pyspark installation

echo "Installing pyspark via pip"
pip install pyspark==3.3.2

# Step 5: Install Jupyter Notebook

pip install notebook

#Install Python Kernel and register the environment
pip install ipykernel
python -m ipykernel install --user --name=spark_env --display-name "Python (Spark Env)"

#Start Jupyter Notebook
jupyter notebook 

# Step 6: Verify Installations

echo "Verifying installations..."
echo -n "Java Version: "
java -version 2>&1 | head -n 1

echo -n "Hadoop Version: "
hadoop version | head -n 1

echo -n "PySpark Version: "
python -c "import pyspark; print(pyspark.__version__)"

# Step 7: Create Required HDFS Directories

echo "Creating HDFS directories for COVID project..."

hdfs dfs -mkdir -p /data/covid/raw
hdfs dfs -mkdir -p /data/covid/staging
hdfs dfs -mkdir -p /data/covid/curated
hdfs dfs -mkdir -p /data/covid/analytics



