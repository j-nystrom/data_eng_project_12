FROM ubuntu:22.04
RUN apt-get update
RUN apt-get -y upgrade
RUN apt install -y openjdk-11-jdk-headless
RUN apt install -y scala
RUN apt install -y wget
RUN apt install -y screen
RUN apt install -y net-tools
RUN apt install -y iputils-ping
RUN apt install -y iptables
RUN apt install -y nmap
RUN apt install -y nano
RUN apt install -y git
RUN apt install -y python3-pip
RUN apt install zstd
RUN pip install pandas
RUN pip install notebook
RUN pip install matplotlib
RUN pip install pyspark==3.2.3
RUN pip install pyarrow
RUN pip install fastparquet
RUN wget https://archive.apache.org/dist/spark/spark-3.2.3/spark-3.2.3-bin-hadoop3.2.tgz
RUN tar xvf spark-3.2.3-bin-hadoop3.2.tgz
RUN rm spark-3.2.3-bin-hadoop3.2.tgz
RUN mv spark-3.2.3-bin-hadoop3.2/ /usr/local/spark
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
RUN tar xzf hadoop-3.3.1.tar.gz
RUN rm hadoop-3.3.1.tar.gz
RUN mv hadoop-3.3.1/ /usr/local/hadoop
ENV PATH="${PATH}:$SPARK_HOME/bin"
ENV SPARK_HOME="/usr/local/spark"
ENV SPARK_NO_DAEMONIZE="true"
ENV HADOOP_HOME="/usr/local/hadoop"
ENV PATH="${PATH}:$HADOOP_HOME/bin"
ENV JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
ADD xml/core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml
ADD xml/hdfs-site.xml /usr/local/hadoop/etc/hadoop/hdfs-site.xml

