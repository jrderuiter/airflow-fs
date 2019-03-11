ARG PYTHON_VERSION=3.6.8
FROM python:${PYTHON_VERSION}-stretch

LABEL org.label-schema.name="airflow-fs ci $PYTHON_VERSION" \
      org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.version=$PYTHON_VERSION

# Setup password-less SSH.
RUN apt-get update \
    && apt-get install -y openssh-server \
    && apt-get clean \
    && ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa \
    && cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

# Install Java JRE.
ENV JAVA_HOME /usr/local/java
RUN apt-get update \
    && apt-get install -y default-jre \
    && apt-get clean \
    && ln -s $(readlink -f /usr/bin/java | sed "s:bin/java::") /usr/local/java

# Install Hadoop.
RUN curl -s http://mirror.koddos.net/apache/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz | tar -xz -C /usr/local/ \
    && mv /usr/local/hadoop-3.1.2 /usr/local/hadoop \
    && mkdir -p /home/root

ENV HADOOP_HOME=/usr/local/hadoop \
    HADOOP_COMMON_HOME=/usr/local/hadoop \
    HADOOP_HDFS_HOME=/usr/local/hadoop \
    HADOOP_MAPRED_HOME=/usr/local/hadoop \
    HADOOP_YARN_HOME=/usr/local/hadoop \
    HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root \
    PATH=$PATH:/usr/local/hadoop/bin

RUN sed -i "/^# export JAVA_HOME/ s:.*:export JAVA_HOME=${JAVA_HOME}\nexport HADOOP_HOME=${HADOOP_HOME}\n:" $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && sed -i "/^export HADOOP_CONF_DIR/ s:.*:export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop/:" $HADOOP_HOME/etc/hadoop/hadoop-env.sh \
    && chmod +x $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Setup Hadoop in pseudo-distributed mode.
ADD hdfs/* $HADOOP_HOME/etc/hadoop/
RUN $HADOOP_HOME/bin/hdfs namenode -format

# Setup proftpd.
RUN set -x \
    && apt-get update \
    && apt-get install -y proftpd \
    && apt-get clean

ADD ftp/proftpd.conf /etc/proftpd/proftpd.conf

RUN set -x \
    && chown root:root /etc/proftpd/proftpd.conf \
    && mkdir /ftp \
    && PASSWORD=$(perl -e 'print crypt("default", "password")' $PASSWORD) \
    && useradd --shell /bin/sh --create-home --password $PASSWORD ftp-user \
    && chown -R ftp-user:ftp-user /ftp

# Pre-install Airflow dependency.
ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
RUN pip install apache-airflow

ADD bootstrap.sh /usr/sbin/bootstrap.sh
ENTRYPOINT ["/bin/bash"]
