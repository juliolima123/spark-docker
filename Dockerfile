FROM python:3.11-bullseye as spark-base

# Instalação de pacotes
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      wget \
      vim \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      openssh-server openssh-client \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Instalando Spark e Hadoop

# Variáveis de Ambiente
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
ENV SCALA_VERSION=2.13

# Criando pastas necessárias
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# Download Spark
RUN curl https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark-3.5.1-bin-hadoop3.tgz \
 && tar xvzf spark-3.5.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-3.5.1-bin-hadoop3.tgz

WORKDIR ${SPARK_HOME}/jars

# Adicionando pacote e dependências do spark-xml 
# RUN ${SPARK_HOME}/bin/spark-shell --packages com.databricks:spark-xml_2.12:0.17.0
RUN wget https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar -O ${SPARK_HOME}/jars/spark-xml_2.12-0.18.0.jar && \
    rm -rf spark-xml_2.12-0.18.0.jar

# Adicionando pacote e dependências do Hadoop AWS 
RUN ${SPARK_HOME}/bin/spark-shell --packages org.apache.hadoop:hadoop-aws:3.3.4

# Copiando jars da pasta /root/.ivy2/jars
RUN mv /root/.ivy2/jars/* ${SPARK_HOME}/jars \
 && rm -rf /root/.ivy2

# Configurar o classpath
ENV CLASSPATH="${SPARK_HOME}/jars/*:${CLASSPATH}"

WORKDIR ${SPARK_HOME}

FROM spark-base as pyspark

# Instalando Bibliotecas
COPY /requirements.txt .
RUN pip3 install -r requirements.txt

# Variáveis de Ambiente
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
ENV PYSPARK_SUBMIT_ARGS="--packages com.databricks:spark-xml_2.13:0.17.0 pyspark-shell"

# Permissões
COPY conf/spark-defaults.conf "${SPARK_HOME}/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY entrypoint.sh .

RUN chmod +x entrypoint.sh  

# Instalando dotenv
RUN pip install --upgrade pip setuptools && \
    pip install git+https://github.com/theskumar/python-dotenv.git

ENTRYPOINT ["./entrypoint.sh"]
