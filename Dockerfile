FROM flink:latest

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
  apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev openjdk-8-jdk-headless lzma liblzma-dev && \
  wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
  tar -xvf Python-3.7.9.tgz && \
  cd Python-3.7.9 && \
  ./configure --without-tests --enable-shared && \
  make -j6 && \
  make install && \
  ldconfig /usr/local/lib && \
  cd .. && rm -f Python-3.7.9.tgz && rm -rf Python-3.7.9 && \
  ln -s /usr/local/bin/python3 /usr/local/bin/python && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# install PyFlink
RUN pip3 install "apache-flink==1.17.0"

# add python script
USER flink
RUN mkdir /opt/flink/usrlib
COPY ./moving_average_service/ /usr/local/lib/flink/lib/
COPY ./moving_average_service/lib/ /opt/flink/plugins/lib/
ADD ./moving_average_service/plugins/flink-sql-connector-kafka-1.17.0.jar /opt/flink/usrlib/plugins/flink-sql-connector-kafka-1.17.0.jar
ADD ./moving_average_service/plugins/flink-kubernetes-1.17.0.jar /opt/flink/usrlib/plugins/flink-kubernetes-1.17.0.jar
ADD ./moving_average_service/main.py /opt/flink/usrlib/main.py