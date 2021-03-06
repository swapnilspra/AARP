# VERSION 1.8.1-1
# AUTHOR: Swapnil  Prabhu
# DESCRIPTION:  Airflow rabbitmq container

FROM docker.io/fedora/apache
MAINTAINER swapnil

# Airflow parameters
ARG AIRFLOW_VERSION=1.8.0
ARG AIRFLOW_HOME=/usr/local/airflow

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL en_US.UTF-8

 # install  the kernel support rpms 
 RUN dnf -y  install postgresql-server \
    && dnf -y update glibc-common \
    && dnf -y reinstall glibc-common \
    && dnf -y install gcc \
    && dnf -y install python.x86_64\
    && dnf -y install python-devel\
	&& dnf -y install net-tools\
	&& dnf -y install supervisor\
	&& dnf -y install postgresql.x86_64 \
	&& dnf -y install postgresql-devel.x86_64 \
	&& dnf -y install redhat-rpm-config \
	&& dnf -y install gcc-c++ \
	&& dnf -y install rabbitmq-server \
	&& dnf -y install python3-requests \
	&& dnf -y install python-requests \
	&& dnf -y install procps-ng \
	&& dnf -y install findutils.x86_64

	
# Install the airflow required python libraries
RUN  useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow 
 RUN python -m pip install -U pip setuptools wheel \
    && pip install Cython \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install airflow[s3,crypto,celery,postgres,hive,jdbc]==$AIRFLOW_VERSION \
    && pip install airflow[rabbitmq]

 # Installation and configuration of Postgresql
 RUN su - postgres -c "/usr/bin/initdb"
 RUN echo  "host all  all    0.0.0.0/0  md5" >>/var/lib/pgsql/data/pg_hba.conf
 #RUN  su - postgres -c "/usr/bin/pg_ctl -D /var/lib/pgsql/data -l logfile start"
 # DB Setup for Airflow and test
#RUN sleep 10
 #RUN su - postgres -c "psql --command \"CREATE USER airflow WITH SUPERUSER PASSWORD 'airflow';\""
# RUN su - postgres -c "createdb -O airflow airflow"
 #RUN su - airflow -c "airflow initdb"
 #RUN su - postgres -c " /usr/bin/pg_ctl -D /var/lib/pgsql/data -l logfile stop"
# Configurion files.
 COPY postgresql.ini /etc/supervisord.d/postgreql.ini
 COPY rabbitmq.ini /etc/supervisord.d/rabbitmq.ini
 COPY airflow.ini /etc/supervisord.d/airflow.ini
 COPY airflow.cfg  ${AIRFLOW_HOME}/airflow.cfg
 COPY rabbitmqadmin.py /bin/rabbitmqadmin.py
 RUN chmod 777 /bin/rabbitmqadmin.py
 RUN mkdir -p /usr/local/airflow/dags
 RUN mkdir -p /usr/local/airflow/logs
 RUN mkdir -p /usr/local/airflow/plugins
 RUN chown -R airflow.airflow /usr/local/airflow/
 COPY HelloWorld*.* /usr/local/airflow/dags/
 # Setup rabbitmq  

#RUN rabbitmq-server &
#RUN sleep 10
#RUN rabbitmq-plugins enable rabbitmq_web_mqtt rabbitmq_web_mqtt_examples rabbitmq_web_stomp rabbitmq_web_stomp_examples rabbitmq_trust_store rabbitmq_top rabbitmq_management_agent rabbitmq_management rabbitmq_jms_topic_exchange rabbitmq_amqp1_0
#RUN sleep 5
#RUN rabbitmqadmin.py  declare user name=airflow  password=airflow  tags=administrator
#RUN rabbitmqadmin.py  declare queue name=airflow
#RUN rabbitmqadmin.py  declare permission vhost=/ user=airflow configure=.* write=.* read=.*

COPY entrypoint.sh /bin/entrypoint.sh

RUN chown -R airflow: ${AIRFLOW_HOME}
EXPOSE 8080 5555 5432 8793 15670

#USER airflow
#WORKDIR ${AIRFLOW_HOME}
CMD "/bin/entrypoint.sh"

