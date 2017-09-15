su - postgres -c " /usr/bin/pg_ctl -D /var/lib/pgsql/data -l logfile start"
sleep 10
su - postgres -c "psql --command \"CREATE USER airflow WITH SUPERUSER PASSWORD 'airflow';\""
su - postgres -c "createdb -O airflow airflow"
su - airflow -c "airflow initdb"
su - postgres -c " /usr/bin/pg_ctl -D /var/lib/pgsql/data -l logfile stop"
rabbitmq-server&
sleep 10
rabbitmq-plugins enable  rabbitmq_web_mqtt rabbitmq_web_mqtt_examples rabbitmq_web_stomp rabbitmq_web_stomp_examples rabbitmq_trust_store rabbitmq_top rabbitmq_management_agent rabbitmq_management rabbitmq_jms_topic_exchange rabbitmq_amqp1_0
sleep 5
rabbitmqadmin.py  declare user name=airflow  password=airflow  tags=administrator
rabbitmqadmin.py  declare queue name=airflow
rabbitmqadmin.py  declare permission vhost=/ user=airflow configure=.* write=.* read=.
rabbitmqctl stop
/usr/bin/supervisord -c /etc/supervisord.conf
sleep 5
/usr/bin/supervisorctl status
