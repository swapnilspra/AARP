import json
import logging
import pika
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
QUEUE = 'mediametrics.servicebus.latch.queue'
ROUTING_KEY = 'mediametrics.servicebus.latch.queue'


class RabbitMQSensor(BaseSensorOperator):
    """
     Listens to rabbitMQ channel for incmoning data
    :param conn_id: The rabbitmq uri to run the sensor against
    :type conn_id: str
    :param user: The rabbitmq user login
    :type user: str
    :param password: duh
    :type password: str
    :param virtualhost: rabbitmq virtual host
    :type virtualhost: str
    """
    ui_color = '#7c7287'

    @apply_defaults
    def __init__(self, conn_id, user, password, virtualhost, *args, **kwargs):
        self._conn_id = conn_id
        self._user = user
        self._pass = password
        self._virtualhost = virtualhost
        self.fileGroup = None
        self.taskId = None

        super(RabbitMQSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        ## add stuff for rabbitmq connection

        logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

        credentials = pika.PlainCredentials(self._user, self._pass)
        parameters = pika.ConnectionParameters(self._conn_id,
                                               5672,
                                               self._virtualhost,
                                               credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE, durable=True)
        method_frame, header_frame, body = channel.basic_get(QUEUE)
##        if method_frame:
        LOGGER.info('Received message # %s from %s: %s',
                        method_frame, header_frame, body)
        #channel.basic_ack(method_frame.delivery_tag)
        #jsonMsg = json.loads(body)
        #self.fileGroup = json.dumps(jsonMsg['filegroup'])
        #self.taskId = "mediametrics."+ jsonMsg['taskId']
        #LOGGER.info("Json msg: %s ",self.fileGroup)
        #Variable.set(self.taskId,self.fileGroup)
        #Variable.set('mediametrics.branch.route', "trigger_"+jsonMsg['taskId']+"_dag")
        #LOGGER.info('Set to trigger: %s', self.taskId)

        return True
        #else:
            #print('No message returned')
            #LOGGER.info('No message returned')
            #return False
