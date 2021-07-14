import logging
from paho.mqtt import client

LOGGER=logging.getLogger(__name__)

client.Client.connected_flag=False
client.Client.disconnected_flag=False
client.Client.bad_connection_flag=False

class MqttHandler:
    """
    This is used as a proxy to the MQTT-broker.
    """
    def __init__(self, mqtt_client=None):
        """
        MQQT handler class constructor to initialize the object.

        Input Arguments: 
            mqtt_client  :  The MQTT client.
        """
        self._mqtt_client = mqtt_client

    @property
    def mqtt_client(self):
        """
        The property to get the mqtt_client.
        """
        return self._mqtt_client

    @mqtt_client.setter
    def mqtt_client(self, newMqttClient):
        """
        This is called when self.mqtt_client is updated.
        """
        self._mqtt_client = newMqttClient

    def on_connect(self, mqtt_client, userdata, flags, rc):
        if rc == 0:
            mqtt_client.connected_flag=True
            mqtt_client.disconnected_flag=False
            LOGGER.info("Client connected to MQTT Broker!")
        else:
            mqtt_client.bad_connection_flag=True
            mqtt_client.disconnected_flag=False
            LOGGER.critical(f"Failed to connect! return code '{rc}'")

    def on_disconnect(self, mqtt_client, userdata, rc):
        if rc == 0:
            LOGGER.info("Client disconnected from MQTT Broker!")
        else:
            LOGGER.critical(f"Disconnected! return code '{rc}'")
        mqtt_client.connected_flag=False
        mqtt_client.disconnected_flag=True

    def on_publish(self, mqtt_client, userdata, mid):
        LOGGER.debug(f"Data published, mid value:'{mid}'")

    def open(self, broker: str, port: int, client_id: str):
        """
        This opens the connection to the MQTT broker.

        Input Arguments:      
            broker (str)         :  The MQTT broker.
            port (int)           :  The port to be used.
            client_id (str)      :  The MQTT client ID.

        Return:
            mqtt_client          :  The MQTT client.
        """

        mqtt_client = client.Client(client_id)
        mqtt_client.on_connect = self.on_connect
        mqtt_client.on_disconnect = self.on_disconnect
        mqtt_client.on_publish = self.on_publish
        mqtt_client.loop_start()
        try:
            mqtt_client.connect(broker, port)
        except:
            LOGGER.error(f"Failed to connect to '{broker}:{port}'")
        return mqtt_client

    def close(self, mqtt_client):
        """
        This closes the connection to the MQTT broker.

        Input Arguments: 
            mqtt_client  :  The MQTT client.
        """

        mqtt_client.loop_stop()
        mqtt_client.disconnect()

    def send(self, mqtt_client, topic: str, value: str):
        """
        This is used to send data on the provided MQTT client.

        Input Arguments: 
            mqtt_client :  The MQTT client.
            topic (str) :  The complete path including the genivi name of the data.
            value (str) :  The timestamp and value data object.
        """
        
        result = self.mqtt_client.publish(topic, value)
        # result: [status, mid]
        status = result[0]
        mid = result[1]
        if status == 0:
            LOGGER.info(f"Send topic '{topic}' with value '{value}'; mid value: '{mid}'")
            print(f"Send topic '{topic}' with value '{value}'")
        else:
            LOGGER.error(f"Failed to send value to topic '{topic}'")
