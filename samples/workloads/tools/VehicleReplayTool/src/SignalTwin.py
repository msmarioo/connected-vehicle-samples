from MqttHandler import MqttHandler
import json

class SignalTwin:
    """
    This class is used to hold the signal object data.
    """
    def __init__(self, mqttHandle: MqttHandler, geniviName: str, timestamp: str, value: str):
        """
        Signal twin class constructor to initialize the object.

        Input Arguments: 
            mqttHandle (MqttHandler)    :  The MQTT handler.
            geniviName (str)            :  The genivi name of the data source.
            timestamp (str)             :  The timestamp (UNIX Epoch or UTC format. See configuration.ini) for when the value was measured.
            value (str)                 :  The value corresponding to the genivi name.
        """

        self.__geniviName = "vehiclesignals/public/" + geniviName
        self.__value = value
        self.__timestamp = timestamp
        self.__mqttHandle = mqttHandle

        self.__mqttHandle.send(mqttHandle.mqtt_client, self.__geniviName, self.serialize(self.__value, self.__timestamp))

    def update(self, mqttHandle: MqttHandler, timestamp: str, value: str):
        """
        This is used to send the data using the provided MQTT handle.

        Input Arguments: 
            mqttHandle (MqttHandler)    :  The MQTT handler.
            timestamp (str)             :  The timestamp (UNIX Epoch or UTC format. See configuration.ini) for when the value was measured.
            value (str)                 :  The value associated with the timestamp.
        """

        if self.__value != value:
            self.__value = value
            self.__timestamp = timestamp
            self.__mqttHandle.send(mqttHandle.mqtt_client, self.__geniviName, self.serialize(self.__value, self.__timestamp))


    def serialize(self, value: str, timestamp: str) -> str:
        """
        This is used to serialize the provided data.

        Input Arguments: 
            value (str)      :  The value associated with the timestamp.
            timestamp (str)  :  The timestamp (UNIX Epoch or UTC format. See configuration.ini) for when the value was measured.
        
        Return:
            data (obj)       :  The serialized data.
        """

        return f"{timestamp} {value}"


    def serializeJson(self, value: str, timestamp: str) -> str:
        """
        This is used to serialize the provided data to json format.

        Input Arguments: 
            value (str)      :  The value associated with the timestamp.
            timestamp (str)  :  The timestamp (UNIX Epoch or UTC format. See configuration.ini) for when the value was measured.

        Return:
            json data (str)  :  The serialized json data.
        """

        data_set = {"value": value, "timestamp": timestamp}
        return json.dumps(data_set)