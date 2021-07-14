import time
import pytz
import datetime
import logging
import SignalTwin
import DataRecord
from typing import Optional
from Mdf4Reader import Mdf4Reader
from MqttHandler import MqttHandler
from GeniviTranslator import GeniviTranslator

LOGGER=logging.getLogger(__name__)

mqttHandle = MqttHandler()
genTrans = GeniviTranslator()
signal_twin_map = dict()
nonTranslatable = []


def processDataRecord(data_record_object: DataRecord):
    """
    This is a callback function which is used to transfer a data record object from the MDF4 Reader component.

    Input Arguments: 
        data_record_object (DataRecord): The data record object.
    """

    if (data_record_object.name in signal_twin_map):
        LOGGER.debug(f"SignalTwin available for '{data_record_object.name}'.")
        tmp_signal_twin = signal_twin_map.get(data_record_object.name)
        tmp_signal_twin.update(mqttHandle, data_record_object.timestamp, data_record_object.value)
    else:
        LOGGER.debug(f"SignalTwin not available for'{data_record_object.name}'.")
        if data_record_object.name not in nonTranslatable:
            tmp_geniviName = genTrans.translate(data_record_object.name)
            if (tmp_geniviName is not None):
                tmp_signal_twin = SignalTwin.SignalTwin(mqttHandle, tmp_geniviName, data_record_object.timestamp, data_record_object.value)
                signal_twin_map.update({data_record_object.name : tmp_signal_twin})
            else:
                nonTranslatable.append(data_record_object.name)


def run(mdf4_filename: str, mqtt_client_id: str, broker: Optional[str]='localhost', port: Optional[int]=1883, log_filename: Optional[str]='VehicleReplayComponent.log', log_level: Optional[int]=logging.INFO):
    """
    This is responsible for executing the complete vehicle replay component.

    Input Arguments: 
        mdf4_filename (str)  :  The MDF4 file to read.
        mqtt_client_id (str) :  The MQTT client ID.
        broker (str)         :  The MQTT broker.
        port (int)           :  The port to be used.
        log_filename (str)   :  The name of the logfile were al logging is to be added.
        log_level (int)      :  The log level.
    """

    logging.basicConfig(filename=log_filename, level=log_level)

    LOGGER.info(f"{(datetime.datetime.now()).astimezone(pytz.timezone('UTC')).isoformat()} Starting vehicle replay of '{mdf4_filename}'")

    #Mqtt client connection timeout in seconds.
    timeout = 10

    #Clear global maps on each run.
    signal_twin_map.clear
    nonTranslatable.clear

    #Open an MQTT client connection.
    print("Trying to connect to MQTT Broker.")
    mqttHandle.mqtt_client = mqttHandle.open(broker, port, mqtt_client_id)

    #Wait until an MQTT client connection has been established. A timeout is defined.
    startTimer = time.time()
    while not mqttHandle.mqtt_client.connected_flag and not mqttHandle.mqtt_client.bad_connection_flag and (time.time() < (startTimer + timeout)):
        time.sleep(1)

    #If MQTT client connection is good then proceed to perform the vehicle replay operation.
    print("Connected to MQTT Broker.")
    if not mqttHandle.mqtt_client.bad_connection_flag:
        mdf4_reader = Mdf4Reader(processDataRecord)
        mdf4_reader.read(mdf4_filename)

    #Close MQTT client connection.
    mqttHandle.close(mqttHandle.mqtt_client)

    startTimer = time.time()
    while not mqttHandle.mqtt_client.disconnected_flag and (time.time() < (startTimer + timeout)):
        time.sleep(1)

    print("Disconnected from MQTT Broker")
    LOGGER.info(f"{(datetime.datetime.now()).astimezone(pytz.timezone('UTC')).isoformat()} Finished vehicle replay of '{mdf4_filename}'")
