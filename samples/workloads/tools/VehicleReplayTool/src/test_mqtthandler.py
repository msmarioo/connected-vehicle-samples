import unittest
from paho.mqtt import client
from MqttHandler import MqttHandler


class TestMqttHandler(unittest.TestCase):

    def setUp(self):
        """
        This runs before any test step
        """
        self.mqttHandler = MqttHandler()
        self.mqttHandler.mqtt_client = client.Client('VehicleReplayComponent_LP1007')

    def test_MqttClient(self):
        """
        This tests if Mqtt client is set and retrieved succesfully.
        """
        mqclient = client.Client('VehicleReplayComponent_LP1007')
        self.mqttHandler.mqtt_client = mqclient
        self.assertEqual(self.mqttHandler.mqtt_client, mqclient)

    def test_Open(self):
        """
        This tests if the Mqtt handler is open.
        """
        self.assertIsInstance(self.mqttHandler.open('localhost', 1883, 'VehicleReplayComponent_LP1007'), client.Client)

    def test_Send(self):
        """
        This tests if the Mqtt handler sends data.
        """
        self.mqttHandler.mqtt_client = None
        self.assertRaises(AttributeError, lambda: self.mqttHandler.send(self.mqttHandler.mqtt_client, 'vehiclesignals/public/Drivetrain/InternalCombustionEngine/RPM', '1606500073267.164 882.5'))
        self.mqttHandler.mqtt_client = self.mqttHandler.open('localhost', 1883, 'VehicleReplayComponent_LP1007')
        self.assertIsNone(self.mqttHandler.send(self.mqttHandler.mqtt_client, 'vehiclesignals/public/Drivetrain/InternalCombustionEngine/RPM', '1606500073267.164 882.5'))
        self.assertRaises(ValueError, lambda: self.mqttHandler.send(self.mqttHandler.mqtt_client, '', ''))
        self.mqttHandler.close(self.mqttHandler.mqtt_client)   

    def test_Close(self):
        """
        This tests if the Mqtt handler is closed.
        """
        self.mqttHandler.mqtt_client = None
        self.assertRaises(AttributeError, lambda: self.mqttHandler.close(self.mqttHandler.mqtt_client))
        self.mqttHandler.mqtt_client = self.mqttHandler.open('localhost', 1883, 'VehicleReplayComponent_LP1007')
        self.assertEqual(self.mqttHandler.mqtt_client.disconnected_flag, False)
        self.mqttHandler.close(self.mqttHandler.mqtt_client)
        self.assertEqual(self.mqttHandler.mqtt_client.disconnected_flag, True)
       

if __name__ == '__main__':
    unittest.main()