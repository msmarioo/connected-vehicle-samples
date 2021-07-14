import unittest
from paho.mqtt import client
from SignalTwin import SignalTwin
from MqttHandler import MqttHandler


class TestSignalTwin(unittest.TestCase):

    def setUp(self):
        """
        This runs before any test step.
        """
        self.mqttHandler = MqttHandler()
        self.mqttHandler.mqtt_client = self.mqttHandler.open('localhost', 1883, 'VehicleReplayComponent_LP1007')
        self.signalTwin = SignalTwin(self.mqttHandler, 'Drivetrain/InternalCombustionEngine/RPM', '16065000700000', '0')
        
    def tearDown(self):
        """
        This runs after all test steps.
        """
        self.mqttHandler.close(self.mqttHandler.mqtt_client)

    def test_SignalTwin(self):
        """
        This tests if a signal twin is send succesfully.
        """
        self.assertIsInstance(SignalTwin(self.mqttHandler, 'Drivetrain/InternalCombustionEngine/RPM', '1606500073390', '1500.5'), SignalTwin)

    def test_Update(self):
        """
        This tests if a signal twin updates data.
        """
        self.assertIsNone(self.signalTwin.update(self.mqttHandler, '1606500074400', '2000'))
        with self.assertRaises(AttributeError):
            self.signalTwin.update(None, 1, 2)

    def test_Serialize(self):
        """
        This tests if a signal twin serializes data.
        """
        self.assertEqual(self.signalTwin.serialize('2000', '1606500074400'), '1606500074400 2000')

    def test_SerializeJson(self):
        """
        This tests if a signal twin serializes data to json.
        """
        self.assertEqual(self.signalTwin.serializeJson('3000', '1606500085500'), '{"value": "3000", "timestamp": "1606500085500"}')


if __name__ == '__main__':
    unittest.main()