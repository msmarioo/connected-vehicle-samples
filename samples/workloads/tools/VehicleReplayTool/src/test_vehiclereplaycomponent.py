import unittest
import asammdf
import VehicleReplayComponent
import logging


class TestVehicleReplayComponent(unittest.TestCase):

    def setUp(self):
        """
        This runs before any test step.
        """

        self.broker = "localhost"
        self.port = 1883
        self.client_id = "VehicleReplayComponent_LP1007"
        self.mdf4_filename = "/home/dsa/Schreibtisch/MDF4_Example_Passat/200820_092105.mf4"
        self.log_filename = "VehicleReplayComponent.log"
        self.log_level = logging.INFO

    def test_Run(self):
        """
        This tests if the correct data is returned.
        """       
        self.assertIsNone(VehicleReplayComponent.run(self.mdf4_filename, self.client_id, self.broker, self.port, self.log_filename, self.log_level))
        self.assertRaises(asammdf.blocks.utils.MdfException, lambda: VehicleReplayComponent.run('no_mdf4_filename', self.client_id, self.broker, self.port, self.log_filename, self.log_level))


if __name__ == '__main__':
    unittest.main()