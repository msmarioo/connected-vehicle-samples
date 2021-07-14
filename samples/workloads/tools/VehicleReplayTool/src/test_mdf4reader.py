import unittest
import asammdf
from VehicleReplayComponent import processDataRecord
from Mdf4Reader import Mdf4Reader, getAllSignals


class TestMdf4Reader(unittest.TestCase):

    def setUp(self):
        """
        This runs before any test step
        """
        self.fileReader = Mdf4Reader(processDataRecord)
        self.mdfData = asammdf.MDF('/home/dsa/Schreibtisch/MDF4_Example_Passat/harshdriving_201119_073606.mf4')

    def test_Read(self):
        """
        This tests if the mdf4 file data is read.
        """
        self.assertIsNone(self.fileReader.read("/home/dsa/Schreibtisch/MDF4_Example_Passat/harshdriving_201119_073606.mf4"))
        self.assertRaises(asammdf.blocks.utils.MdfException, lambda: self.fileReader.read("/home/dsa/Schreibtisch/MDF4_Example_Passat/200812_094299.mf4"))

    def test_GetAllSignals(self):
        """
        This tests if mdf4 signals are read.
        """
        sigs = getAllSignals(self.mdfData)
        self.assertEqual(len(sigs), 189)


if __name__ == '__main__':
    unittest.main()