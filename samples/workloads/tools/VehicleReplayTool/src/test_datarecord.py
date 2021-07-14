import unittest
from DataRecord import DataRecord


class TestDataRecord(unittest.TestCase):

    def setUp(self):
        """
        This runs before any test step
        """
        self.data_record_object = DataRecord('Speed', '60', '1606299014235.6177')

    def test_GetData(self):
        """
        This tests if the correct data is returned.
        """       
        self.assertEqual(self.data_record_object.name, 'Speed')
        self.assertEqual(self.data_record_object.value, '60')
        self.assertEqual(self.data_record_object.timestamp, '1606299014235.6177')
        
    def test_SetData(self):
        """
        This tests if data is set succesfully.
        """       
        self.data_record_object.name = 'Engine RPM'
        self.data_record_object.value = '75'
        self.data_record_object.timestamp = '1606307168.7246625'

        self.assertEqual(self.data_record_object.name, 'Engine RPM')
        self.assertEqual(self.data_record_object.value, '75')
        self.assertEqual(self.data_record_object.timestamp, '1606307168.7246625')
        self.assertNotEqual(self.data_record_object.name, 'Speed')
        self.assertNotEqual(self.data_record_object.value, '60')
        self.assertNotEqual(self.data_record_object.timestamp, '1606299014235.6177')        


if __name__ == '__main__':
    unittest.main()