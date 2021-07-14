import unittest
import configparser
from GeniviTranslator import GeniviTranslator


class TestGeniviTranslator(unittest.TestCase):
    def test_Translate(self):
        """
        This tests if the genivi translator gets the correct mapping data.
        """

        genTrans = GeniviTranslator()
        
        self.assertNotEqual(genTrans.translate('Body_BodyType()'), 'Body/Body')
        self.assertEqual(genTrans.translate('Body_BodyType()'), 'Body/BodyType')
        self.assertEqual(genTrans.translate('ADAS_ObstacleSensor_DistanceToObject_FrontRight(Unit_Meter)'), 'ADAS/ObstacleSensor/DistanceToObject/FrontRight')
        self.assertIsNone(genTrans.translate('no_name'))
        

if __name__ == '__main__':
    unittest.main()