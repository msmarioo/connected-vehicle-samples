from DataRecord import dataRecord
from DataSourceReader import mdf4Reader
import logging


#logging.basicConfig(filename='Test_DataSourceReader.log', level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

def processDataRecord(dataRec: dataRecord):
    """
    This represents the callback function prvoided to the data source reader.
    """
    LOGGER.info("processDataRecord - name:%s value:%s timestamp:%s",dataRec.name,dataRec.value,dataRec.timestamp)


def main():
    """
    This executes the main test. 
    """
    pass    

    LOGGER.info("Test mdf4Reader running... ")
    fileReader = mdf4Reader(processDataRecord)
    fileReader.read("/home/dsa/Schreibtisch/MDF4_Example_Passat/200812_094213.mf4") #200820_092105 #200812_094213
    LOGGER.info("Test mdf4Reader finished... ") 

if __name__ == "__main__":
    main()