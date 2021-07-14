import logging
import configparser
import os.path
from typing import Optional


LOGGER=logging.getLogger(__name__)


class GeniviTranslator:
    """
    This reads and retrieves a predefined genivi name from a mapping file, using the original name as a key.
    """
    def __init__(self, filename: Optional[str]=None):
        """
        Genivi translator class constructor to initialize the object.

        Input Arguments: 
            filename (str)  :  The name of the configuration file containing the genivi name mapping.
        """
        self.filename = filename
        
        if filename is None:
            filename = "configuration.ini"
        
        #Get genivi mapping from config file
        try:   
            self.geniviMap = read(filename)
        except:
            LOGGER.error("Failed to read genivi mapping data from config file.")
            return

    def translate(self, name: str)->str:
        """
        This translates the original MDF4 channel name to a defined ginivi names from a mapping file.

        Input Arguments: 
            name (str)          :  The original name to be translated.

        Return:
            geniviName (str)    :  The translated genivi name.
        """

        geniviMap = self.geniviMap
        if name in geniviMap:
            geniviName = geniviMap[name]
        else:
            LOGGER.error(f"Signal '{name}' not translatable! Will be ignored!")
            geniviName = None

        return geniviName


def read(filename: str):
    """
    This reads the genivi mapping file.

    Input Arguments: 
        filename (str)  :  The name of the configuration file containing the genivi name mapping.

    Return:
        Map (dict)      :  The map of data source names and their corresponding genivi names.
    """

    config = configparser.ConfigParser()
    configPath = os.path.join(os.path.dirname(__file__), filename)
    config.optionxform = lambda option: option
    config.read(configPath, encoding='utf-8')

    return dict(config.items('translation'))