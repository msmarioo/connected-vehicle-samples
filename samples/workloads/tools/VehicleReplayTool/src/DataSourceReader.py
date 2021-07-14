import abc


class DataSourceReader(abc.ABC):
    """
    This represents an abstract class for access to source files. 
    """
    def __init__(self, processDataRec):
        """
        Data source reader class constructor to initialize the object.

        Input Arguments: 
            processDataRec (str)  :  The process data record callback function.
        """
        self.processDataRec = processDataRec


    @classmethod
    def __subclasshook__(cls, subclass):
        """
        Class method to ensure the abstract class is implemented.
        """
        if subclass is DataSourceReader:
            if any("read" in B.__dict__ for B in subclass.__mro__):
                return True
        return NotImplemented


    @abc.abstractmethod
    def read(self, filename: str):
        """
        Extract data from MDF4 file.
        """
        raise NotImplementedError

