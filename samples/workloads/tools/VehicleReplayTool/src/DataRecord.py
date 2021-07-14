class DataRecord:
    """
    This class is used to hold the data record object data i.e. name, value and timestamp.
    """
    def __init__(self, name: str, value: str, timestamp: str):
        """
        Data record class constructor to initialize the object.

        Input Arguments: 
            name (str)      :  The original name of the data source.
            value (str)     :  The value corresponding with the name.
            timestamp (str) :  The timestamp (UNIX Epoch or UTC format. See configuration.ini) for when the value was measured.
        """

        self._name = name
        self._value = value
        self._timestamp = timestamp

    @property
    def name(self):
        """
        The property to get the original name of the data source.
        """
        return self._name

    @name.setter
    def name(self, newName):
        """
        This is used to set the original name of the data source.
        """
        self._name = newName

    @property
    def value(self):
        """
        The property to get the read value.
        """
        return self._value

    @value.setter
    def value(self, newValue):
        """
        This is called when self.value is updated.
        """
        self._value = newValue

    @property
    def timestamp(self):
        """
        The property to get the timestamp.
        """
        return self._timestamp

    @timestamp.setter
    def timestamp(self, newTimestamp):
        """
        This is called when self.timestamp is updated.
        """
        self._timestamp = newTimestamp

    def __repr__(self):
        """
        This is used to return the attributes of the class.
        """
        return "%s %s %s" %(self._name, self._value, self._timestamp)

