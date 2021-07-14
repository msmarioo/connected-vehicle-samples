import logging
import configparser
import os.path
import time
import pytz
import datetime
import DataRecord
import MqttHandler
from asammdf import MDF
from DataSourceReader import DataSourceReader


LOGGER = logging.getLogger(__name__)


class Mdf4Reader(DataSourceReader):
    """
    This handles the reading of the mdf files and adds the read data into a data record object.
    """ 
    def read(self, filename: str):
        """
        This reads mdf file versions 3.x and 4.x.

        Input Arguments: 
            filename (str)  :  The name of the MDF file to read.
        """

        #Read the MDF file
        LOGGER.debug(f"Trying to open MDF4 file '{filename}'.")
        print(f"Trying to open MDF4 file '{filename}'.")
        with MDF(filename) as mdfData:

            #Reads the configuration file 
            cfgINI = readConfig('configuration.ini')

            #Get timestamp conversion info from config file
            try:   
                unixEpoch = cfgINI.getboolean('date', 'UnixEpoch')
                currentTimeMode = cfgINI.getboolean('date', 'CurrentTimeMode')
                quickReplayMode = cfgINI.getboolean('date', 'QuickReplayMode')
            except:
                LOGGER.error("Failed to read 'UnixEpoch' or 'RealTimeMode' data from config file.")
                return

            if currentTimeMode:
                startTime = time.time()
            else:
                try:
                    setTime = cfgINI.get('date', 'timestamp')
                    startTime = ((datetime.datetime.strptime(setTime, '%Y-%m-%d %H:%M:%S.%f')).replace(tzinfo=datetime.timezone.utc)).timestamp()
                except:
                    LOGGER.error("Timestamp defined in config not valid! Should be format: Y-m-d H:M:S.f")
                    return

            #Filter MDF4 data.  
            mdfData = filterMdfData(mdfData, cfgINI)

            #Get all available signals.
            sigDataMap = getAllSignals(mdfData)

            #Get all avalaible samples from signals by ascending timestamp
            mapOfRecords, sortedMap = getAllSamplesByTimestamp(sigDataMap)

            if quickReplayMode:
                #Get all samples in timestamp ascending order and pass the data through a data record to the callback function.
                programmStartTime = time.time()
                for key in dict(sortedMap):
                    data = mapOfRecords[key]
                    smpl = data[0]
                    name = data[1]
                    try:
                        tmstmp = startTime + key[0]
                        if not unixEpoch:
                            tmstmp = (datetime.datetime.fromtimestamp(tmstmp)).astimezone(pytz.timezone('UTC')).isoformat()
                        else:
                            tmstmp = tmstmp * 1000
                    except:
                        LOGGER.error("Failed to set timestamp, UNIX Epoch '%s', Real time mode '%s'.", unixEpoch, currentTimeMode)
                    
                    self.processDataRec(DataRecord.DataRecord(name, smpl, str(tmstmp)))
                programmEndTime = time.time()
                totalTime = programmEndTime - programmStartTime
                print("TOTAL TIME: ", totalTime)
            else:
                #Get all samples in timestamp ascending order and pass the data through a data record to the callback function.
                prevTime = 0
                totalTime = 0
                currentRealTime = 0
                programmStartTime = time.time()
                for indx, key in enumerate(dict(sortedMap)):
                    data = mapOfRecords[key]
                    smpl = data[0]
                    name = data[1]
                    try:
                        currentRealTime = time.time()
                        tmstmp = startTime + key[0]
                        if indx == 0:
                            prevTime = key[0]
                        delayRealTime = currentRealTime - programmStartTime
                        delayTime = key[0] - prevTime
                        totalTime = totalTime + delayTime
                        if totalTime > delayRealTime:
                            time.sleep(totalTime - delayRealTime)
                        #LOGGER.info(f"Index '{indx}', Current time '{key[0]}', last time '{prevTime}', total time '{totalTime}', delay real time '{delayRealTime}'.")
                        #print(f"Index '{indx}', Current time '{key[0]}', last time '{prevTime}', total time '{totalTime}', delay real time '{delayRealTime}'.")
                        prevTime = key[0]
                        if not unixEpoch:
                            tmstmp = (datetime.datetime.fromtimestamp(tmstmp)).astimezone(pytz.timezone('UTC')).isoformat()
                        else:
                            tmstmp = tmstmp * 1000
                    except:
                        LOGGER.error("Failed to set timestamp, UNIX Epoch '%s', Real time mode '%s'.", unixEpoch, currentTimeMode)
                    
                    self.processDataRec(DataRecord.DataRecord(name, smpl, str(tmstmp)))

                programmEndTime = time.time()
                totalTime = programmEndTime - programmStartTime
                print("TOTAL TIME: ", totalTime)
        LOGGER.debug(f"MDF4 file closed: '{filename}'.")
        print(f"MDF4 file closed: '{filename}'.")


def readConfig(filename: str) -> configparser.ConfigParser:
    """
    This reads the 'configuration.ini' file and returns the contents.

    Input Arguments: 
        filename (str)                      :  The name of the configuration file containing the genivi name mapping.

    Return:
        config (configparser.ConfigParser)  :  The config parser containing the read data.
    """   
    config = configparser.ConfigParser()
    configPath = os.path.join(os.path.dirname(__file__), filename)
    config.optionxform = lambda option: option
    config.read(configPath, encoding='utf-8')

    return config


def getAllSignals(mdfData):
    """
    This function gets all available signals from the mdfData. It gets each individual signal and adds them to a map with signal name and 
    timestamps tuple as key.

    Input Arguments: 
        mdfData (MDF)       :  The MDF data.

    Return:
        sigDataMap (dict)   :  The map of data source names and their corresponding data.
    """

    signals = mdfData.iter_channels(skip_master=True, copy_master=True)
    sigDataMap = {}

    for sig in signals:
        sigName = sig.name
        sigTimestamp = tuple(sig.timestamps)
        if (sigName, sigTimestamp) in sigDataMap.items():
            LOGGER.info("Signal already exists! %s", sig.name)
        else:
            sigDataMap[(sigName, sigTimestamp)]  = ([sig.samples, sig.timestamps])
    
    return sigDataMap


def getAllSamplesByTimestamp(sigDataMap):
    """
    This gets all avalaible data samples by ascending timestamp from all signals and adds them to a map with timestamp and signal name tuple as key.
    Returns map of all sample records and map of all keys sorted by timestamp.

    Input Arguments: 
        sigDataMap (dict)     :  The map of data source names and their corresponding data.

    Return:
        mapOfRecords (dict)   :  The map of all available channel data and timestamped values.
        Keys (dict)           :  The map ascending timestamp dictionary keys.
    """

    mapOfRecords = {}     

    for tupleKey in sigDataMap:
        sigName = tupleKey[0]
        sigData = sigDataMap[tupleKey]

        for indx in range(0, len(sigData[0])):
            smpl = sigData[0][indx]
            tmstmp = sigData[1][indx]
            mapOfRecords[(tmstmp, sigName)] = ([smpl, sigName])
    
    return mapOfRecords, sorted(mapOfRecords.items(), key=lambda x:x[0])


def filterMdfData(mdfData, cfgINI):
    """
    This filters the mdf4 data with the given channel filter list.
    Returns reduced mdf4 data only with channels from channel filter list.

    Input Arguments: 
        mdfData (MDF)           :  The MDF data.
        cfgINI (ConfigParser)   :  The config parser with filter data.

    Return:
        mdfData (MDF)           :  The filtered MDF data.
    """

    #Get channels to filter from config file
    filterChannels = (''.join(cfgINI.get('channels', 'filter').split())).split(',')
    
    #Reduce filterChannels to only existing channels inside MDF4 file.
    #If a channel inside the filter doesn't exist inside the MDF4 file, the filter()-Function of asammdf returns with Exception
    #and the MDF4 file isn't reduced.
    tmp_filter_list = []
    available_filter_list = []
    if filterChannels is not None:
        for entry in filterChannels:
            tmp_filter_list.clear()
            #Perform channel filtering to get only the desired channels.
            try:
                tmp_filter_list.append(entry)
                mdfData.filter(tmp_filter_list)
                available_filter_list.append(entry)
                LOGGER.debug(f"The channel '{entry}' is available inside the MDF4 file! Channel will be used as filter!")
            except:
                LOGGER.warn(f"The channel '{entry}' is not available inside the MDF4 file! Channel will be removed from filter list!")
    LOGGER.info(f"The finally used filter list is'{available_filter_list}'")
    
    #Reduce MDF4 file based on the filtered channels.
    if filterChannels is not None:
        #Perform channel filtering to get only the desired channels.
        try:
            filteredMdfData = mdfData.filter(available_filter_list)
            mdfData = filteredMdfData
        except:
            LOGGER.error("Failed to filter channels as one of the channel is not in the MDF4 data. All channels are to be returned!")
    return mdfData  
