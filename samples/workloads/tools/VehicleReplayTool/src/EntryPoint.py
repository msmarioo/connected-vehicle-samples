import VehicleReplayComponent as vrc
import logging
import sys
import os

broker = sys.argv[1]
port = 1883

client_id = "VehicleReplayComponent_LP1007"

#mdf4_folder = "/home/azureUser/mdf4/"
mdf4_folder = os.getcwd() +"/"

mdf4_filename = mdf4_folder + sys.argv[2] 
#mdf4_filename = "/home/azureUser/mdf4/normaldriving_201125_104306.mf4"

log_filename = "VehicleReplayComponent.log"
log_level = logging.INFO

if __name__ == '__main__':
    vrc.run(mdf4_filename, client_id, broker, port, log_filename, log_level)
