# Vehicle Replay Tool

The vehicle replay tool reads signals from an mdf4 file and publishes them to a MQTT broker. 
The mdf4 files are expected to be in a predetermined folder (/home/azureUser/mdf4/)
It looks for a docker container named 'mosquittomodule' based on an MQTT docker image.

## Prerequisites
1. A mdf4 folder '/home/azureUser/mdf4/' in the device.
1. IoT Edge modules (including mosquittomodule) running in the device.
1. Python3 installed.
1. Dependencies installed.

## Setup
Use WinSCP, Putty or any tool to connect to the device and upload files. The following commands are to be executed in the Iot Edge Device (VM).

### Installing Python3
Full tutorial in this [link](https://rajputankit22.medium.com/upgrade-python-2-7-to-3-6-and-3-7-in-ubuntu-97d2727bf911).

Our current LinuxVM device comes with python 2.7, in order to use the replay tool we need python3.

*Install ppa*  
`sudo add-apt-repository ppa:deadsnakes/ppa`

*Update Packages*  
`sudo apt-get update`

*Upgrade python 2.x to 3.x*  
Before install 3.7, we should have to install python 3.6 by running the following command.

`sudo apt-get install python3.6`  
`sudo apt-get install python3.7`

*PIP installation*  
`sudo apt install python3-pip`

Verify your install by running  
`python3 --version`

### Installing Dependencies
Now we install the vehicle replay tool dependencies.

```
sudo pip3 install paho-mqtt
sudo pip3 install asammdf
sudo apt-get install python3-pyqt5
sudo apt-get install python3-yaml
sudo pip3 install psutil pyqtgraph XlsxWriter xlwt xlrd
```

###  Upload files
Use your prefer SSH tool to move fileS between your local system and the Iot Edge Device (VM).

1. Upload the the /VehicleReplayTool/ directory to the /home/azureUser directory of the device.
1. Upload the startReplayTool.sh script at the root of this repository to the /home/azureuser directory.
1. (Optional) Upload the mdf4 recordings to /home/azureUser/mdf4/ directory if it doesn't exist.

### Allow files to execute
Open a terminal to the device and cd to /home/azureUser/

Run   
`chmod +x startReplayTool.sh`

## Usage
Once you have all elements in place you can start the replay tool by executing the following command.  

`./startReplayTool.sh <name-of-mdf4-file>`

## Author


