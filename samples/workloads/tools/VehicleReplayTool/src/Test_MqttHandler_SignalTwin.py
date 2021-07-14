import time
import MqttHandler as mqtt_handler
import SignalTwin as signal_twin

mqtt_handler.mqtt_client.Client.connected_flag=False
mqtt_handler.mqtt_client.Client.bad_connection_flag=False

def run():
    counter = 0

    client = mqtt_handler.open()

    while not client.connected_flag and not client.bad_connection_flag:
        time.sleep(1)
    if client.bad_connection_flag:
        mqtt_handler.close(client)
        time.sleep(5)
        return

####
    signal_break = signal_twin.SignalTwin(client, "geniviBreak", "2020-11-12:10-49-01", "100N")
    signal_acc = signal_twin.SignalTwin(client, "geniviACC", "2020-11-12:10-49-01", "25m")

    while True:
        counter+=1
        time.sleep(3)
        if counter >= 15: break
        if counter == 5:
            signal_break.update(client, "2020-11-12:10-50-22", "123N")
            signal_acc.update(client, "2020-11-12:10-50-22", "45m")
        if counter == 10:
            signal_break.update(client, "2020-11-12:10-55-44", "12N")
            signal_acc.update(client, "2020-11-12:10-55-44", "100m")    
####

    mqtt_handler.close(client)
    time.sleep(5)

if __name__ == '__main__':
    run()