# **Introduction**
In this step-by-step workshop you will connect modbus device(s) with IoT Central application via an IoT Edge Gateway device

## **Scenarios**
modbus devices connected to IoT Central via IoT Edge Gateway using the following patterns:
- [Opaque](#opaque-pattern): In this pattern IoT Edge Gateway is only device known in the cloud
- [Lucid](#lucid-pattern): here IoT Edge Gateway (modbus client) and leaf devices (modbus server) known in the cloud

&nbsp;
## Opaque pattern
In this pattern IoT Edge Gateway is only device known in the cloud. All capabilities are part of that one device.

1. Setup and run [modbus Server Simulator](https://github.com/iot-for-all/iotc-modbus-iotedge-gateway/tree/main/modbus-server-sim/README.md#to-setup-simulator)
2. Build and publish [modbus opaque custom IoT Edge module](https://github.com/iot-for-all/iotc-modbus-iotedge-gateway/tree/main/edge-gateway-modules/modbus-opaque/README.md)
3. Setup [IoT Central application](iotcentral.md)
4. Deploy an [IoT Edge enabled Linux VM](edgevm.md)
5. Confim that IoT Edge device status shows _"Provisioned"_ in your IoT Central application
    
    [<img src=./assets/02_device_status.png heigth="60%" width="60%">](/assets/02_device_status.png)
6. [IoT Edge Gateway commands to handle modbus CRUD](commands.md)
7. Connect to your modbus tcp server using the following _"model-less command"_ command as mentioned in step 6:
    - Method name: **connect**, Module name: **modbus_crud**, Payload: **[{"serverId": "modbus01", "host": "<YOUR_MODBUS_SERVER_VM_IPADDRESS>", "port": <YOUR_MODBUS_SERVER_PORT>}]**
8. click on IoT Edge Gateway device and select _"Raw data"_ tab and verify the telemetry is flowing

    [<img src=./assets/03_device_rawdata.png heigth="60%" width="60%">](/assets/03_device_rawdata.png)
    

&nbsp;
## Lucid pattern
In this pattern IoT Edge Gateway (modbus client) and leaf devices (modbus server) known in the cloud.

1. Setup and run [modbus Server Simulator](https://github.com/iot-for-all/iotc-modbus-iotedge-gateway/tree/main/modbus-server-sim/README.md#to-setup-simulator)
2. Build and publish [modbus lucid custom IoT Edge modules](https://github.com/iot-for-all/iotc-modbus-iotedge-gateway/tree/main/edge-gateway-modules/modbus-lucid/README.md)
3. Setup [IoT Central application](iotcentral.md)
4. Deploy an [IoT Edge enabled Linux VM](edgevm.md)
5. Confim that IoT Edge device status shows _"Provisioned"_ in your IoT Central application

    [<img src=./assets/03_device_status.png heigth="60%" width="60%">](/assets/03_device_status.png)
6. [IoT Edge Gateway commands to handle modbus CRUD](commands.md)
7. Connect to your modbus server using the following _"model-less command"_ command as mentioned in step 6:
    - Method name: **connect**, Module name: **modbus_crud**, Payload: **[{"serverId": "modbus01", "host": "<YOUR_MODBUS_SERVER_VM_IPADDRESS>", "port": <YOUR_MODBUS_SERVER_PORT>}]**
8. Confim that IoT Edge device child device **modbus01** status shows _"Provisioned"_ in your IoT Central application

    [<img src=./assets/04_device_status.png heigth="60%" width="60%">](/assets/04_device_status.png)
9. Confim that IoT Edge device has **modbus01** device as its child device in your IoT Central application

    [<img src=./assets/04_gateway_child_device.png heigth="60%" width="60%">](/assets/04_gateway_child_device.png)
10. Click on **modbus01** device and select _"Raw data"_ tab and verify the telemetry is flowing

    [<img src=./assets/04_device_rawdata.png heigth="60%" width="60%">](/assets/04_device_rawdata.png)