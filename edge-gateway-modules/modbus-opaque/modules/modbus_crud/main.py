# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for
# full license information.
from asyncio.tasks import sleep
import json
import time
import random
import base64
import os
import sys
sys.path.insert(0, "..")
import asyncio
import cryptocode

from datetime import datetime
from azure.iot.device import Message, MethodResponse
from azure.iot.device.aio import IoTHubModuleClient

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.compat import iteritems
from collections import OrderedDict

try:  
    # python 3.4
    from asyncio import JoinableQueue as Queue
except:  
    # python 3.5
    from asyncio import Queue

ORDER_DICT = {
    "<": "LITTLE",
    ">": "BIG"
}

# global counters
TWIN_CALLBACKS = 0
RECEIVED_MESSAGES = 0
PAUSE_IN_SECOND = 15
PUBLISH_INTERVAL_MS = 500
OPAQUE = False

server_dict = {}
node_list = ["vibration_3d", "error_count", "widget_count", "spindle_rpm", "coolant_temp"]
startTimer = time.process_time()
publishTimer = time.process_time()


def message_handler(message):
    print("Message received on INPUT 1")
    print("the data in the message received was ")
    print(message.data)
    print("custom properties are")
    print(message.custom_properties)


async def get_twin(module_client):
    # get the twin
    twin = await module_client.get_twin()
    print("Twin document:")
    print("{}".format(twin))
    return twin


# define behavior for receiving a twin patch
async def twin_patch_handler(patch):
    print("-" * 60)
    print("the data in the desired properties patch was: {}".format(patch))
    print("set default publishing interval in desired properties")
    # send new reported properties
    if 'publishInterval' in patch:
        print("Reporting desired changes {}".format(patch))
        reported = { "publishInterval": patch['publishInterval'] }
        await module_client.patch_twin_reported_properties(reported)
        print("Reported twin patch")
        pubInterval = patch["publishInterval"]
        if len(server_dict) > 0:
            for k, config in server_dict.items():
                if config == None:
                    pass
                else:
                    print("changing publishing interval to %d ms" % pubInterval)
                    await config.publish_interval_update(pubInterval)
    print("Patched twin")
    print("-" * 60)


# Define behavior for handling methods
async def method_request_handler(method_request):
    print("-" * 60)
    print("Method request payload received: {}".format(method_request.payload))
    # Determine how to respond to the method request based on the method name
    if method_request.name == "connect":
        await connect_method_handler(method_request)
    elif method_request.name == "disconnect":
        await disconnect_method_handler(method_request)
    elif method_request.name == "config":
        await config_method_handler(method_request)
    elif method_request.name == "filter":
        await filter_method_handler(method_request)
    elif method_request.name == "pubInterval":
        await pubInterval_method_handler(method_request)
    else:
        payload = {"result": False, "data": "unknown method"}  # set response payload
        status = 400  # set return status code
        print("executed unknown method: " + method_request.name)

        # Send the response
        method_response = MethodResponse.create_from_method_request(method_request, status, payload)
        await module_client.send_method_response(method_response)
    print("-" * 60)


async def connect_method_handler(method_request):
    result = True
    data = {}

    for item in method_request.payload:
        reported_properties = {}
        reported_properties["modbus"] = {}
        serverId = item["serverId"]
        host = item["host"]
        port = item["port"]
        url = "{}:{}".format(host, port)
        
        value = {}
        print("item: {}".format(item))
        for att, val in item.items():
            if val != "serverId":
                value.update({ att: val })
        
        if  value.get("publishInterval") == None:
            value.update({"publishInterval": PUBLISH_INTERVAL_MS})
            
        print("connect_method_handler: {}: {}".format(serverId, value))
        secrets = value.get("secrets")
        if secrets != None:
            value["secrets"] = cryptocode.encrypt(secrets, url.lower())
        reported_properties["modbus"].update({ serverId: value })
        print("Setting reported modbus to {}".format(reported_properties["modbus"]))
        try:
            await module_client.patch_twin_reported_properties(reported_properties)
            data.update({ serverId: { "status": 201, "data": "Scheduled connection to modbus server '{}'".format(serverId)}})
        except:
            data.update({ serverId: { "status": 400, "data": "Failed to schedule connection to modbus server '{}'".format(serverId)}})
    
    payload = {"result": result, "data": data}  # set response payload
    status = 207
    
    # Send the response
    method_response = MethodResponse.create_from_method_request(method_request, status, payload)
    await module_client.send_method_response(method_response)
    print("executed connect")


async def disconnect_method_handler(method_request):
    if len(server_dict) == 0:
        print("Found no modbus client to disconnect")
        payload = {"result": False, "data": "Found no client to disconnect"}
        status = 404
        method_response = MethodResponse.create_from_method_request(method_request, status, payload)
        await module_client.send_method_response(method_response)
        print("executed disconnect")
        return

    result = True
    data = {}

    for item in method_request.payload:
        reported_properties = {}
        reported_properties["modbus"] = {}
        serverId = item["serverId"]
        print("Removing modbus server config %s from reported properties" % serverId)
        reported_properties["modbus"].update({ serverId: None })
        await module_client.patch_twin_reported_properties(reported_properties)
        
        config = server_dict.get(serverId)
        if config == None:
            print("Found no config to apply disconnect for %s" % serverId)
            data.update({config.serverId: { "status": 404, "data": "Found no config to apply disconnect for '{}'".format(serverId)}})
            result = False
        else:
            print("disconnect server: %s" % serverId)
            config.disconnect()
            server_dict.pop(serverId, None)
            data.update({config.serverId: { "status": 200, "data": "Disconnect modbus server '{}'".format(serverId)}})
    
    payload = {"result": result, "data": data}  # set response payload
    status = 207

    # Send the response
    method_response = MethodResponse.create_from_method_request(method_request, status, payload)
    await module_client.send_method_response(method_response)
    print("executed disconnect")


async def config_method_handler(method_request):
    result = True
    data = {}
    config_array = {}
    config_array["nodes"] = []
    if len(server_dict) <= 0:
        print("Found no client to retrieve the config")
        payload = {"result": False, "data": "config"}
        status = 404
        method_response = MethodResponse.create_from_method_request(method_request, status, payload)
        await module_client.send_method_response(method_response)
        print("executed config")
        return

    for key, config in server_dict.items():
        if config == None:
            data.update({config.serverId: { "status": 404, "data": "No config found for OPC UA server '{}'".format(key)}})
            result = False
        else:
            print("Processing server config %s" % config.serverId)
            print("Variable nodes: {}".format(config.variable_nodes))
            rootNode = "Widget_Maker_1000"
            config_array["nodes"].append({"serverId": config.serverId, "host": config.host, "port": config.port, "rootNode": rootNode, "nodes": config.variable_nodes})
            data.update({config.serverId: { "status": 200, "data": "Got OPC UA server '{}' config".format(key)}})
    
    payload = {"result": result, "data": data}
    status = 207
    
    print("      {}".format(config_array))
    msg = Message("{}".format(config_array))
    msg.content_type = "application/json"
    msg.content_encoding = "utf-8"
    msg.custom_properties = dict([("iotc_message_type", "modbus_server_config")])
    try:
        await module_client.send_message_to_output(msg, "output1")
        print("completed sending config message")
    except Exception as e:
        print("Failed to send config message: {}".format(e))
        payload = {"result": False, "data": data}
        status = 400

    # Send the response
    method_response = MethodResponse.create_from_method_request(method_request, status, payload)
    await module_client.send_method_response(method_response)
    print("executed config")


async def filter_method_handler(method_request):
    if len(server_dict) == 0:
        print("Found no client to apply the filter")
        payload = {"result": False, "data": "filter"}
        status = 404
        method_response = MethodResponse.create_from_method_request(method_request, status, payload)
        await module_client.send_method_response(method_response)
        print("executed filter")
        return
    
    result = True
    data = {}
    reported_properties = {}
    reported_properties["modbus"] = {}
    for item in method_request.payload:
        serverId = item["serverId"]
        config = server_dict.get(serverId)
        if config == None:
            print("Found no config to apply filter for %s" % serverId)
            data.update({config.serverId: { "status": 404, "data": "Found no config to apply filter for '{}'".format(serverId)}})
            result = False
        else:
            print("Applying filter to %s" % serverId)
            filter = item.get("filter")
            if filter == None:
                print("Found no config to apply filter for %s" % serverId)
                data.update({config.serverId: { "status": 400, "data": "Missing filter for '{}'".format(serverId)}})
                result = False
                    
            pubInterval = item.get("publishInterval")
            if pubInterval == None:
                pubInterval = config.publish_interval()
                print("filter_method_handler: Using config publish interval: %d" % pubInterval)

            action = filter["action"]
            if action == "reset":
                print("Reseting nodeid filter on server %s" % config.serverId)
                config.reset_filter()
                
                entry = { "host": config.host, "port": config.port, "publishInterval": pubInterval, "filter": None }
                reported_properties["modbus"].update({ config.serverId: entry })
                print("Removing reported modbus filter section {}".format(entry))
                data.update({config.serverId: { "status": 200, "data": "Reseted filter on server '{}'".format(serverId)}})
            else:
                print("Apply filter mode %s" % action)
                nodes = filter.get("nodes")
                if nodes == None or len(nodes) <= 0:
                    print("Cannot apply empty filter for %s" % serverId)
                    continue
            
                print("Filter nodes: {}".format(nodes))
                config.apply_filter({ "action": action, "nodes": nodes})
            
                entry = { "host": config.host, "port": config.port, "publishInterval": pubInterval, "filter": { "action": action, "nodes": nodes} }
                reported_properties["modbus"].update({ config.serverId: entry })
                print("Setting reported modbus to {}".format(entry))
                data.update({config.serverId: { "status": 200, "data": "Applied filter on server '{}'".format(serverId)}})
    
    if len(reported_properties["modbus"]) > 0:
        print("Set the state in reported properties")
        await module_client.patch_twin_reported_properties(reported_properties)
    
    payload = {"result": result, "data": data}  # set response payload
    status = 207
    
    # Send the response
    method_response = MethodResponse.create_from_method_request(method_request, status, payload)
    await module_client.send_method_response(method_response)
    print("executed filter")


async def pubInterval_method_handler(method_request):
    if len(server_dict) == 0:
        print("Found no client to apply publish interval")
        payload = {"result": False, "data": "pubInterval"}
        status = 404
        method_response = MethodResponse.create_from_method_request(method_request, status, payload)
        await module_client.send_method_response(method_response)
        print("executed pubInterval")
        return

    result = True
    data = {}
    reported_properties = {}
    reported_properties["modbus"] = {}
    for item in method_request.payload:
        serverId = item["serverId"]
        config = server_dict.get(serverId)
        if config == None:
            print("Found no config to apply publish interval for %s" % serverId)
            data.update({config.serverId: { "status": 404, "data": "Found no config to apply publish interval for '{}'".format(serverId)}})
            result = False
        else:
            print("Applying publish interval to %s" % serverId)        
            pubInterval = item.get("publishInterval")
            if pubInterval == None:
                pubInterval = config.publish_interval()
                print("pubInterval_method_handler: Using config publish interval: %d" % pubInterval)

            print("changing publishing interval for server %s to %d ms" % (serverId, pubInterval))
            config.publish_interval_update(pubInterval)
            
            entry = { "publishInterval": pubInterval }
            reported_properties["modbus"].update({ config.serverId: entry })
            print("Setting reported modbus to {}".format(entry))
            data.update({config.serverId: { "status": 200, "data": "Changed publish interval on server '{}'".format(serverId)}})
    
    if len(reported_properties["modbus"]) > 0:
        print("Set the state in reported properties")
        await module_client.patch_twin_reported_properties(reported_properties)
    
    payload = {"result": result, "data": data}  # set response payload
    status = 207
    
    # Send the response
    method_response = MethodResponse.create_from_method_request(method_request, status, payload)
    await module_client.send_method_response(method_response)
    print("executed pubInterval")


class ModbusConfig(object):
    def __init__(self, serverId, host, port, modbus_client, variable_nodes) -> None:
        self.serverId = serverId
        self.secrets = None
        self.cert = None
        self.certKey = None
        self.modelId = None
        self.host = host
        self.port = port
        self.modbus_client = modbus_client
        self.variable_nodes = variable_nodes
        self.incoming_queue = []
        self.publishInterval = None
        self.filtered_nodes = []
        self.registrationId = serverId
        if len(variable_nodes) > 0:
            for variable_node in variable_nodes:
                self.filtered_nodes.append(variable_node)
    
    def is_client_connected(self):
        connected = False
        if self.modbus_client:
            connected = self.modbus_client.is_socket_open()
        return connected
    
    def disconnect(self):
        if self.modbus_client:
            self.modbus_client.close()
      
    def datachange_process(self, name, value):
        self.incoming_queue.append({"registrationId": self.registrationId, "secrets": self.secrets, "cert": self.cert, "certKey": self.certKey, "modelId": self.modelId, "source_time_stamp": datetime.utcnow().strftime("%m/%d/%Y, %H:%M:%S"), "name": name, "value": value})
    
    def publish_interval(self):
        if self.publish_interval == None:
            return PUBLISH_INTERVAL_MS
        return self.publishInterval
    
    def publish_interval_update(self, publishInterval):
        if self.publishInterval != publishInterval:
            self.publishInterval = publishInterval
            self.apply_filter({ "action": "include", "nodes": self.filtered_nodes })

    def apply_filter(self, filter):
        action = filter.get("action")
        nodes = filter.get("nodes")
        if action == None:
            print("Filter 'action' cannot be empty . . .")
            return
        
        if action == 'reset':
            return self.apply_filter()

        if nodes != None and len(nodes) > 0:
            filteredNodes = []
            for variable_node in self.variable_nodes:
                if variable_node in nodes:
                    if  action == 'include':
                        filteredNodes.append(variable_node)
                else:
                    if action == 'exclude':
                        filteredNodes.append(variable_node)

            self.filtered_nodes = filteredNodes
    
    def reset_filter(self):
        filteredNodes = []
        for variable_node in self.variable_nodes:
            filteredNodes.append(variable_node)
        
        self.filtered_nodes = filteredNodes
        
    def is_filtered(self, name):
        if name in self.filtered_nodes:
            return False
        return True
        


async def send_to_upstream(data, module_client, customProperties):
    if module_client and module_client.connected:
        name = f'"{data["name"]}"'
        timestamp = f'"{data["source_time_stamp"]}"'
        valueKey = "value"

        if type(data["value"]) == int or type(data["value"]) == float or type(data["value"]) == bool:
            value = data["value"]
        elif str(type(data["value"])) == "string":
            value = f'"{data["value"]}"'
        elif str(type(data["value"])).startswith("<class"):
            value = data["value"]
            valueKey = "valueObject"

        payload = '{ "name": %s, "source_time_stamp": %s, %s: %s}' % (name, timestamp, valueKey, value)

        print("      %s" % (payload))
        msg = Message(payload)
        msg.content_type = "application/json"
        msg.content_encoding = "utf-8"

        for k, v in customProperties.items():
            if v != None:
                msg.custom_properties[k] = v

        try:
            await module_client.send_message_to_output(msg, "output1")
            print("completed sending message")
        except asyncio.TimeoutError:
            print("call to send message timed out")


async def incoming_queue_processor(module_client):
    global startTimer
    global publishTimer
    startTimer = time.process_time()
    publishTimer = time.process_time()
    while True:
        try:
            if len(server_dict) > 0:
                for key, config in server_dict.items():
                    if config == None:
                        print("incoming_queue_processor: cannot find config for %s" % key)
                        continue
                        
                    queue = config.incoming_queue
                    if queue == None:
                        print("incoming_queue_processor: queue is not defined for %s" % key)
                        continue
                    
                    if time.process_time() - publishTimer > config.publish_interval() / 1000:
                        publishTimer = time.process_time()
                        if config.is_client_connected():
                            try:
                                print("-" * 60)
                                print("Simulate by writing registers to modbus server")
                                run_sim(config)
                                print("-" * 60)
                            except Exception as e:
                                print("incoming_queue_processor: exception thrown running simulation for %s" % key)
                                print("Exception: {}".format(e))
                        else:
                            print("incoming_queue_processor: modebus client is not connected for %s" % key)

                    if len(queue) > 0:
                        data = queue.pop(0)
                        registrationId = data.get("registrationId")
                        properties = {}
                        properties["registrationId"] = registrationId
                        properties["modelId"] = data.get("modelId")
                        if data.get("cert") != None:
                            properties["cert"] = data["cert"]
                        if data.get("certKey") != None:
                            properties["certKey"] = data["certKey"]
                            
                        print("incoming_queue_processor: CustomeProperties: {}".format(properties))
                        # Adding secrets to properties to not to pollute the logs with secrets
                        properties["secrets"] = data.get("secrets")

                        if registrationId == None:
                            print("===>> [{}] {} - {}".format(data["source_time_stamp"], data["name"], data["value"]))
                        else:
                            print("===>> {}: [{}] {} - {}".format(registrationId, data["source_time_stamp"], data["name"], data["value"]))
                            
                        await send_to_upstream(data, module_client, properties)
                
            if time.process_time() - startTimer > 15:
                print("-" * 60)
                startTimer = time.process_time()
                await ping(module_client)
                print("-" * 60)
                
        except Exception as e:
            print("incoming_queue_processor: Processing incoming queue failed with exception: {}".format(e))
            pass


def run_sim(config):
    # queue = config.incoming_queue
    client = config.modbus_client
    # if queue == None or config.is_client_connected():
    #     return
    
    combos = [(wo, bo) for wo in [Endian.Big, Endian.Little] for bo in [Endian.Big, Endian.Little]]
    for wo, bo in combos:
        # print("-" * 60)
        # print("Word Order: {}".format(ORDER_DICT[wo]))
        # print("Byte Order: {}".format(ORDER_DICT[bo]))
        # print()
        builder = BinaryPayloadBuilder(byteorder=bo, wordorder=wo)

        obj = {
            "x": random.random(),
            "y": random.random(),
            "z": random.random()
        }
            
        objStr = json.dumps(obj)
        builder.add_string(objStr)
        builder.add_8bit_uint(random.randint(0, 255))
        builder.add_16bit_uint(random.randint(0, 65535))
        builder.add_32bit_float(random.uniform(0, 7038))
        builder.add_32bit_float(random.uniform(-3.39*1038, 3.39*1038))
        payload = builder.to_registers()
        # print("-" * 60)
        # print("Writing Registers")
        # print("-" * 60)
        # print(payload)
        # print("\n")
        payload = builder.build()
        address = 0
        
        # write encoded binary string
        client.write_registers(address, payload, skip_encode=True, unit=1)

        address = 0x0
        count = len(payload)
        result = client.read_holding_registers(address, count,  unit=1)
        # print("-" * 60)
        # print("Registers")
        # print("-" * 60)
        # print(result.registers)
        # print("\n")

        if result.registers:
            decoder = BinaryPayloadDecoder.fromRegisters(result.registers, byteorder=bo, wordorder=wo)

            assert decoder._byteorder == builder._byteorder, \
                "Inconsistent byteorder between BinaryPayloadBuilder & BinaryPayloadDecoder"

            assert decoder._wordorder == builder._wordorder, \
                "Inconsistent wordorder between BinaryPayloadBuilder & BinaryPayloadDecoder"

            decoded = OrderedDict([
                ('vibration_3d', decoder.decode_string(len(objStr))),
                ('error_count', decoder.decode_8bit_uint()),
                ('widget_count', decoder.decode_16bit_uint()),
                ('spindle_rpm', decoder.decode_32bit_float()),
                ('coolant_temp', decoder.decode_32bit_float()),
            ])

            # print("-" * 60)
            # print("Decoded Data")
            # print("-" * 60)
            for name, value in iteritems(decoded):
                if config.is_filtered(name):
                    print("Skipping filtered node: '%s'" % name)
                    continue
                
                val = json.loads(value.decode("UTF-8")) if isinstance(value, bytes) else value
                config.datachange_process(name, val)
                # print("%s\t" % name, hex(value) if isinstance(value, int) else value)
                # print("%s\t" % name, json.loads(value.decode("UTF-8")) if isinstance(value, bytes) else value)
                print("%s\t" % name,  val)

    
def modbus_client_connect(value, serverId):
    global root_node_dict
    connected = False
    server_host = value.get("host")
    server_port = value.get("port")
    modelId = value.get("modelId")
    pubInterval = value.get("publishInterval")
    if pubInterval == None:
        pubInterval = PUBLISH_INTERVAL_MS

    filter = value.get("filter")
    if filter != None and filter.get("action") == None:
        print("Skipping filter since required 'filter.action' is missing")
        filter = None
        
    print ( "modbus_client_connect: %s:%d" % (server_host, server_port))
    modbus_client = ModbusClient(server_host, server_port)
    
    try:
        modbus_client.connect()
        connected = modbus_client.is_socket_open()
        print("connected to modbus server")
    except Exception as e:
        print("Connection to modbus server failed with exception: {}".format(e))
        return False

    # walk the objects and variable tree
    variable_nodes = []
    for node in node_list:
        variable_nodes.append(node)

    config = ModbusConfig(serverId, server_host, server_port, modbus_client, variable_nodes)
    server_dict.update({serverId: config})
    
    secrets = value.get("secrets")
    if secrets != None:
        b64Decoded = base64.b64decode(secrets.encode('utf-8'))
        secretsString = b64Decoded.decode("utf-8")
        # secrets = eval(secretsString)
        secretsJson = json.loads(secretsString)
        print("modbus_client_connect: secrets: {}".format(secretsJson))
        
        clientSecrets = secretsJson.get("client")
        if clientSecrets != None:
            clientSecretsType = clientSecrets.get("type")
            if clientSecretsType == "cert":
                cert = clientSecrets.get("cert")
                if cert != None:
                    publicCert = cert.get("public")
                    if publicCert != None:
                        publicFileName = "{}_public.pem".format(serverId)
                        publicFile = open("/certs/{}".format(publicFileName), "w+")
                        publicFile.write(publicCert)
                        publicFile.close()
                        config.cert = "/certs/{}".format(publicFileName)
                        print("public cert abspath: {}".format(config.cert))
                        
                    privateCert = cert.get("private")
                    if privateCert != None:
                        privateFileName = "{}_private.pem".format(serverId)
                        privateFile = open("/certs/{}".format(privateFileName), "w+")
                        privateFile.write(privateCert)
                        privateFile.close()
                        config.certKey = "/certs/{}".format(privateFileName)
                        print("private cert abspath: {}".format(config.certKey))

        config.secrets = secrets
        print("modbus_client_connect: secrets: {}".format(secrets))
    
    config.modelId = modelId
    config.publishInterval = pubInterval
    config.registrationId = None if OPAQUE else serverId
    
    if filter == None:
        config.publishInterval = pubInterval
    else:
        config.apply_filter(filter)
        
    return connected


async def ping(module_client):
    try:
        print("Ping modbus servers . . .")
        twin = await get_twin(module_client)
        print("twin: {}".format(twin))
        reported = twin["reported"]

        if 'modbus' in reported and len(reported['modbus']) > 0:
            for key, value in reported['modbus'].items():
                print("Found configuration in twin.reported for modbus server %s" % key)
                config = server_dict.get(key)
                if config == None:
                    print("Not found modbus server '%s' in cache . . ." % key)
                    secrets = value.get("secrets")
                    if secrets != None:
                        url = "{}:{}".format(value.get("host"), value.get("port"))
                        value["secrets"] = cryptocode.decrypt(secrets, url.lower())
                        print("secrets: %s" % value["secrets"])
                    
                    print("Connecting to modbus server %s" % key)
                    modbus_client_connect(value, key)
                else:
                    print("Checking connection for modbus server %s" % key)
                    if not config.is_client_connected():
                        print("Trying to re-connect to modbus server %s" % key)
                        secrets = value.get("secrets")
                        if secrets != None:
                            url = "{}:{}".format(value.get("host"), value.get("port"))
                            value["secrets"] = cryptocode.decrypt(secrets, url.lower())
                        
                        modbus_client_connect(value, key)
    
    except Exception as ex:
        print("Ping failed with eception {}".format(ex))
        pass
        
    print("Ping modbus servers completed . . .")


async def main():
    try:
        if not sys.version >= "3.5.3":
            raise Exception( "The sample requires python 3.5.3+. Current version of Python: %s" % sys.version )
        print ( "IotEdge module Client for Processing modbus protocols" )

        # The client object is used to interact with your Azure IoT hub.
        global module_client
        global PUBLISH_INTERVAL_MS
        global OPAQUE
        
        module_client = IoTHubModuleClient.create_from_edge_environment()

        # connect the client.
        await module_client.connect()
        
        if os.getenv("opaque", "false") == "true":
            OPAQUE = True
            
        print("Opaque: {}".format(OPAQUE))
        
        twin = await get_twin(module_client)
        desired = twin["desired"]
        print("Twin properties desired:")
        print("{}".format(desired))
        if 'publishInterval' in desired and desired['publishInterval'] > 10:
            PUBLISH_INTERVAL_MS = desired['publishInterval']
                
        # set the message handler on the module
        module_client.on_message_received = message_handler
        
        # set the twin patch handler on the module
        module_client.on_twin_desired_properties_patch_received = twin_patch_handler
        
        # Set the method request handler on the module
        module_client.on_method_request_received = method_request_handler
       
        tasks = []
        tasks.append(asyncio.create_task(incoming_queue_processor(module_client)))
        await asyncio.gather(*tasks)

        print ( "Disconnecting . . .")

        # Finally, disconnect
        await module_client.disconnect()

    except Exception as e:
        print ( "Unexpected error {}".format(e))
        raise
        
if __name__ == "__main__":
    asyncio.run(main())