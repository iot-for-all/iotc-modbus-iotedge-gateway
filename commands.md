# IoT Edge Gateway commands to handle modbus CRUD
You're using IoT Central preview feature **"model-less command"** to execute modbus CRUD commands on IoT Edge Gateway.
First, verify that you can see **"model-less command"** feature in your IoT Central app:
1. Click on your IoT Edge Gateway device to go to device detail page
2. On device detail page click on **Manage device** tab
3. Follow the instruction below if you cannot find **Command** to select in the dropdown list:
    - append _"?flights=directmethod"_ flighting option to the end of device exlorer url
    - refersh the page
    - verify **Command** is accessible under **Manage device** tab

    [<img src=./assets/15_model_less_command.png heigth="60%" width="60%">](/assets/15_model_less_command.png)

Using _"model-less command"_ you can send the following commands to IoT Edge Gateway module **"modbus_crud"** to handle modbus CRUD operations:
- **connect**: Connect to modbus server(s) once the modbus server(s) accessible <br />
&nbsp;&nbsp;&nbsp;&nbsp;payload examples:<br />
    - _"[{"serverId": "<SERVER_ID>", "host": "<YOUR_MODBUS_SERVER_HOST>", "port": "<YOUR_MODBUS_SERVER_PORT>"]"_<br />
    - _"[{"serverId": "<SERVER_ID>", "host": "<YOUR_MODBUS_SERVER_URL>", "port": "<YOUR_MODBUS_SERVER_PORT>", "secrets": "<YOUR_B64_ENCODED_SECRETS>}]"_<br />
- **disconnect**: Disconnect from modbus server(s)<br />
&nbsp;&nbsp;&nbsp;&nbsp;payload example: _"[{"serverId": "<SERVER_ID>"}]"_
- **pubInterval**: Modify the modbus client publish events interval in millisecond<br />
&nbsp;&nbsp;&nbsp;&nbsp;payload example: _"[{"serverId": "<SERVER_ID>", "publishInterval": <PUB_INTERVAL_MS>}]"_
- **filter**: Apply filter on modbus nodes specifying the node(s) to _"include"_, _"exclude"_, or _"reset"_ the filter<br />
&nbsp;&nbsp;&nbsp;&nbsp;payload examples:<br />
    - _"[{"serverId": "<SERVER_ID>", "filter": { "action": "include", "nodes": ["<NODE_ID>", "<NODE_ID>"]}}]"_<br />
    - _"[{"serverId": "<SERVER_ID>", "filter": { "action": "exclude", "nodes": ["<NODE_ID>", "<NODE_ID>"]}}]"_<br />
    - _"[{"serverId": "<SERVER_ID>", "filter": { "action": "reset"}}]"_<br />
- **config**: Sends telemetry message containing modbus server(s) nodeid list

Executing commands, you need to fill up the following areas:
- **Method name**: please use one of the above command
- **Module name**: use IoT Edge Gateway module **"modbus_crud"**
- **Payload**: provide an array of structure defined below:
``` ts
{
    serverId: string;                               // Unique value serves as dictiobary key and as deviceId in Lucid pattern
    host: string;                                   // modbus server ipaddress
    port: string;                                   // modbus server port
    modelId?: string;                               // Used in Lucid pattern for provisioning child device
    publishInterval?: number;                       // Publish events interval. default to desired property publishInterval
    filter?: {                                      // Applying filter on modbus nodes
        action: 'include' | 'exclude' | 'reset';
        nodes?: string[];                           // No need to specify if you're using reset action
    };
    secrets?: {                                     // Value should be base64 encoded prior to use in command's payload
        server?: {                                  // Defines secrets to be used to connect to the modbus server
            type: 'pwd' | 'sas' | 'cert';
            pwd?: {                                 // Required if type is pwd
                user: string;
                pwd: string;
            };
            sas?: {                                 // Required if type is sas
                key: string;
            };
            cert?: {                                // Required if type is cert
                thumbprint?: string;
                public: string;                     // Content of public PEM file
                private: string;                    // Content of private PEM file
            };
        };
        client?: {                                  // Client section is to be used for provisioning child device
            type: 'sas' | 'cert' | 'tpm';
            tpm?: {                                 // Required if type is tpm
                endorsementKey?: string;            // Required if storageRootKey not specified
                storageRootKey?: string;            // Required if endorsementKey not specified
            };
            sas?: {                                 // Required if type is sas
                key: string;
            };
            cert?: {                                // Required if type is cert
                thumbprint?: string;
                public: string;                     // Content of public PEM file
                private: string;                    // Content of private PEM file
            };
        };
    };
}[];
```

To do base64 encoding the secrets value you might want to execute some thing like following python script:

``` py
    import json
    import base64

    payloads = []
    payloads.append({
        "serverId": "<YOUR_DEVICE_ID>",
        "host": "<YOUR_MODBUS_SERVER_IPADDRESS>",
        "port": "<YOUR_MODBUS_SERVER_PORT>",
        "secrets": {
            "client": {
                "type": "sas",
                "sas": {
                    "key": "<YOUR_MODBUS_CLIENT_SASKEY>"
                }
            }
        }
    })

    payloads.append({
        "serverId": "<YOUR_DEVICE_ID>",
        "host": "<YOUR_MODBUS_SERVER_IPADDRESS>",
        "port": "<YOUR_MODBUS_SERVER_PORT>",
        "secrets": {
            "client": {
                "type": "cert",
                "cert": {
                    "public": """<YOUR_PUBLIC_PEM_CONTENT>""",
                    "private": """<YOUR_PRIVATE_PEM_CONTENT>"""
                }
            }
        }
    })

    for payload in payloads:
        secrets = payload.get("secrets")
        if secrets:
            secretsStr = json.dumps(secrets, separators=(',', ':'))
            b64Encoded1 = base64.b64encode(secretsStr.encode('utf-8'))
            payload["secrets"] = b64Encoded1.decode("utf-8")

    print(">")
    print(">")
    print(">")
    print("-" * 100)
    print("{}".format(json.dumps(payloads)))
    print("-" * 100)

```