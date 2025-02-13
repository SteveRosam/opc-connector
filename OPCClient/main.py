import asyncio
import logging
import os
import json
import signal
import time

from asyncua import Client
from quixstreams import Application

# keep the app running?
run = True

OPC_NAMESPACE = os.environ["OPC_NAMESPACE"]
TOPIC_NAME = os.environ["output"]

_logger = logging.getLogger(__name__)
logging.getLogger("asyncua.common.subscription").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO)

# Create an Application
app = Application(
        consumer_group="data_source", 
        auto_create_topics=True)

producer = app.get_producer()

# define the topic using the "output" environment variable
topic = app.topic(TOPIC_NAME)

def handle_sigterm(signum, frame):
    global run
    print("\nReceived SIGTERM. Exiting gracefully.")
    run = False


# Register the signal handler
signal.signal(signal.SIGTERM, handle_sigterm)

class SubHandler:
    """
    Subscription Handler. To receive events from server for a subscription
    data_change and event methods are called directly from receiving thread.
    Do not do expensive, slow or network operation there. Create another
    thread if you need to do such a thing
    """
    global producer

    async def datachange_notification(self, node, val, data):
        print(f"Data change event for node {node.nodeid.Identifier}: {val}")

        browse_name = await node.read_browse_name()
        friendly_name = browse_name.Name
        id = f'{OPC_NAMESPACE}/{friendly_name}'

        print(data)

        json_data = json.dumps(val)  # convert the row to JSON

        # Extract the DataValue from the data parameter
        data_value = data.monitored_item.Value

        # Extract the source timestamp
        source_timestamp = data_value.SourceTimestamp

        # Extract the variant type
        variant_type = data_value.Value.VariantType

        xxx = {
            'srv_ts': source_timestamp,
            'rx_ts': time.time_ns(),
            'type': variant_type,
            'val': val,
            'param': friendly_name
        }

        print(json.dumps(xxx))

        # publish the data to the topic
        producer.produce(
            topic=topic.name,
            key=id,
            value=json_data,
        )

    def event_notification(self, event):
        print("New event", event)


async def main():
    global run, OPC_NAMESPACE

    opc_url = os.environ["OPC_SERVER_URL"]
    opc_namespace = os.environ["OPC_NAMESPACE"]
    tracked_values = {}

    try:
        async with Client(url=opc_url) as client:
            
            # # Access the Objects node
            # objects_node = client.nodes.objects
            
            # # Get all child nodes of the Objects node
            # objects = await objects_node.get_children()
            
            # # Iterate over each object node
            # for obj in objects:
            #     # Get the object's browse name and NodeId
            #     browse_name = await obj.read_browse_name()
            #     node_id = obj.nodeid
                
            #     # Print the object's browse name and NodeId
            #     print(f"Object: {browse_name.Name}, NodeId: {node_id}")
                
            #     # Optionally, print the children of each object
            #     children = await obj.get_children()
            #     for child in children:
            #         child_browse_name = await child.read_browse_name()
            #         child_node_id = child.nodeid
            #         print(f"  Child Node: {child_browse_name.Name}, NodeId: {child_node_id}")

            # print("END END END END END END END END END END END END END ")





            namespace_array_node = client.get_node("i=2255")  # NodeId for NamespaceArray
            namespace_array = await namespace_array_node.read_value()
            target_namespace_index = 0
            
            if OPC_NAMESPACE in namespace_array:
                target_namespace_index = namespace_array.index(OPC_NAMESPACE)

            # Get the Objects node
            objects_node = client.nodes.objects
            # Get all child nodes of the Objects node
            objects = await objects_node.get_children()
            
            # Iterate over each object node
            for obj in objects:
                # Get the object's browse name
                browse_name = await obj.read_browse_name()
                # print(f"ObjectBrowseName: {browse_name}")
                obj_id = obj.nodeid.Identifier
                # print(obj_id)

                # if browse_name.Name in ["Device0001", "3D_PRINTER_1"]:
                    # print("fooo")
                    # Optionally, get and print the children of each object
                children = await obj.get_children()
                # print("===========")
                # print(children)
                for child in children:
                    child_browse_name = await child.read_browse_name()
                    # print("++++++++++")
                    # print(f"CHILDBrowseName: {child_browse_name}")

                    try:
                        # print(child.nodeid)
                        child_id = child.nodeid.Identifier
                        # print(child_id)
                            
                        param_string = f"/Objects/{target_namespace_index}:{browse_name.Name}/{target_namespace_index}:{child_browse_name.Name}"
                    #     print("---------")
                        # print(param_string)

                    # #         if param_string not in tracked_values:
                        # print(type(child_id))
                        # print(child_id in [12,13])
                        if child_id in [12,13]:
                            # print("~_~_~_~_~_~_~_~_")
                            myvar = await client.nodes.root.get_child(param_string)
                            tracked_values[param_string] = myvar
                            # print(tracked_values)
                            # print(myvar)
                            # print("~_~_~_~_~_~_~_~_")
                    #         # print(f"  Child Node: {child_browse_name}, Value: {child_value}")
                    except Exception as e:
                        print(e)
                

            # # Now getting a variable node using its browse path
            # myvar = await client.nodes.root.get_child("/Objects/2:MyObject/2:MyVariable")
            # obj = await client.nodes.root.get_child("Objects/2:MyObject")
            # _logger.info("myvar is: %r", myvar)

            # subscribing to a variable node
            subscriptions = {}
            handles = {}
            # print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
            # print(tracked_values)
            for val in tracked_values:

                # Get the node for the current value
                myvar = await client.nodes.root.get_child(val)  # Adjust this line to get the correct node

                # Create a handler and subscription for each node
                handler = SubHandler()
                sub = await client.create_subscription(10, handler)

                # Subscribe to data changes for the node
                handle = await sub.subscribe_data_change(myvar)

                # Store the subscription and handle
                subscriptions[val] = sub
                handles[val] = handle
                
                # Optional: Sleep to stagger subscriptions
                await asyncio.sleep(0.1)


            
            # # calling a method on server
            # res = await obj.call_method("2:multiply", 3, "klk")
            # _logger.info("method result is: %r", res)
            while run:
                await asyncio.sleep(1)

            # unsubscribe the handler
            # await sub.unsubscribe(handle)
            # await sub.delete()
    except ConnectionError as ce:
        print(ce)

if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO)
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting gracefully.")