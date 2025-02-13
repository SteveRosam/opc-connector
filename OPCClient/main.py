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

params_to_process = os.getenv("PARAMETER_NAMES_TO_PROCESS", '')
PARAMETER_NAMES_TO_PROCESS = json.loads(params_to_process)

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
        browse_name = await node.get_parent().read_browse_name()
        
        friendly_name = browse_name.Name

        print(f"Data change event for node {friendly_name}: {val}")
        id = f'{OPC_NAMESPACE}/{friendly_name}'

        # Extract the DataValue from the data parameter
        data_value = data.monitored_item.Value

        # Extract the source timestamp
        source_timestamp = data_value.SourceTimestamp

        # Extract the variant type
        variant_type = data_value.Value.VariantType

        json_obj = {
            'srv_ts': source_timestamp,
            'rx_ts': time.time_ns(),
            'type': variant_type,
            'val': val,
            'param': friendly_name
        }

        json_str = json.dumps(json_obj)
        json_bytes = json_str.encode('utf-8')

        # publish the data to the topic
        producer.produce(
            topic=topic.name,
            key=id,
            value=json_bytes,
        )

    def event_notification(self, event):
        print("New event", event)


async def main():
    global run, OPC_NAMESPACE, PARAMETER_NAMES_TO_PROCESS

    opc_url = os.environ["OPC_SERVER_URL"]
    tracked_values = {}

    try:
        async with Client(url=opc_url) as client:
            namespace_array_node = client.get_node("i=2255")  # NodeId for NamespaceArray
            namespace_array = await namespace_array_node.read_value()
            target_namespace_index = 0
            
            # determine namespace index
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
                children = await obj.get_children()
                for child in children:
                    child_browse_name = await child.read_browse_name()
                    print("CHILD BROWSE NAME")
                    child_name = child_browse_name.Name
                    try:
                        param_string = f"/Objects/{target_namespace_index}:{browse_name.Name}/{target_namespace_index}:{child_browse_name.Name}"
                        if child_name in PARAMETER_NAMES_TO_PROCESS:
                            myvar = await client.nodes.root.get_child(param_string)
                            tracked_values[param_string] = myvar
                    except Exception as e:
                        print(e)

            # subscribing to a variable node
            subscriptions = {}
            handles = {}
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


            # keep working while 'run' flag is True
            while run:
                await asyncio.sleep(1)


            # unsubscribe handlers on exit
            for val in tracked_values:
                sub = subscriptions[val]
                handle = handles[val]
                await sub.unsubscribe(handle)
                await sub.delete()

                
    except ConnectionError as ce:
        print(ce)

if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO)
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting gracefully.")