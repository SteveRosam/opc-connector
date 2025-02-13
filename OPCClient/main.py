import asyncio
import logging
import os
import json
import signal


from asyncua import Client
from quixstreams import Application

# keep the app running?
run = True

_logger = logging.getLogger(__name__)

# Create an Application
app = Application(
        consumer_group="data_source", 
        auto_create_topics=True)

# define the topic using the "output" environment variable
topic_name = os.environ["output"]
topic = app.topic(topic_name)

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

    def datachange_notification(self, node, val, data):
        # print("New data change event", node, val)
        print("New data change event")
        print("--------------" + str(node))
        print(node.nodeid.NamespaceIndex)
        
        print(node.nodeid.Identifier)
        id = f'{node.nodeid.NamespaceIndex}__{node.nodeid.Identifier}'
        print("--------------")
        print(val)
        print("--------------")
        print(data)
        print("--------------")

        with app.get_producer() as producer:
            json_data = json.dumps(val)  # convert the row to JSON

            # publish the data to the topic
            producer.produce(
                topic=topic.name,
                key=id,
                value=json_data,
            )

    def event_notification(self, event):
        print("New event", event)


async def main():
    global run

    url = os.environ["OPC_SERVER_URL"]
    tracked_values = {}

    async with Client(url=url) as client:
        # _logger.info("Root node is: %r", client.nodes.root)
        # _logger.info("Objects node is: %r", client.nodes.objects)

        # Node objects have methods to read and write node attributes as well as browse or populate address space
        # _logger.info("Children of root are: %r", await client.nodes.root.get_children())

        # Get the Objects node
        objects_node = client.nodes.objects
        # Get all child nodes of the Objects node
        objects = await objects_node.get_children()
        
        # Iterate over each object node
        for obj in objects:
            # Get the object's browse name
            browse_name = await obj.read_browse_name()
            print(f"ObjectBrowseName: {browse_name}")

            # if browse_name.Name in ["Device0001", "3D_PRINTER_1"]:
                # print("fooo")
                # Optionally, get and print the children of each object
            children = await obj.get_children()
            print("===========")
            print(children)
            for child in children:
                child_browse_name = await child.read_browse_name()
                print("++++++++++")
                print(f"CHILDBrowseName: {child_browse_name}")

                try:
                        
                    param_string = f"/Objects/2:{browse_name.Name}/2:{child_browse_name.Name}"
                    print("---------")
                    print(param_string)

                #         if param_string not in tracked_values:
                #             myvar = await client.nodes.root.get_child(param_string)
                #             tracked_values[param_string] = myvar
                #         print(myvar)
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
        for val in tracked_values:

            handler = SubHandler()
            sub = await client.create_subscription(10, handler)
        
            # we can also subscribe to events from server
            await sub.subscribe_events()
            subscriptions[val] = sub
            handles[val] = await sub.subscribe_data_change(myvar)
        
        await asyncio.sleep(0.1)

        
        # # calling a method on server
        # res = await obj.call_method("2:multiply", 3, "klk")
        # _logger.info("method result is: %r", res)
        while run:
            await asyncio.sleep(1)

        # unsubscribe the handler
        # await sub.unsubscribe(handle)
        # await sub.delete()

if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO)
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user. Exiting gracefully.")