from mqtt_as import MQTTClient, config
from mqtt_local import config
import uasyncio as asyncio
import dht, machine
import btree

d = dht.DHT22(machine.Pin(15))
# BDD
try:
    f = open("mydb", "r+b")
except OSError:
    f = open("mydb", "w+b")

# Now open a database itself
db = btree.open(f)

# db[b"temperatura"]
# db[b"humedad"]
# db[b"setpoint"]
# db[b"periodo"]
# db[b"modo"]

def sub_cb(topic, msg, retained):
    print('Topic = {} -> Valor = {}'.format(topic.decode(), msg.decode()))

async def wifi_han(state):
    print('Wifi is ', 'up' if state else 'down')
    await asyncio.sleep(1)

# If you connect with clean_session True, must re-subscribe (MQTT spec 3.1.2.4)
async def conn_han(client):
    await client.subscribe('riedmaier/temperatura', 1)
    await client.subscribe('riedmaier/humedad', 1)

config["queue_len"] = 1  # Use event interface with default queue size
MQTTClient.DEBUG = True  # Optional: print diagnostic messages
client = MQTTClient(config)

async def main(client):
    await client.connect()
    n = 0
    await asyncio.sleep(2)  # Give broker time
    while True:
        try:
            d.measure()
            try:
                temperatura=d.temperature()
                await client.publish('riedmaier/temperatura', '{}'.format(temperatura), qos = 1)
            except OSError as e:
                print("sin sensor temperatura")
            try:
                humedad=d.humidity()
                await client.publish('riedmaier/humedad', '{}'.format(humedad), qos = 1)
            except OSError as e:
                print("sin sensor humedad")
        except OSError as e:
            print("sin sensor")
        await asyncio.sleep(20)  # Broker is slow

# Define configuration
config['subs_cb'] = sub_cb
config['connect_coro'] = conn_han
config['wifi_coro'] = wifi_han
config['ssl'] = True
config['queue_len'] = 1  # Use event interface with default queue size

# Set up client
MQTTClient.DEBUG = True  
client = MQTTClient(config) # Funcionan estas configuraciones?


try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()
