from mqtt_as import MQTTClient, config
from mqtt_local import config
import uasyncio as asyncio
#import network
#import ubinascii  #estas dos lineas use para la mac
import dht, machine
import json


led_interno = machine.Pin("LED", machine.Pin.OUT)
d = dht.DHT22(machine.Pin(15))

DB_FILE = "midb.json"
db_cache = {}

# Cargar la base de datos al iniciar
try:
    with open(DB_FILE, "r") as f:
        db_cache = json.load(f)
except OSError:
    db_cache = {}  # Si no existe, inicia vacía

def get_db(key, default):
    if key in db_cache:
        return db_cache[key]
    else:
        set_db(key, default)
        return default

def set_db(key, val):
    db_cache[key] = val
    # Guardar en la memoria flash
    with open(DB_FILE, "w") as f:
        json.dump(db_cache, f)

async def parpadear():
    for _ in range(5):  # Destella 5 veces (2.5 segundos en total)
        led_interno.on()
        await asyncio.sleep(0.25)
        led_interno.off()
        await asyncio.sleep(0.25)

async def messages(client):  # Quitamos 'datos'
    async for topic, msg, retained in client.queue:
        comando = topic.decode().split('/')[-1]
        val = msg.decode()
        
        # 1. Guardar SOLO los parámetros permitidos en la BD no volátil
        if comando in ["setpoint", "periodo", "modo", "rele"]:
            set_db(comando, val)
            print(f"Guardado en BD {comando}: {val}")

        # 2. Ejecutar acciones que NO se guardan en la BD
        if comando == "destello":
            asyncio.create_task(parpadear())
            print("Acción: Destellando...")
        

async def up(client):
    while True:
        await client.up.wait()
        client.up.clear()
        await client.subscribe('28:cd:c1:04:d8:a7/setpoint', 1)
        await client.subscribe('28:cd:c1:04:d8:a7/periodo', 1)
        await client.subscribe('28:cd:c1:04:d8:a7/destello', 1)
        await client.subscribe('28:cd:c1:04:d8:a7/modo', 1)
        await client.subscribe('28:cd:c1:04:d8:a7/rele', 1)

async def main(client):
    await client.connect()
    asyncio.create_task(up(client))
    asyncio.create_task(messages(client))
    n = 0
    await asyncio.sleep(2)  # Give broker time
    while True:
        #mac = ubinascii.hexlify(network.WLAN().config('mac'), ':').decode() #Obtiene mac
        #print(mac) #muestra mac
        temperatura = None
        humedad = None
        try:
            d.measure()
            try:
                temperatura=d.temperature()
            except OSError as e:
                print("sin sensor temperatura")
            try:
                humedad=d.humidity()
            except OSError as e:
                print("sin sensor humedad")

            datos = {
                "temp": d.temperature(),
                "hum": d.humidity(),
                "setpoint": float(get_db("setpoint", 24.0)),
                "modo": get_db("modo", "auto"),
                "rele": int(get_db("rele", 1)),
                "periodo": int(get_db("periodo", 20))
            }

            json_datos = json.dumps(datos)
            id_dispositivo = "28:cd:c1:04:d8:a7"
            await client.publish(f"{id_dispositivo}/", json_datos, qos=1)    
            print("Publicado:", json_datos)

        except OSError as e:
            print("sin sensor")
        await asyncio.sleep(datos["periodo"])  # Broker is slow

# Define configuration
config['ssl'] = True
config['queue_len'] = 1  # Use event interface with default queue size

# Set up client
MQTTClient.DEBUG = True  
client = MQTTClient(config) # Funcionan estas configuraciones
try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()
