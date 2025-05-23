import json
import pika
import datetime
import sys
import os
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm

def cargar_config(entorno):
    ruta = os.path.join(os.path.dirname(__file__), '..', 'config', f'{entorno}.json')
    if not os.path.exists(ruta):
        print(f"Archivo de configuración para entorno '{entorno}' no encontrado.")
        sys.exit(1)

    with open(ruta, 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    if len(sys.argv) != 3:
        print("Uso: python main.py [entorno: dev|qa|prod] [nombre_cola|listar]")
        sys.exit(1)

    entorno = sys.argv[1]
    queue_name = sys.argv[2]

    config = cargar_config(entorno)

    if queue_name.lower() == "listar":
        url = f"http://{config['RABBITMQ_HOST']}:15672/api/queues"
        try:
            response = requests.get(url, auth=HTTPBasicAuth(config['USERNAME'], config['PASSWORD']))
            response.raise_for_status()
            colas = response.json()
            colas_filtradas = [
                (q['name'], q['messages']) for q in colas
                if q['name'].endswith('-errores') and q.get('messages', 0) > 0
            ]

            if not colas_filtradas:
                print("No hay colas de errores con mensajes pendientes.")
                return

            print("\nColas con errores y mensajes pendientes:\n")
            for i, (nombre, total) in enumerate(colas_filtradas, 1):
                print(f"{i}. {nombre} ({total} mensajes)")

            respuesta = input("\n¿Quieres exportar los mensajes de alguna de estas colas? (s/n): ").strip().lower()
            if respuesta != 's':
                return

            indice = input("Introduce el número de la cola que deseas exportar: ").strip()
            if not indice.isdigit() or not (1 <= int(indice) <= len(colas_filtradas)):
                print("Índice no válido.")
                return

            queue_name = colas_filtradas[int(indice) - 1][0]
            print(f"\nExportando mensajes de: {queue_name}\n")

        except Exception as e:
            print(f"Error al consultar la API de RabbitMQ: {e}")
            return

    try:
        credentials = pika.PlainCredentials(config['USERNAME'], config['PASSWORD'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=config['RABBITMQ_HOST'],
            port=5672,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        ))
        channel = connection.channel()

        # Obtener cantidad total de mensajes
        queue = channel.queue_declare(queue=queue_name, passive=True)
        total_messages = queue.method.message_count

        # Determinar opciones según tamaño
        limites = []
        if total_messages <= 1000:
            limites = [100, total_messages]
        elif total_messages <= 5000:
            limites = [100, 1000, total_messages]
        elif total_messages <= 10000:
            limites = [100, 1000, 3000, total_messages]
        else:
            limites = [100, 1000, 3000, 10000, total_messages]

        # Mostrar opciones
        print(f"La cola '{queue_name}' contiene {total_messages} mensajes.")
        print("Cuántos deseas exportar?")
        for i, val in enumerate(limites, 1):
            etiqueta = f"{val}" if val != total_messages else "todos"
            print(f"{i}. {etiqueta}")

        opcion = input("Selecciona una opción por número: ").strip()
        if not opcion.isdigit() or not (1 <= int(opcion) <= len(limites)):
            print("Opción no válida. Cancelando.")
            return

        max_mensajes = limites[int(opcion) - 1]
        print(f"\n🔽 Exportando {max_mensajes} mensaje(s) de '{queue_name}'...\n")

        messages = []
        contador = 0

        with tqdm(total=min(total_messages, max_mensajes), unit="msg") as pbar:
            while contador < max_mensajes:
                method_frame, properties, body = channel.basic_get(queue=queue_name, auto_ack=False)
                if not method_frame:
                    break

                exchange_from_xdeath = "N/A"
                queue_from_xdeath = "N/A"
                time_from_xdeath = "N/A"
                toppic = None

                try:
                    if properties.headers and "x-death" in properties.headers:
                        x_death_list = properties.headers["x-death"]
                        if isinstance(x_death_list, list) and len(x_death_list) > 0:
                            first_x_death = x_death_list[0]
                            exchange_from_xdeath = first_x_death.get("exchange", "N/A")
                            queue_from_xdeath = first_x_death.get("queue", "N/A")
                            unix_time = first_x_death.get("time")
                            toppic = first_x_death.get("routing-keys")
                            if isinstance(unix_time, datetime.datetime):
                                time_from_xdeath = unix_time.strftime('%Y-%m-%d %H:%M:%S')
                            else:
                                time_from_xdeath = datetime.datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    print(f"Error al procesar x-death: {e}")

                messages.append({
                    "queue": queue_from_xdeath,
                    "exchange_from_xdeath": exchange_from_xdeath,
                    "time_from_xdeath": time_from_xdeath,
                    "toppic": toppic,
                    "message": body.decode('utf-8')
                })

                contador += 1
                pbar.update(1)

        output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"{queue_name}_{max_mensajes}_{timestamp}.json"
        output_path = os.path.join(output_dir, output_filename)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(messages, f, ensure_ascii=False, indent=4)

        print(f"\n✅ Descargados {len(messages)} mensajes. Guardados en:\n{output_path}")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"❌ Error de conexión a RabbitMQ: {e}")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
    finally:
        try:
            if 'connection' in locals() and connection.is_open:
                connection.close()
        except Exception as e:
            print(f"⚠️ Error al cerrar la conexión: {e}")

if __name__ == "__main__":
    main()