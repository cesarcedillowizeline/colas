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

            indice = input("Introduce el número de la cola que deseas exportar: ").strip()
            if not indice.isdigit() or not (1 <= int(indice) <= len(colas_filtradas)):
                print("Índice no válido.")
                return

            queue_name = colas_filtradas[int(indice) - 1][0]
            print(f"\nExportando mensajes de: {queue_name}\n")

        except Exception as e:
            print(f"Error al consultar la API de RabbitMQ: {e}")
            return

    # Obtener total de mensajes usando API HTTP
    cola_url = f"http://{config['RABBITMQ_HOST']}:15672/api/queues/%2F/{queue_name}"
    response = requests.get(cola_url, auth=HTTPBasicAuth(config['USERNAME'], config['PASSWORD']))
    total_messages = response.json().get("messages", 0)

    if total_messages <= 5000:
        max_mensajes = total_messages
        print(f"La cola '{queue_name}' contiene {total_messages} mensajes.")
        print(f"\n🔽 Exportando directamente {max_mensajes} mensaje(s) de '{queue_name}'...\n")
    else:
        limites = [i for i in range(5000, total_messages, 5000)]
        if total_messages not in limites:
            limites.append(total_messages)

        print(f"La cola '{queue_name}' contiene {total_messages} mensajes.")
        print("¿Cuántos deseas exportar?")
        for i, val in enumerate(limites, 1):
            etiqueta = f"{val}" if val != total_messages else "todos"
            print(f"{i}. {etiqueta}")

        opcion = input("Selecciona una opción por número: ").strip()
        if not opcion.isdigit() or not (1 <= int(opcion) <= len(limites)):
            print("Opción no válida. Cancelando.")
            return

        max_mensajes = limites[int(opcion) - 1]
        print(f"\n🔽 Exportando {max_mensajes} mensaje(s) de '{queue_name}'...\n")

    url_get = f"http://{config['RABBITMQ_HOST']}:15672/api/queues/%2F/{queue_name}/get"
    auth = HTTPBasicAuth(config['USERNAME'], config['PASSWORD'])

    output_dir = os.path.join(os.path.dirname(__file__), '..', 'output')
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"{queue_name}_{max_mensajes}_{timestamp}.json"
    output_path = os.path.join(output_dir, output_filename)

    obtenidos = 0
    batch_size = 50
    first = True

    with open(output_path, 'w', encoding='utf-8') as f_out:
        f_out.write('[\n')  # Abrir lista JSON

        with tqdm(total=max_mensajes, unit="msg") as pbar:
            while obtenidos < max_mensajes:
                count = min(batch_size, max_mensajes - obtenidos)
                body = {
                    "count": count,
                    "ackmode": "ack_requeue_true",
                    "encoding": "auto",
                    "truncate": 50000
                }
                resp = requests.post(url_get, json=body, auth=auth)
                if resp.status_code != 200:
                    print(f"Error al obtener mensajes: {resp.status_code}")
                    break

                lote = resp.json()
                if not lote:
                    break

                for item in lote:
                    x_death = item.get("properties", {}).get("headers", {}).get("x-death", [])
                    if x_death:
                        x_info = x_death[0]
                        exchange = x_info.get("exchange", "N/A")
                        queue_from = x_info.get("queue", "N/A")
                        toppic = x_info.get("routing-keys", [])
                        unix_time = x_info.get("time")
                        if isinstance(unix_time, int):
                            time_str = datetime.datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            time_str = str(unix_time)
                    else:
                        exchange = queue_from = time_str = "N/A"
                        toppic = []

                    mensaje = {
                        "queue": queue_from,
                        "exchange_from_xdeath": exchange,
                        "time_from_xdeath": time_str,
                        "toppic": toppic,
                        "message": item.get("payload", "")
                    }

                    if not first:
                        f_out.write(',\n')
                    f_out.write(json.dumps(mensaje, ensure_ascii=False, indent=4))
                    first = False

                obtenidos += len(lote)
                pbar.update(len(lote))

        f_out.write('\n]')  # Cierra lista JSON

    print(f"\n✅ Descargados {obtenidos} mensajes. Guardados en:\n{output_path}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⛔ Ejecución interrumpida por el usuario. Saliendo...")
