# 🐍 ColaExport - Exportador de mensajes desde RabbitMQ

Este proyecto permite conectarse a un servidor RabbitMQ, listar dinámicamente las colas que terminan en `-errores`, y exportar sus mensajes a archivos `.json`, organizados automáticamente por fecha y nombre de cola.

---

## 📦 Requisitos

- Python 3.8 o superior
- Acceso a un servidor RabbitMQ con el plugin de administración habilitado (puerto `15672`)
- Usuario con permisos para acceder a la API REST de RabbitMQ
- Las colas deben estar en un virtual host accesible

---

## 🚀 Instalación

1. Clona o descarga el proyecto.
2. Crea un entorno virtual (opcional pero recomendado):

```bash
python -m venv .venv
.venv\Scripts\activate  # En Windows
```

3. Instala las dependencias:

```bash
pip install -r requirements.txt
```

---

## 🗂️ Estructura del proyecto

```
/
├── config/
│   ├── dev.json
│   ├── qa.json
│   └── prod.json
├── src/
│   └── main.py
├── output/              ← Se genera automáticamente
├── requirements.txt
└── README.md
```

---

## ⚙️ Configuración

Crea los archivos de configuración dentro de la carpeta `config/`. Ejemplo de `dev.json`:

```json
{
  "RABBITMQ_HOST": "10.239.2.41",
  "USERNAME": "usuario_rabbit",
  "PASSWORD": "clave_segura"
}
```

---

## 🧑‍💻 Uso

### ✅ Listar colas con errores y exportar una interactivamente:

```bash
python src/main.py dev listar
```

1. Muestra todas las colas que terminan en `-errores` y tienen mensajes.
2. Te preguntará si deseas exportar alguna.
3. Podrás elegirla por su número de índice.
4. Podrás escoger si quieres copiar los mensajes a una cola nueva con la fecha actual como sufijo en su nombre
5. Podrás purgar la cola original si lo deseas

### ✅ Exportar directamente una cola específica:

```bash
python src/main.py dev nombre-de-la-cola parametros
```

Ejemplo:
```bash
python src/main.py qa dac-errores -crear -purgar
```

---

## 📝 Salida

Los mensajes se guardan automáticamente en la carpeta `output/`, con nombres como:

```
output/dac-errores_20250522_134501.json
```

---

## 📝 Parámetros

Los parámetros permitidos son

- ```-crear```

Crea una cola nueva con la fecha actual como sufijo en su nombre

- ```-purgar```

Purga la cola original

---

## 🛑 Notas

- **Los mensajes NO se eliminan** de la cola (no se hace `ack`).
- Se usa `basic_get` con `auto_ack=False`, por lo que puedes revisar los errores sin alterar el sistema.

---

## 👨‍💻 Autor

Este proyecto fue desarrollado para facilitar la revisión de errores en entornos RabbitMQ en tiempo real, permitiendo extraer los mensajes sin impactar la cola original.
