# vidasaludable-merger - Procesador de PDFs con Hilos y Estadísticas

Este proyecto es una herramienta en Python para procesar masivamente archivos PDF ubicados en una estructura de carpetas y subcarpetas.

Cada PDF se procesa así:

1. Se leen todas las páginas del PDF original.
2. Se agrega al final una página extra desde otro PDF de **1 sola hoja** (por ejemplo, una portada, sello, leyenda, etc.).
3. Se guarda un nuevo PDF en una carpeta de salida configurable.
4. Se registran tiempos y estado del procesamiento en una base de datos SQLite.
5. Se muestra el progreso en consola con **porcentaje, tiempo transcurrido y ETA** (tiempo estimado restante).

En una fase posterior (opcional), se puede:

- Copiar o mover los PDFs generados a un servidor remoto por **SFTP**.

---

## Características principales

- Procesamiento de PDFs en **paralelo** usando `ThreadPoolExecutor`.
- Soporte para grandes volúmenes de archivos (recorrido recursivo de carpetas).
- **Configuración sencilla** mediante un archivo `config.json`.
- Registro de estadísticas en **SQLite**:
  - Duración por archivo (ms).
  - Fecha/hora de inicio y fin.
  - Estado (`OK` / `ERROR`).
  - Mensaje de error en caso de fallo.
- Cálculo de:
  - Tiempo total.
  - Promedio por archivo.
  - ETA durante la ejecución.
- Estructura pensada para añadir fácilmente:
  - Subida por SFTP con `paramiko`.
  - Más columnas o tablas de estadísticas si se necesitan.

---

## Requisitos

- **Python** 3.8 o superior (recomendado 3.10+).
- Sistema operativo: Linux, macOS o Windows.
- Dependencias Python (ver `requirements.txt`).

### `requirements.txt` sugerido

```txt
pypdf>=5.0.0
paramiko>=3.0.0
```

> Nota: `paramiko` es para la parte SFTP futura. Si no la usas todavía, sigue siendo seguro tenerla en el entorno.

---

## Instalación

### 1. Clonar o copiar el proyecto

Coloca `main.py`, `config.json`, `requirements.txt` y demás archivos en una carpeta de tu preferencia:

```bash
/proyecto-pdf/
  ├─ main.py
  ├─ config.json
  └─ requirements.txt
```

### 2. Crear un entorno virtual

**En Linux / macOS:**

```bash
cd /ruta/a/proyecto-pdf
python3 -m venv .venv
source .venv/bin/activate
```

**En Windows (PowerShell o CMD):**

```powershell
cd C:\ruta\a\proyecto-pdf
python -m venv .venv
.\.venv\Scripts\activate
```

### 3. Instalar dependencias

Con el entorno virtual activado:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Configuración (`config.json`)

El archivo `config.json` controla el comportamiento del programa. Ejemplo:

```json
{
  "source_dir": "/ruta/a/carpeta/origen",
  "extra_page_pdf": "/ruta/a/hoja_unica.pdf",
  "output_dir": "/ruta/a/salida",
  "threads": 4,
  "delete_file": true,
  "postgres": {
    "enabled": true,
    "dsn": "postgresql://usuario:password@host:5432/tu_basedatos",
    "table_name": "pdf_stats"
  },
  "nodo": "name_nodo"
}
```

### Campos

* `nodo`
  Nombre del nodo donde se ejecuta el proceso  

* `source_dir`
  Carpeta raíz donde se buscarán PDFs recursivamente (incluye subcarpetas).

* `extra_page_pdf`
  Ruta al PDF que contiene **una sola página** que se agregará al final de cada PDF procesado.

* `output_dir`
  Carpeta donde se guardarán los PDFs modificados.

  * Mantiene la misma estructura de subcarpetas que `source_dir`.
  * Si quieres sobrescribir los archivos originales, puedes poner `output_dir` igual a `source_dir` (aunque no se recomienda hasta hacer pruebas).

* `threads`
  Número de hilos de procesamiento en paralelo.
  Ajusta según los núcleos de tu CPU y el tipo de carga (IO vs CPU).

* `delete_file`
  Indica si se eliminaran los archivos origen despues de procesarlo.

* `postgres` (para uso futuro)

  * `enabled`: `true` o `false` para activar/desactivar la subida SFTP.
  * `dsn`: parámetros de conexión a la BD.
  * `table_name`: Nombre de la tabla a la Base de Datos.
  

---

## Estructura de la base de datos

Se usa **SQLite** para registrar estadísticas en una tabla llamada `pdf_stats`.

Esquema:

```sql
CREATE TABLE IF NOT EXISTS pdf_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  file_path TEXT NOT NULL,
  output_path TEXT,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  duration_ms INTEGER NOT NULL,
  status TEXT NOT NULL,        -- 'OK' o 'ERROR'
  error_message TEXT
);
```

### Campos

* `file_path`
  Ruta completa al PDF original.

* `output_path`
  Ruta completa al PDF generado (o `NULL` si falló).

* `started_at` / `finished_at`
  Timestamps en formato ISO (UTC).

* `duration_ms`
  Duración del procesamiento en milisegundos.

* `status`
  `OK` si todo salió bien, `ERROR` si hubo alguna excepción.

* `error_message`
  Texto con el mensaje de error en caso de fallo.

### Consultas útiles de ejemplo

* **Promedio de tiempo por PDF:**

  ```sql
  SELECT AVG(duration_ms) AS avg_ms
  FROM pdf_stats
  WHERE status = 'OK';
  ```

* **Cantidad de errores:**

  ```sql
  SELECT COUNT(*) AS errores
  FROM pdf_stats
  WHERE status = 'ERROR';
  ```

* **Archivos procesados en una carpeta específica:**

  ```sql
  SELECT *
  FROM pdf_stats
  WHERE file_path LIKE '/ruta/a/carpeta/origen/subcarpeta%';
  ```

---

## Funcionamiento interno (resumen)

1. El programa **carga** `config.json`.
2. Ejecuta un `os.walk(source_dir)` para encontrar todos los archivos `.pdf`.
3. Inicializa la base de datos SQLite si no existe.
4. Carga una sola vez el PDF de `extra_page_pdf` para reutilizarlo en todos los hilos.
5. Crea un `ThreadPoolExecutor` con `max_workers = threads`.
6. Para cada PDF:

   * Crea una tarea que:

     * Lee el PDF original con `PdfReader`.
     * Copia todas sus páginas a un `PdfWriter`.
     * Agrega la página extra.
     * Escribe el resultado en `output_dir`, replicando la estructura de carpetas.
     * Mide el tiempo de procesamiento.
     * Inserta un registro en la tabla `pdf_stats`.
7. Un objeto de tipo `ProgressTracker`:

   * Lleva conteo de:

     * Total de archivos.
     * Procesados.
     * OK.
     * Errores.
     * Tiempo total acumulado.
   * Calcula un ETA aproximado usando el promedio de duración de los archivos ya procesados.
   * Muestra en consola algo similar a:

     ```txt
     [12/100] 12.0% | elapsed: 35s | ETA: 4m 10s
     ```
8. Al finalizar, imprime un resumen global con:

   * Total de archivos.
   * Procesados OK / Errores.
   * Tiempo total.
   * Promedio por archivo.

---

## Uso

Con el entorno virtual activado y `config.json` correctamente configurado:

```bash
python main.py
```

Ejemplo de salida en consola:

```text
Encontrados 250 PDFs para procesar.
[1/250] 0.4% | elapsed: 0s | ETA: 2m 30s
[2/250] 0.8% | elapsed: 1s | ETA: 2m 28s
...
=== Resumen de procesamiento ===
Total archivos: 250
Procesados:     250
OK:             248
Errores:        2
Tiempo total:   2m 15s
Promedio por archivo: 540.23 ms
```

---

## SFTP (futuro)

La arquitectura está pensada para **separar** el procesamiento del PDF de la subida por SFTP.

La idea es:

1. Usar este script para procesar y registrar estadísticas.
2. Crear (en el futuro) un segundo script, por ejemplo `upload_sftp.py`, que:

   * Lea la configuración SFTP desde `config.json`.
   * Consulte en `pdf_stats` los registros con `status = 'OK'`.
   * Recorra `output_path` y haga `sftp.put(output_path, remote_dir + nombre_archivo)`.
   * Opcionalmente marque en la BD qué archivos ya se subieron (añadiendo una columna o tabla nueva).

Esto te permite reintentar subidas sin volver a procesar PDFs.

---

## Troubleshooting

* **Error: no encuentra `extra_page_pdf`**
  Verifica la ruta en `config.json` y que el archivo exista y tenga exactamente una página.

* **No se crean PDFs de salida**

  * Revisa que tengas permisos de escritura en `output_dir`.
  * Verifica que `source_dir` contenga PDFs y que se muestren en el conteo inicial.

* **La ejecución es muy lenta**

  * Aumenta `threads` si tu CPU lo soporta.
  * Revisa que el disco no esté saturado.
  * Consulta el promedio por archivo en el resumen y en la BD.

* **Errores relacionados con SFTP**

  * Si todavía no estás usando la parte SFTP, puedes ignorar la configuración SFTP o dejar `enabled: false`.
  * Cuando se implemente el módulo SFTP, revisa host, puerto, usuario y permisos.

---

## Estadisticas en una MAC lectura desde SSD y escritura en SSD

### 4 threads M2 16 GB RAM (INSERSIÓN EN SQLITE)

=== Resumen de procesamiento ===
Total archivos: 56576
Procesados:     56576
OK:             56576
Errores:        0
Tiempo total:   17m 40s
Promedio por archivo: 7.19 ms

### 8 threads M2 16 GB RAM (SIN INSERSIÓN EN SQLITE)

=== Resumen de procesamiento ===
Total archivos: 56576
Procesados:     56576
OK:             56576
Errores:        0
Tiempo total:   16m 47s
Promedio por archivo: 141.86 ms

---

## Licencia

Este proyecto está licenciado bajo los términos de la **Licencia MIT**, lo que permite su uso libre, copia, modificación, distribución y uso comercial, siempre y cuando se mantenga el aviso de copyright.


```
MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
