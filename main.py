import os
import uuid
from pathlib import Path
import json
import time
import threading
import platform
import socket
from weasyprint import HTML, CSS
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

# o from PyPDF2 import PdfReader, PdfWriter
from pypdf import PdfReader, PdfWriter
import psycopg2
from psycopg2.extras import execute_values


HTML_TEMPLATE = """
    <!doctype html>
<html lang="es">

<head>
  <title>Carta</title>
  <meta charset="utf-8" />
  <style>
    body {
      font-family: Arial, sans-serif;
      font-size: 12px;
      color: #111;
    }

    .box {
      border: 1px solid #ddd;
      padding: 16px;
      border-radius: 8px;
    }

    .title {
      font-size: 18px;
      font-weight: 700;
      margin-bottom: 6px;
    }

    .meta {
      color: #555;
      margin-bottom: 14px;
    }

    .label {
      font-weight: 700;
      margin-top: 12px;
    }

    .value {
      margin-top: 4px;
    }

    .footer {
      margin-top: 18px;
      font-size: 10px;
      color: #666;
    }
  </style>
</head>

<body>
  <ps="box">
    Estimada Señor(a) {{campo1}}:
    <p>Tu hijo participó en la jornada de salud de la Estrategia Nacional Vive Saludable, Vive Feliz, impulsada por
      nuestra
      presidenta, la Dra. Claudia Sheinbaum Pardo, que se llevó a cabo en su escuela primaria con la intervención de un
      grupo de especialistas que realizó la medición de su peso y talla, evaluó su agudeza visual y revisó su salud
      bucal.
    </p>
    <p>En el reverso de esta carta, encontrarás el informe de resultados de las valoraciones realizadas, las cuales
      forman
      parte de su Expediente Digital de Salud Escolar que se actualizará cada año. Te invito a seguir las
      recomendaciones
      de los especialistas y acudir con Manuel Alejandro Mendoza Segura a las clínicas de salud para que, de ser
      requerido, reciba atención médica.
    </p>
    <p>Es importante que sepas que todas las consultas, incluida la entrega de lentes, en caso de que los necesite, son
      totalmente gratuitas. Si tienes alguna duda sobre a qué clínica asistir para dar seguimiento a la salud de {{campo2}}, comunícate a los teléfonos de tu entidad que aparecen en el directorio publicado en la
      página vidasaludable.gob.mx</p>
    <p>Para obtener la versión digital del informe de resultados, puedes descargarlo desde:
      resultados.vidasaludable.sep.gob.mx o mediante el correo electrónico y/o número de celular que registraste en el
      consentimiento informado.</p>
    <p></p><b>¡Ayúdanos a formar a la generación más saludable, fuerte y feliz de nuestra historia!</b></p>
    <h2>¡Vive Saludable, Vive Feliz!</h2>
    <h3>Mario Delgado Carrillo</h3>
    <h4>Secretario de Educación Pública</h4>
    <p>¿Tienes alguna
      pregunta o comentario? Comunícate con la mesa de ayuda de tu entidad. https://bit.ly/MesaAyudaVS</p>

    <div class="title">Documento demo (Carta)</div>
    <div class="meta">Generado: {{fecha}}</div>

    <div class="label">Campo 1</div>
    <div class="value">{{campo1}}</div>

    <div class="label">Campo 2</div>
    <div class="value">{{campo2}}</div>

    <div class="footer">* Plantilla base con 2 campos dinámicos</div>
    </div>
</body>

</html>
    """
CSS_PAGED = """
    @page {
    size: Letter;       /* <-- Carta */
    margin: 20mm;       /* ajusta si quieres más/menos margen */
    }
    """


@dataclass
class PdfStat:
    file_path: str
    output_path: Optional[str]
    started_at: str
    finished_at: str
    duration_ms: int
    status: str
    error_message: Optional[str]
    batch_id: str
    nodo: Optional[str] = None
    procesoid: Optional[str] = None


# ----------------------------
# Config
# ----------------------------

class Config:
    def __init__(self, data: dict):

        self.source_dir = data["source_dir"]
        self.extra_page_pdf = data["extra_page_pdf"]
        self.output_dir = data["output_dir"]
        self.threads = data.get("threads", 4)
        self.sftp = data.get("sftp", {})
        self.delete_file = data.get("delete_file", False)

        self.postgres = data.get("postgres", {"enabled": False})
        # self.nodo = data.get("nodo", os.uname().nodename)
        self.nodo = data.get("nodo", platform.node() or socket.gethostname())

    @staticmethod
    def load(path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return Config(data)


# ----------------------------
# Postgres
# ----------------------------

def init_postgres(pg_cfg: dict):
    """
    Inicializa la tabla pdf_stats en Postgres si no existe.
    Espera un dict con al menos: {"enabled": bool, "dsn": "postgresql://..."}
    """
    if not pg_cfg.get("enabled"):
        return

    dsn = pg_cfg.get("dsn")
    if not dsn:
        print("⚠️ Postgres habilitado pero falta 'dsn' en config.postgres")
        return

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pdf_stats (
                        id SERIAL PRIMARY KEY,
                        file_path TEXT NOT NULL,
                        output_path TEXT,
                        started_at TEXT NOT NULL,
                        finished_at TEXT NOT NULL,
                        duration_ms INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        error_message TEXT,
                        batch_id TEXT,
                        nodo TEXT,
                        procesoid TEXT
                    );
                """)
    finally:
        conn.close()


def insert_stats_postgres(pg_cfg: dict, stats: list[PdfStat]):
    """
    Inserta las estadísticas en Postgres de forma individual,
    haciendo COMMIT cada cierto número de filas para no perder todo
    si el proceso se interrumpe.
    """
    if not stats:
        return
    if not pg_cfg.get("enabled"):
        return

    dsn = pg_cfg.get("dsn")
    if not dsn:
        print("⚠️ Postgres habilitado pero falta 'dsn' en config.postgres")
        return

    # Cada cuántas filas hacemos COMMIT
    commit_every = int(pg_cfg.get("commit_every", 500))

    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            count = 0

            for s in stats:
                cur.execute(
                    """
                    INSERT INTO pdf_stats
                        (file_path, output_path, started_at, finished_at,
                         duration_ms, status, error_message,
                         batch_id, nodo, procesoid)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        s.file_path,
                        s.output_path,
                        s.started_at,
                        s.finished_at,
                        s.duration_ms,
                        s.status,
                        s.error_message,
                        s.batch_id,
                        s.nodo,
                        s.procesoid,
                    ),
                )

                count += 1
                # COMMIT parcial
                if count % commit_every == 0:
                    conn.commit()
                    # print(f"✅ Commit parcial de {commit_every} filas (total: {count})")

            # COMMIT final para las que falten
            conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"❌ Error insertando en Postgres: {e}")
        raise
    finally:
        conn.close()


# ----------------------------
# Utilidades
# ----------------------------


def discover_pdfs(source_dir: str):
    pdfs = []
    for root, dirs, files in os.walk(source_dir):
        for name in files:
            if name.lower().endswith(".pdf"):
                pdfs.append(os.path.join(root, name))
    return pdfs


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def remove_empty_dirs(path):
    """
    Elimina subcarpetas vacías dentro de 'path' y finalmente intenta eliminar 'path'
    si también queda vacía.
    """
    # Recorremos de abajo hacia arriba
    for root, dirs, files in os.walk(path, topdown=False):
        for d in dirs:
            dir_path = os.path.join(root, d)
            try:
                if not os.listdir(dir_path):  # Directorio vacío
                    os.rmdir(dir_path)
                    print(f"🗑️ Carpeta eliminada: {dir_path}")
            except Exception as e:
                print(f"⚠️ Error eliminando carpeta {dir_path}: {e}")

    # Finalmente intenta eliminar la carpeta raíz si queda vacía
    try:
        if not os.listdir(path):
            os.rmdir(path)
            print(f"🗑️ Carpeta raíz eliminada: {path}")
    except Exception as e:
        print(f"⚠️ Error eliminando carpeta raíz {path}: {e}")


def format_eta(seconds: float) -> str:
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    elif m > 0:
        return f"{m}m {s}s"
    else:
        return f"{s}s"


# ----------------------------
# Procesamiento de un PDF
# ----------------------------

def process_single_pdf(pdf_path: str, cfg: Config, extra_page_reader: PdfReader, batch_id: str, nodo: str,
                       procesoid: str,) -> PdfStat:
    # started_at = datetime.utcnow().isoformat()
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.time()

    rel_path = os.path.relpath(pdf_path, cfg.source_dir)
    output_path = os.path.join(cfg.output_dir, rel_path)
    ensure_dir(os.path.dirname(output_path))

    status = "ERROR"
    error_message = None

    try:
        reader = PdfReader(pdf_path)
        writer = PdfWriter()

        for page in reader.pages:
            writer.add_page(page)

        writer.add_page(extra_page_reader.pages[0])

        with open(output_path, "wb") as f_out:
            writer.write(f_out)

        status = "OK"

    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        output_path = None

    finally:
        t1 = time.time()
        finished_at = datetime.now(timezone.utc).isoformat()
        duration_ms = int((t1 - t0) * 1000)

        # ============================
        # 🔥 ELIMINAR ORIGINAL SI OK
        # ============================
        if status == "OK" and cfg.delete_file:
            try:
                os.remove(pdf_path)
            except Exception as e:
                if error_message:
                    error_message += f" | Fallo al borrar: {e}"
                else:
                    error_message = f"Procesado OK, pero fallo al borrar: {e}"

    return PdfStat(
        file_path=pdf_path,
        output_path=output_path,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        status=status,
        error_message=error_message,
        batch_id=batch_id,
        nodo=nodo,
        procesoid=procesoid,
    )


# ----------------------------
# Control de progreso / ETA
# ----------------------------

class ProgressTracker:
    def __init__(self, total_files: int):
        self.total_files = total_files
        self.processed_files = 0
        self.successful = 0
        self.failed = 0
        self.total_duration_ms = 0
        self.start_time = time.time()
        self._lock = threading.Lock()

    def update(self, status: str, duration_ms: int):
        with self._lock:
            self.processed_files += 1
            if status == "OK":
                self.successful += 1
            else:
                self.failed += 1

            self.total_duration_ms += duration_ms

            elapsed = time.time() - self.start_time
            avg_per_file = elapsed / self.processed_files
            remaining = self.total_files - self.processed_files
            eta = remaining * avg_per_file

            percent = (self.processed_files / self.total_files) * \
                100 if self.total_files else 100

            print(
                f"[{self.processed_files}/{self.total_files}] "
                f"{percent:.1f}% | "
                f"elapsed: {format_eta(elapsed)} | "
                f"ETA: {format_eta(eta)}"
            )

    def summary(self):
        elapsed = time.time() - self.start_time
        avg_ms = self.total_duration_ms / \
            self.processed_files if self.processed_files else 0
        return {
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "successful": self.successful,
            "failed": self.failed,
            "elapsed_seconds": elapsed,
            "avg_ms_per_file": avg_ms,
        }


def render_template(campo1: str, campo2: str) -> str:
    html = HTML_TEMPLATE
    html = html.replace("{{fecha}}", datetime.now().strftime("%Y-%m-%d %H:%M"))
    html = html.replace("{{campo1}}", escape_html(campo1))
    html = html.replace("{{campo2}}", escape_html(campo2))
    return html


def escape_html(s: str) -> str:
    # Evita que caracteres rompan el HTML
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#39;"))


def render_template_from_file(template_path: Path, campo1: str, campo2: str) -> str:
    html = template_path.read_text(encoding="utf-8")
    html = html.replace("{{fecha}}", datetime.now().strftime("%Y-%m-%d %H:%M"))
    html = html.replace("{{campo1}}", escape_html(campo1))
    html = html.replace("{{campo2}}", escape_html(campo2))
    return html


def generar_pdf_carta(output_pdf: str, campo1: str, campo2: str) -> None:
    template_path = Path("report") / "carta.html"
    # <- para resolver globals.css y fonts/...
    base_dir = template_path.parent.resolve()

    html_str = render_template_from_file(template_path, campo1, campo2)

    HTML(string=html_str, base_url=str(base_dir)).write_pdf(
        output_pdf,
        stylesheets=[CSS(string=CSS_PAGED)]
    )


def main():
    # generar_pdf_carta(
    #     "salida_carta.pdf",
    #     campo1="David Jesus Enciso Guadarrama",
    #     campo2="Eliott David Enciso"
    # )
    # print("PDF generado: salida_carta.pdf")
    
    cfg = Config.load("config.json")
    batch_id = str(uuid.uuid4())
    process_id = str(uuid.uuid4())  # procesoid para esta corrida
    nodo = cfg.nodo
    print(f"Batch ID actual: {batch_id}")

    # Descubrir PDFs
    pdf_files = discover_pdfs(cfg.source_dir)
    total_files = len(pdf_files)
    print(f"Encontrados {total_files} PDFs para procesar.")

    if total_files == 0:
        return

    ensure_dir(cfg.output_dir)

    use_postgres = cfg.postgres.get("enabled", False)

    if use_postgres:
        print("📚 Usando Postgres para registrar estadísticas.")
        init_postgres(cfg.postgres)

    progress = ProgressTracker(total_files)

    # Cargar el PDF de una sola página solo una vez
    extra_page_reader = PdfReader(cfg.extra_page_pdf)

    all_stats: list[PdfStat] = []

    # Procesar en paralelo
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=cfg.threads) as executor:
        future_to_path = {
            executor.submit(
                process_single_pdf,
                pdf_path,
                cfg,
                extra_page_reader,
                batch_id,
                nodo,
                process_id,
            ): pdf_path
            for pdf_path in pdf_files
        }

        for future in as_completed(future_to_path):
            stat: PdfStat = future.result()
            all_stats.append(stat)
            progress.update(stat.status, stat.duration_ms)

    if use_postgres:
        print("\n=== Sincronizando a BD... ===")
        insert_stats_postgres(cfg.postgres, all_stats)

    # Eliminar carpetas vacias en el origen.
    if cfg.delete_file:
        print("🧹 Eliminando carpetas vacías...")
        remove_empty_dirs(cfg.source_dir)

    # Resumen final
    summary = progress.summary()
    print("\n=== Resumen de procesamiento ===")
    print(f"Total archivos: {summary['total_files']}")
    print(f"Procesados:     {summary['processed_files']}")
    print(f"OK:             {summary['successful']}")
    print(f"Errores:        {summary['failed']}")
    print(f"Tiempo total:   {format_eta(summary['elapsed_seconds'])}")
    print(f"Promedio por archivo: {summary['avg_ms_per_file']:.2f} ms")


if __name__ == "__main__":
    main()
