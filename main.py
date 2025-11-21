import os
import uuid
import json
import time
import sqlite3
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

# o from PyPDF2 import PdfReader, PdfWriter
from pypdf import PdfReader, PdfWriter
import psycopg2
from psycopg2.extras import execute_values


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


# ----------------------------
# Config
# ----------------------------

class Config:
    def __init__(self, data: dict):

        self.source_dir = data["source_dir"]
        self.extra_page_pdf = data["extra_page_pdf"]
        self.output_dir = data["output_dir"]
        self.threads = data.get("threads", 4)
        self.db_insert = data.get("db_insert", False)
        self.db_path = data.get("db_path", "procesamiento_pdfs.sqlite")
        self.sftp = data.get("sftp", {})
        self.delete_file = data.get("delete_file", False)
        self.postgres = data.get("postgres", {})

    @staticmethod
    def load(path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return Config(data)


def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pdf_stats (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              file_path TEXT NOT NULL,
              output_path TEXT,
              started_at TEXT NOT NULL,
              finished_at TEXT NOT NULL,
              duration_ms INTEGER NOT NULL,
              status TEXT NOT NULL,
              error_message TEXT,
              batch_id TEXT NOT NULL
            );
        """)
        conn.commit()
    finally:
        conn.close()


def insert_stats_bulk(db_path: str, stats: list[PdfStat]):
    if not stats:
        return

    conn = sqlite3.connect(db_path)
    try:
        conn.executemany("""
            INSERT INTO pdf_stats
            (file_path, output_path, started_at, finished_at,
            duration_ms, status, error_message, batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, [
            (
                s.file_path,
                s.output_path,
                s.started_at,
                s.finished_at,
                s.duration_ms,
                s.status,
                s.error_message,
                s.batch_id
            )
            for s in stats
        ])
        conn.commit()
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

def process_single_pdf(pdf_path: str, cfg: Config, extra_page_reader: PdfReader, batch_id: str) -> PdfStat:
    started_at = datetime.utcnow().isoformat()
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
        finished_at = datetime.utcnow().isoformat()
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
    )


def sync_sqlite_to_postgres(sqlite_path: str, pg_dsn: str, table_name: str = "pdf_stats", batch_size: int = 5000):
    """
    Lee todos los registros de pdf_stats en SQLite y los inserta en una tabla equivalente en PostgreSQL.
    Se ejecuta típicamente al final del procesamiento.
    """

    print("Iniciando sincronización SQLite -> Postgres...")

    # Conexión a SQLite
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_cur = sqlite_conn.cursor()

    # Conexión a Postgres
    pg_conn = psycopg2.connect(pg_dsn)
    pg_cur = pg_conn.cursor()

    try:
        # Crear tabla en Postgres si no existe
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            file_path TEXT NOT NULL,
            output_path TEXT,
            started_at TIMESTAMP NOT NULL,
            finished_at TIMESTAMP NOT NULL,
            duration_ms INTEGER NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            batch_id UUID NOT NULL
        );
        """
        pg_cur.execute(create_table_sql)
        pg_conn.commit()

        # Leer desde SQLite en streaming
        sqlite_cur.execute("""
            SELECT file_path, output_path, started_at, finished_at, duration_ms, status, error_message, batch_id
            FROM pdf_stats
        """)

        rows_fetched = 0
        total_inserted = 0

        while True:
            rows = sqlite_cur.fetchmany(batch_size)
            if not rows:
                break

            rows_fetched += len(rows)

            # Insertar en Postgres por lotes
            insert_sql = f"""
                INSERT INTO {table_name}
                (file_path, output_path, started_at, finished_at,
                 duration_ms, status, error_message, batch_id)
                VALUES %s
            """

            execute_values(pg_cur, insert_sql, rows)
            pg_conn.commit()

            total_inserted += len(rows)
            print(f"Sincronizados {total_inserted} registros...")

        print(
            f"Sincronización completada. Total registros insertados en Postgres: {total_inserted}")

    finally:
        sqlite_cur.close()
        sqlite_conn.close()
        pg_cur.close()
        pg_conn.close()


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


def main():
    cfg = Config.load("config.json")
    batch_id = str(uuid.uuid4())
    print(f"Batch ID actual: {batch_id}")

    # Descubrir PDFs
    pdf_files = discover_pdfs(cfg.source_dir)
    total_files = len(pdf_files)
    print(f"Encontrados {total_files} PDFs para procesar.")

    if total_files == 0:
        return

    ensure_dir(cfg.output_dir)

    # Inicializar BD (solo crea tabla si no existe)
    init_db(cfg.db_path)

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
                extra_page_reader, batch_id,
            ): pdf_path
            for pdf_path in pdf_files
        }

        for future in as_completed(future_to_path):
            stat: PdfStat = future.result()
            all_stats.append(stat)
            progress.update(stat.status, stat.duration_ms)

    # Insertar todas las estadísticas en un solo paso
    if cfg.db_insert:
        insert_stats_bulk(cfg.db_path, all_stats)

    # Resumen final
    summary = progress.summary()
    print("\n=== Resumen de procesamiento ===")
    print(f"Total archivos: {summary['total_files']}")
    print(f"Procesados:     {summary['processed_files']}")
    print(f"OK:             {summary['successful']}")
    print(f"Errores:        {summary['failed']}")
    print(f"Tiempo total:   {format_eta(summary['elapsed_seconds'])}")
    print(f"Promedio por archivo: {summary['avg_ms_per_file']:.2f} ms")

    # ============================
    # 🔁 Sincronización a Postgres
    # ============================
    pg_cfg = cfg.postgres
    if pg_cfg.get("enabled", False):
        try:
            sync_sqlite_to_postgres(
                sqlite_path=cfg.db_path,
                pg_dsn=pg_cfg["dsn"],
                table_name=pg_cfg.get("table_name", "pdf_stats"),
            )
        except Exception as e:
            print(f"⚠ Error al sincronizar con Postgres: {e}")


if __name__ == "__main__":
    main()
