import os
import json
import time
import sqlite3
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

from pypdf import PdfReader, PdfWriter  # o from PyPDF2 import PdfReader, PdfWriter


@dataclass
class PdfStat:
    file_path: str
    output_path: Optional[str]
    started_at: str
    finished_at: str
    duration_ms: int
    status: str
    error_message: Optional[str]


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

    @staticmethod
    def load(path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return Config(data)


# ----------------------------
# DB / Estadísticas
# ----------------------------

# class StatsDB:
#     def __init__(self, db_path: str):
#         self.db_path = db_path
#         self._lock = threading.Lock()
#         self._init_db()

#     def _init_db(self):
#         conn = sqlite3.connect(self.db_path)
#         try:
#             conn.execute("""
#                 CREATE TABLE IF NOT EXISTS pdf_stats (
#                   id INTEGER PRIMARY KEY AUTOINCREMENT,
#                   file_path TEXT NOT NULL,
#                   output_path TEXT,
#                   started_at TEXT NOT NULL,
#                   finished_at TEXT NOT NULL,
#                   duration_ms INTEGER NOT NULL,
#                   status TEXT NOT NULL,
#                   error_message TEXT
#                 );
#             """)
#             conn.commit()
#         finally:
#             conn.close()

#     def insert_stat(self, file_path, output_path, started_at, finished_at,
#                     duration_ms, status, error_message=None):
#         with self._lock:
#             conn = sqlite3.connect(self.db_path)
#             try:
#                 conn.execute("""
#                     INSERT INTO pdf_stats
#                     (file_path, output_path, started_at, finished_at,
#                      duration_ms, status, error_message)
#                     VALUES (?, ?, ?, ?, ?, ?, ?);
#                 """, (file_path, output_path, started_at, finished_at,
#                       duration_ms, status, error_message))
#                 conn.commit()
#             finally:
#                 conn.close()

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
              error_message TEXT
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
             duration_ms, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, [
            (
                s.file_path,
                s.output_path,
                s.started_at,
                s.finished_at,
                s.duration_ms,
                s.status,
                s.error_message,
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

# def process_single_pdf(pdf_path: str, cfg: Config, stats_db: StatsDB, extra_page_reader: PdfReader):
#     ENABLED = False
#     started_at = datetime.utcnow().isoformat()
#     t0 = time.time()

#     rel_path = os.path.relpath(pdf_path, cfg.source_dir)
#     output_path = os.path.join(cfg.output_dir, rel_path)
#     ensure_dir(os.path.dirname(output_path))

#     try:
#         reader = PdfReader(pdf_path)
#         writer = PdfWriter()

#         # Copiar todas las páginas del PDF original
#         for page in reader.pages:
#             writer.add_page(page)

#         # Agregar la primera (y única) página del PDF extra
#         writer.add_page(extra_page_reader.pages[0])

#         # Guardar resultado
#         with open(output_path, "wb") as f_out:
#             writer.write(f_out)

#         status = "OK"
#         error_message = None
#     except Exception as e:
#         status = "ERROR"
#         error_message = str(e)
#         output_path = None
#     finally:
#         t1 = time.time()
#         finished_at = datetime.utcnow().isoformat()
#         duration_ms = int((t1 - t0) * 1000)

#         if ENABLED:
#             stats_db.insert_stat(
#                 file_path=pdf_path,
#                 output_path=output_path,
#                 started_at=started_at,
#                 finished_at=finished_at,
#                 duration_ms=duration_ms,
#                 status=status,
#                 error_message=error_message,
#             )

#     return status, duration_ms
def process_single_pdf(pdf_path: str, cfg: Config, extra_page_reader: PdfReader) -> PdfStat:
    started_at = datetime.utcnow().isoformat()
    t0 = time.time()

    rel_path = os.path.relpath(pdf_path, cfg.source_dir)
    output_path = os.path.join(cfg.output_dir, rel_path)
    ensure_dir(os.path.dirname(output_path))

    try:
        reader = PdfReader(pdf_path)
        writer = PdfWriter()

        # Copiar todas las páginas del PDF original
        for page in reader.pages:
            writer.add_page(page)

        # Agregar la página extra
        writer.add_page(extra_page_reader.pages[0])

        # Guardar resultado
        with open(output_path, "wb") as f_out:
            writer.write(f_out)

        status = "OK"
        error_message = None
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        output_path = None
    finally:
        t1 = time.time()
        finished_at = datetime.utcnow().isoformat()
        duration_ms = int((t1 - t0) * 1000)

    return PdfStat(
        file_path=pdf_path,
        output_path=output_path,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        status=status,
        error_message=error_message,
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

            percent = (self.processed_files / self.total_files) * 100 if self.total_files else 100

            print(
                f"[{self.processed_files}/{self.total_files}] "
                f"{percent:.1f}% | "
                f"elapsed: {format_eta(elapsed)} | "
                f"ETA: {format_eta(eta)}"
            )

    def summary(self):
        elapsed = time.time() - self.start_time
        avg_ms = self.total_duration_ms / self.processed_files if self.processed_files else 0
        return {
            "total_files": self.total_files,
            "processed_files": self.processed_files,
            "successful": self.successful,
            "failed": self.failed,
            "elapsed_seconds": elapsed,
            "avg_ms_per_file": avg_ms,
        }


# ----------------------------
# (Futuro) SFTP
# ----------------------------
# Placeholder: luego puedes implementar la subida por SFTP
# usando paramiko, leyendo cfg.sftp y recorriendo
# lo que ya está en la BD con status = 'OK'.


# ----------------------------
# main
# ----------------------------

# def main():
#     cfg = Config.load("config.json")

#     # Descubrir PDFs
#     pdf_files = discover_pdfs(cfg.source_dir)
#     total_files = len(pdf_files)
#     print(f"Encontrados {total_files} PDFs para procesar.")

#     if total_files == 0:
#         return

#     ensure_dir(cfg.output_dir)

#     stats_db = StatsDB(cfg.db_path)
#     progress = ProgressTracker(total_files)

#     # Cargar el PDF de una sola página solo una vez
#     extra_page_reader = PdfReader(cfg.extra_page_pdf)

#     # Procesar en paralelo
#     with ThreadPoolExecutor(max_workers=cfg.threads) as executor:
#         future_to_path = {
#             executor.submit(
#                 process_single_pdf,
#                 pdf_path,
#                 cfg,
#                 stats_db,
#                 extra_page_reader
#             ): pdf_path
#             for pdf_path in pdf_files
#         }

#         for future in as_completed(future_to_path):
#             status, duration_ms = future.result()
#             progress.update(status, duration_ms)

#     # Resumen final
#     summary = progress.summary()
#     print("\n=== Resumen de procesamiento ===")
#     print(f"Total archivos: {summary['total_files']}")
#     print(f"Procesados:     {summary['processed_files']}")
#     print(f"OK:             {summary['successful']}")
#     print(f"Errores:        {summary['failed']}")
#     print(f"Tiempo total:   {format_eta(summary['elapsed_seconds'])}")
#     print(f"Promedio por archivo: {summary['avg_ms_per_file']:.2f} ms")

def main():
    cfg = Config.load("config.json")

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
                extra_page_reader
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



if __name__ == "__main__":
    main()
