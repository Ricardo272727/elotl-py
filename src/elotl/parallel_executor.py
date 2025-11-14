from .base_executor import ExecutorBase
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os
from typing import List
from .base_executor import StepResult

# ---------------------------
# Helpers
# ---------------------------

def dynamic_chunk_size(total_items: int) -> int:
    """
    Calcula un tamaño de chunk dinámico basado en el número de CPUs.
    """
    cpu_count = os.cpu_count() or 4
    return max(1, math.ceil(total_items / cpu_count))


def chunk_list(items, size):
    """
    Divide una lista en chunks del tamaño especificado.
    """
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ---------------------------
# Núcleo del framework
# ---------------------------

def parallel_map(items, worker_fn, max_workers=None):
    """
    Ejecuta worker_fn(item) en paralelo.
    Retorna una lista con los resultados.
    """
    if not items:
        return []
    
    max_workers = max_workers or os.cpu_count() or 4

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker_fn, item) for item in items]

        for future in as_completed(futures):
            results.append(future.result())

    return results


def parallel_map_chunks(items, worker_fn, max_workers=None):
    """
    Divide items en chunks dinámicos, procesa cada chunk en paralelo.
    worker_fn recibe un chunk (lista).
    """
    if not items:
        return []

    max_workers = max_workers or os.cpu_count() or 4
    chunk_size = dynamic_chunk_size(len(items))

    chunks = list(chunk_list(items, chunk_size))

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker_fn, chunk) for chunk in chunks]

        for future in as_completed(futures):
            results.append(future.result())

    return results


class ParallelExecutor(ExecutorBase):
    def execute(self) -> List[StepResult]:
        raise NotImplementedError()

