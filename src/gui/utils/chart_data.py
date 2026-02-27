from dataclasses import dataclass


@dataclass
class MethodRun:
    rows: int
    elapsed: float
    rps: float
    batch_size: int | None = None  # None для default_insert и file_insert


ChartStore = dict[str, dict[str, list[MethodRun]]]


def add_run(
    store: ChartStore,
    db_type: str,
    method: str,
    rows: int,
    elapsed: float,
    batch_size: int | None = None,
) -> None:
    rps = round(rows / elapsed, 1) if elapsed > 0 else 0
    run = MethodRun(rows=rows, elapsed=elapsed, rps=rps, batch_size=batch_size)
    store.setdefault(db_type, {}).setdefault(method, []).append(run)


def get_latest(store: ChartStore) -> dict[str, dict[str, MethodRun]]:
    """
    Для bulk_insert каждый batch_size — отдельная запись.
    Ключ: "bulk_insert:1000", "bulk_insert:5000" и т.д.
    """
    result = {}
    for db_type, methods in store.items():
        result[db_type] = {}
        for method, runs in methods.items():
            if not runs:
                continue

            if method == "bulk_insert":
                # Группируем по batch_size, берём последний прогон каждой группы
                by_batch: dict = {}
                for run in runs:
                    by_batch[run.batch_size] = run  # перезапись = последний прогон

                for batch_size, run in by_batch.items():
                    key = f"bulk_insert:{batch_size}"
                    result[db_type][key] = run
            else:
                result[db_type][method] = runs[-1]

    return result


def series_label(db_type: str, method: str, run: MethodRun) -> str:
    """Формирует подпись линии/бара с учётом batch_size."""
    if method == "bulk_insert" and run.batch_size is not None:
        return f"{db_type} / {method} (batch={run.batch_size})"
    return f"{db_type} / {method}"
