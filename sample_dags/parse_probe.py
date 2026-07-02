#!/usr/bin/env python
"""
parse_probe.py — reproduce EXACTLY what the RE DAG processor does to parse a
single DAG file, and time each phase. Mirrors airflow.dag_processing.processor._parse_file:

    1. pre-import airflow modules found in the file   (_pre_import_airflow_modules)
    2. BundleDagBag(dag_folder=<file>, bundle_path=<bundle>, load_op_links=False)
    3. SerializedDAG.to_dict(dag) for each parsed DAG   (_serialize_dags)

Usage (inside the dag-processor pod):
    python parse_probe.py <dag_file> [bundle_path]

Examples:
    python parse_probe.py /opt/airflow/dags/saxo_warehouse/bdv/bv_instrument.py
    python parse_probe.py /opt/airflow/dags/saxo_warehouse/bdv/bv_instrument.py /opt/airflow/dags
"""
import os
import sys
import time
from pathlib import Path

# RE marks the parse subprocess as client-side BEFORE importing user code.
os.environ.setdefault("_AIRFLOW_PROCESS_CONTEXT", "client")


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    dag_file = str(Path(sys.argv[1]).resolve())
    bundle_path = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else Path("/opt/airflow/dags")

    print(f"dag_file    : {dag_file}")
    print(f"bundle_path : {bundle_path}")
    print("-" * 72)

    # --- import the machinery (not counted in parse time) ---
    from airflow.models.dagbag import BundleDagBag
    from airflow.serialization.serialized_objects import SerializedDAG
    from airflow.utils.file import iter_airflow_imports

    # ---------------------------------------------------------------
    # Phase 1: pre-import airflow modules (default parsing_pre_import_modules=True)
    # ---------------------------------------------------------------
    import importlib

    t0 = time.perf_counter()
    preimport_count = 0
    for module in iter_airflow_imports(dag_file):
        try:
            importlib.import_module(module)
            preimport_count += 1
        except Exception as e:  # noqa: BLE001 — same swallow as RE
            print(f"  [pre-import warn] {module}: {e}")
    t_preimport = time.perf_counter() - t0

    # ---------------------------------------------------------------
    # Phase 2: build the DagBag for this ONE file (this is the real parse/import)
    # ---------------------------------------------------------------
    t0 = time.perf_counter()
    bag = BundleDagBag(dag_folder=dag_file, bundle_path=bundle_path, load_op_links=False)
    t_dagbag = time.perf_counter() - t0

    # ---------------------------------------------------------------
    # Phase 3: serialize every DAG (what RE ships back to the manager)
    # ---------------------------------------------------------------
    t0 = time.perf_counter()
    serialized = 0
    ser_errors = {}
    for dag in bag.dags.values():
        try:
            SerializedDAG.to_dict(dag)
            serialized += 1
        except Exception as e:  # noqa: BLE001
            ser_errors[dag.fileloc] = repr(e)
    t_serialize = time.perf_counter() - t0

    total = t_preimport + t_dagbag + t_serialize

    print("-" * 72)
    print("PARSE TIMING (mirrors RE _parse_file)")
    print(f"  1. pre-import modules   : {t_preimport:8.3f} s   ({preimport_count} modules)")
    print(f"  2. BundleDagBag build   : {t_dagbag:8.3f} s   <-- import + DAG construction")
    print(f"  3. serialize DAGs       : {t_serialize:8.3f} s   ({serialized} dags)")
    print(f"  {'TOTAL parse time':22}: {total:8.3f} s")
    print("-" * 72)
    print(f"  dags found      : {len(bag.dags)}  ({list(bag.dags.keys())})")
    print(f"  import_errors   : {len(bag.import_errors)}")
    for loc, err in bag.import_errors.items():
        print(f"    - {loc}: {err.splitlines()[-1] if err else ''}")
    for loc, err in ser_errors.items():
        print(f"    - [serialize] {loc}: {err}")


if __name__ == "__main__":
    main()
