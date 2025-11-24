#!/usr/bin/env python3
"""Script de exemplo: enriquece um dataset com os resultados de uma reconciliação.

Uso:
  python scripts/enrich_dataset.py <dataset_id> <run_id> [<out_path>]

Se `out_path` não for informado, salva em `settings.STORAGE_PATH/exports/enriched_dataset_{dataset_id}_run_{run_id}.xlsx`.
"""
from pathlib import Path
import sys

import pandas as pd

from app.pipeline.reconciliation import merge_results_with_dataframe
from app.core.config import settings


def main(dataset_id: int, run_id: int, out_path: str | None = None) -> Path:
    df = merge_results_with_dataframe(int(dataset_id), int(run_id))

    if out_path:
        out = Path(out_path)
    else:
        out = Path(settings.STORAGE_PATH) / "exports" / f"enriched_dataset_{dataset_id}_run_{run_id}.xlsx"

    out.parent.mkdir(parents=True, exist_ok=True)

    # Save as Excel (user can change to CSV by calling df.to_csv(...))
    df.to_excel(out, index=False)
    print(f"Enriquecimento salvo em: {out}")
    return out


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/enrich_dataset.py <dataset_id> <run_id> [<out_path>]")
        sys.exit(1)
    dataset_id = int(sys.argv[1])
    run_id = int(sys.argv[2])
    out = sys.argv[3] if len(sys.argv) > 3 else None
    main(dataset_id, run_id, out)
