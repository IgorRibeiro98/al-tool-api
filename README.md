# AL-Tool API — SQL-First Pipeline (documentação resumida)

Este repositório implementa uma API e pipeline para conciliação que agora segue o modelo "SQL-first":


**Principais pastas**

**Visão geral do novo fluxo**

1. Upload do arquivo (`POST /datasets`) — salva o arquivo e cria um registro em `datasets`.
2. Ingestão: o arquivo é lido para um pandas DataFrame apenas na etapa de upload e, em seguida, inserido na tabela `dataset_{id}` via `ingest_dataset_to_sql(conn, dataset_id, df)`.
3. Pré-processamento SQL:
   - `fill_nulls_sql(conn, dataset_id, schema_info)` — preenche nulos via UPDATEs SQL.
   - `mark_canceled_sql(conn, dataset_id, indicator_column, canceled_value)` — marca `__is_canceled__` via SQL.
   - `detect_and_mark_reversals_sql(conn, dataset_id, colA, colB, value_col)` — detecta e marca reversals via JOIN SQL.
4. Agregação por chave: `aggregate_by_key_sql(conn, dataset_id, key_columns, value_column)` produz agregados por chave (key → total) usando SQL.
5. Reconciliação SQL: `run_sql_reconciliation(conn, config_id)` agrega A e B, combina via UNION/GROUP BY (simula FULL OUTER JOIN), aplica inversões quando configurado e classifica cada chave (classificação é feita no código, mas os agregados vêm de SQL). O runner persiste `reconciliation_runs` e `reconciliation_results`.
6. Exportação final: `generate_reconciliation_xlsx(conn, run_id)` lê `reconciliation_runs` e `reconciliation_results` por SQL, monta DataFrames apenas aqui e escreve `storage/exports/reconciliation_run_{run_id}.xlsx`.

**Regras importantes**
  - Ler o arquivo no momento do upload;
  - Gerar o arquivo XLSX de exportação no final do fluxo.

**Endpoints (resumo relevante)**
 - `PUT /datasets/{dataset_id}/config/cancellation` — cria/atualiza a configuração de cancelamento para o dataset; aceita `template_id` ou campos `indicator_column`/`canceled_value`/`active_value`.
 - `POST /cancellation-templates` — criar/atualizar um template de cancelamento reutilizável.
 - `GET /cancellation-templates` — listar templates disponíveis.

**Passo a passo: Como usar a API (exemplo rápido)**

1) Inicie a aplicação (local com `uvicorn` ou via Docker Compose):

```bash
# dev
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# com Docker
docker compose up --build -d
```

2) Faça upload de um arquivo para criar um `dataset` (endpoint `POST /datasets`). Exemplo com `curl` (multipart/form-data):

```bash
curl -X POST "http://localhost:8000/datasets" \
  -F "file=@/caminho/para/seu_arquivo.xlsx" \
  -F "name=meu_dataset_de_teste"
```

Resposta esperada (JSON):
```json
{
  "dataset_id": 42,
  "filename": "seu_arquivo.xlsx",
  "rows": 1234
}
```

3) (Opcional) Crie uma configuração de cancelamento ou execute pré-processos SQL via endpoints específicos (se disponíveis). Caso queira criar a configuração de reconciliação, poste em `POST /reconciliation-configs` com payload JSON:

```bash
curl -X POST "http://localhost:8000/reconciliation-configs" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Minha conciliação teste",
    "base_a_id": 42,
    "base_b_id": 43,
    "value_column_a": "amount",
    "value_column_b": "amount",
    "invert_a": 0,
    "invert_b": 0,
    "threshold": 0.01,
    "keys": [
      {"name": "key1", "base_a_columns": ["col1","col2"], "base_b_columns": ["col1","col2"]}
    ]
  }'
```

Resposta esperada (JSON): `{"reconciliation_config_id": 1, "reconciliation_keys": [...]}`

4) Dispare a reconciliação SQL-first: `POST /reconciliations/configs/{config_id}/run`.

```bash
curl -X POST "http://localhost:8000/reconciliations/configs/1/run"
```

Resposta esperada (JSON) com o sumário do run:
```json
{ "run": { "run_id": 7, "status": "finished", "summary": { ... } } }
```

5) Baixe o Excel gerado para o run: `GET /reconciliations/runs/{run_id}/export`.

```bash
curl -L -o reconciliation_run_7.xlsx "http://localhost:8000/reconciliations/runs/7/export"
```

Obs.: os arquivos XLSX gerados são salvos em `storage/exports/` (ou conforme `settings.STORAGE_PATH`).

6) Consultas e inspeção:


Se quiser que eu acrescente exemplos em `httpie` ou snippets para Postman/Insomnia, ou ainda scripts automatizados em `scripts/`, eu adiciono em seguida.

**Trechos de uso (exemplos rápidos)**

Ingestão (exemplo em Python — chamado no upload):
```python
import sqlite3
from app.repo.datasets import ingest_dataset_to_sql

conn = sqlite3.connect('data/app.db')
# df: pandas DataFrame carregado do Excel/CSV/TXT durante upload
res = ingest_dataset_to_sql(conn, dataset_id=42, df=df)
print(res)  # {'dataset_id': 42, 'rows': N}
```

Rodar reconciliação SQL (exemplo):
```python
from app.pipeline.reconciliation import run_sql_reconciliation
conn = sqlite3.connect('data/app.db')
out = run_sql_reconciliation(conn, config_id=1)
print(out['summary'])
```

Exportar XLSX (após run):
```python
from app.pipeline.exporter import generate_reconciliation_xlsx
conn = sqlite3.connect('data/app.db')
res = generate_reconciliation_xlsx(conn, run_id=1)
print(res['path'])
```

**Comandos úteis**

Start local (dev):
```bash
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Start with Docker Compose:
```bash
docker compose up -d
```

Run tests:
```bash
pytest -q
```

**Notas de migração e compatibilidade**

Se quiser que eu atualize exemplos cURL, scripts de demonstração em `scripts/` ou adicione documentação de operações SQL (ex.: índices recomendados), diga qual parte prefere que eu documente a seguir.
