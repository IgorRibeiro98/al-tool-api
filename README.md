# AL-Tool API (minimal)

Breve documentação atualizada cobrindo os fluxos da pipeline, endpoints e como executar/rodar os testes.

**Project Layout**: key folders and files
- `app/api/`: FastAPI routers (`datasets`, `preprocess`, `reconciliation`, `cancellation`, `reversal`)
- `app/core/`: `config.py`, `db.py`, `storage.py` (helpers for settings, SQLite and file storage)
- `app/pipeline/`: pipeline steps and helpers (importer, utils, preprocess flows, reconciliation)
- `app/repo/`: DB schema (`schema.py`) and repository helpers
- `tests/`: pytest test suite

**Primary capabilities / flows**
- **Upload dataset**: `POST /datasets` accepts multipart file uploads. Uploaded file is saved to `settings.STORAGE_PATH` and a `datasets` record is created.

- **Preprocess dataset (Base B flow)**: `POST /datasets/{id}/preprocess/base-b/run` (and internal `run_preprocess_base_b`)
  - loads original dataset via `app.pipeline.importer.load_dataset_dataframe`
  - infers column types (`infer_column_types`) and fills nulls (`fill_nulls`)
  - applies cancellation marking (`mark_canceled`) using `cancellation_configs`
  - saves preprocessed CSV to `storage/pre/<dataset_id>.csv` via `save_preprocessed_df`
  - records a `preprocess_runs` entry with summary

- **Reversal detection & cancellation support**
  - reversal detection marks rows with `__is_reversal__` (pipeline helper)
  - cancellation marking adds `__is_canceled__` (pipeline helper)
  - `filter_concilable(df)` removes reversal/cancelled rows prior to reconciliation

- **Reconciliation configuration**: `POST /reconciliation-configs` stores a config in `reconciliation_configs` and associated `reconciliation_keys`.
  - A reconciliation config includes `base_a_id`, `base_b_id`, value columns, invert flags and `threshold`.
  - Each `reconciliation_key` lists `base_a_columns` and `base_b_columns` used to build composite keys.

- **Run reconciliation**: `POST /reconciliations/configs/{config_id}/run` executes `run_reconciliation`:
  - loads preprocessed DFs for both bases (`storage/pre/<id>.csv`)
  - filters concilable rows
  - generates composite keys (`generate_keys`) and aggregates values (`group_by_key`)
  - compares keys/values with tolerance (`classify`) and produces per-key results
  - persists a `reconciliation_runs` record and per-key `reconciliation_results`
  - returns `run_id`, `summary` and `details`

- **Query results & export**
  - `GET /reconciliations/results` — list results with filters (`run_id`, `status`, `key_used`, `group_name`) and pagination (`limit`, `offset`).
  - `GET /reconciliations/runs/{run_id}` — get run metadata + details.
  - `GET /reconciliations/runs/{run_id}/export` — export run (summary + details) to Excel (`storage/exports/reconciliation_run_{run_id}.xlsx`) and return file.
  - `merge_results_with_dataframe(dataset_id, run_id)` — pipeline helper to enrich a preprocessed DataFrame with reconciliation results per row.

# AL-Tool API (documentação em Português)

Este repositório contém uma implementação mínima da API e da pipeline de conciliação contábil x fiscal.

Objetivo deste README: descrever a estrutura do projeto, os fluxos principais e um passo-a-passo prático de "Como usar a pipeline".

**Estrutura do projeto**
- `app/api/`: routers FastAPI (`datasets`, `preprocess`, `reconciliation`, `cancellation`, `reversal`)
- `app/core/`: `config.py`, `db.py`, `storage.py` (configurações, helpers de SQLite e armazenamento de arquivos)
- `app/pipeline/`: passos da pipeline e helpers (importer, utils, pré-processos, reconciliação)
- `app/repo/`: esquema do banco de dados (`schema.py`) e helpers de acesso a dados
- `tests/`: suíte de testes com `pytest`

**Fluxos principais**
- Upload de dataset: `POST /datasets` — faz upload de arquivo (multipart), salva no `STORAGE_PATH` e cria um registro em `datasets`.
- Pré-processamento (ex.: Base B): `POST /datasets/{id}/preprocess/base-b/run` — roda o fluxo de pré-processamento e salva `storage/pre/<dataset_id>.csv`.
- Detecção de reversals e marcação de cancelamentos: a pipeline adiciona flags internas `__is_reversal__` e `__is_canceled__` e o filtro `filter_concilable(df)` remove linhas não conciliáveis.
- Configuração de reconciliação: `POST /reconciliation-configs` — cria um registro em `reconciliation_configs` e as chaves em `reconciliation_keys`.
- Execução da reconciliação: `POST /reconciliations/configs/{config_id}/run` — agrega por chave, compara valores com tolerância e persiste `reconciliation_runs` + `reconciliation_results`.
- Consulta e exportação de resultados: `GET /reconciliations/results`, `GET /reconciliations/runs/{run_id}`, `GET /reconciliations/runs/{run_id}/export`.

---

**Como usar a pipeline — passo a passo (guia prático)**

Pré-requisitos
- Ter Python e dependências instaladas (ver `requirements.txt`).
- Ter o serviço rodando (localmente via `uvicorn` ou via `docker compose`).

Passo 0 — Iniciar o serviço (opcional local)
```bash
# no diretório do projeto
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Passo 1 — Fazer upload dos datasets
- Suba os arquivos das duas bases (Base A e Base B) usando o endpoint `POST /datasets`.
- Exemplo cURL:
```bash
curl -X POST "http://localhost:8000/datasets" \
  -F "file=@/caminho/para/base_a.csv" \
  -F "name=base_a" \
  -F "base_type=A"

curl -X POST "http://localhost:8000/datasets" \
  -F "file=@/caminho/para/base_b.csv" \
  -F "name=base_b" \
  -F "base_type=B"
```

Anote o `dataset_id` retornado para cada upload (ex.: `id: 1` para Base A e `id: 2` para Base B).

Passo 2 — (Opcional) Visualizar e mapear colunas
- Use `GET /datasets/{id}/columns` para ver as colunas detectadas.
- Se necessário, atualize mapeamentos de colunas usando os endpoints do módulo `datasets` (se implementados).

Passo 3 — Rodar pré-processamento (ex.: Base B)
- Execute o pré-processo específico (Base B) para normalizar colunas, preencher nulos, marcar cancelamentos/reversals e gerar o CSV pré-processado:
```bash
POST http://localhost:8000/datasets/{base_b_id}/preprocess/base-b/run
```

- Exemplo cURL:
```bash
curl -X POST "http://localhost:8000/datasets/2/preprocess/base-b/run"
```

- Saída esperada: arquivo salvo em `storage/pre/<dataset_id>.csv` e um registro em `preprocess_runs` com resumo.

Passo 4 — Criar configuração de reconciliação
- Crie uma `reconciliation_config` que aponta para `base_a_id` e `base_b_id`, define colunas de valor, sinal de inversão (invert), tolerância (`threshold`) e lista de chaves (cada chave relaciona colunas de A com colunas de B).
- Exemplo de payload JSON:
```json
{
  "name": "Reconciliação Exemplo",
  "base_a_id": 1,
  "base_b_id": 2,
  "value_a_column": "valor_a",
  "value_b_column": "valor_b",
  "invert_b": false,
  "threshold": 0.01,
  "keys": [
    {"base_a_columns": ["cnpj", "nota"], "base_b_columns": ["cnpj_emitente", "numero_nota"], "group_name": "por_nota"}
  ]
}
```

Use o endpoint `POST /reconciliation-configs` para criar a configuração e anote o `config_id` retornado.

Passo 5 — Executar a reconciliação
- Com `config_id` criado, execute:
```bash
POST http://localhost:8000/reconciliations/configs/{config_id}/run
```

Exemplo cURL:
```bash
curl -X POST "http://localhost:8000/reconciliations/configs/1/run"
```

- Resultado: a API cria um registro em `reconciliation_runs` e insere resultados por chave em `reconciliation_results`. A resposta inclui `run_id`, resumo (`matched`, `missing_a`, `missing_b`, etc.) e detalhes por chave.

Passo 6 — Consultar resultados
- Listar resultados com filtros (status, run_id, group_name):
```bash
GET /reconciliations/results?run_id={run_id}&status=missing_a&limit=100&offset=0
```

- Obter resumo e detalhes do run:
```bash
GET /reconciliations/runs/{run_id}
```

Passo 7 — Exportar resultados
- Baixe um Excel com resumo+detalhes do run:
```bash
GET /reconciliations/runs/{run_id}/export
```

O arquivo é salvo em `storage/exports/reconciliation_run_{run_id}.xlsx` e retornado como download.

Passo 8 — (Opcional) Enriquecer dataset com resultados
- Use `merge_results_with_dataframe(dataset_id, run_id)` (helper de pipeline) para anexar colunas de status e diferença ao DataFrame pré-processado.

---

**Boas práticas e observações**
- Sempre pré-processe as bases antes de rodar a reconciliação (arquivo `storage/pre/<id>.csv`).
- Verifique as flags `__is_reversal__` e `__is_canceled__` se precisar auditar por linha.
- Ajuste `threshold` conforme a precisão desejada (valor absoluto ou percentual conforme implementação do `classify`).
- Para produção, recomendo:
  - Adicionar índices e chaves estrangeiras no schema do banco.
  - Usar transações para gravações em lote.
  - Validar payloads com Pydantic nos endpoints.

**Comandos rápidos**
- Iniciar local (uvicorn):
```bash
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

- Rodar testes:
```bash
pip install -r requirements.txt
pytest -q
```

---

Se quiser, eu adiciono exemplos cURL completos para cada endpoint e um pequeno script de exemplo (`scripts/run_example.sh`) que automatiza os passos descritos (upload → preprocess → create config → run → export). Basta dizer qual opção prefere.
