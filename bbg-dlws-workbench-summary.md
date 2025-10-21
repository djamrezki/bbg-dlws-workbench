
# bbg-dlws-workbench — Project Summary

## 🧠 Overview
**Project name:** `bbg-dlws-workbench`  
**Language:** Python 3.12  
**Goal:** Build and execute Bloomberg Data License Web Services (DLWS) SOAP requests, poll for responses, and export results to CSV. Configurable via YAML and supports identifiers/fields from files.

---

## ✅ Supported request types (phase 1)
1. `submitGetHistoryRequest` / `retrieveGetHistoryResponse`
2. `submitGetDataRequest` / `retrieveGetDataResponse`
3. `getFields` (for *GetFundamentalsHeaders*)

Select via YAML:
```yaml
request:
  kind: history | data | fundamentals_headers
```

---

## ⚙️ Features
- Config-driven execution (`config.yaml`)
- Identifiers from CSV or inline (`id, yellow_key, type`)
- Fields from text file or inline
- Chunking for large identifier lists
- Polling config (`attempts`, `interval_seconds`, `per_attempt_timeout_seconds`)
- Async submit + poll + retrieve workflow
- p12 certificate authentication (extracted cert + private key)
- Normalization: CSV headers = `identifier` (+`date` for history) + requested fields
- Store abstraction (filesystem default, S3 later)

---

## 🧩 Key files
| File | Purpose |
|------|----------|
| `cli.py` | CLI entrypoint (`bbg-dlws run -c config.yaml`) |
| `config.py` | Pydantic config models |
| `soap/builder.py` | Builds SOAP payloads dynamically from config |
| `soap/submitter.py` | Executes Zeep client ops (submit, retrieve, call_sync) |
| `soap/registry.py` | Maps request kinds to WSDL operation names |
| `transform/normalize.py` | Converts SOAP → rows (`identifier`, `date`, fields) |
| `store/filesystem.py` | Default file output implementation |
| `identifiers/csv_loader.py` | Loads identifiers from CSV |
| `identifiers/fields_loader.py` | Loads fields from file |
| `pyproject.toml` | Dependencies: Typer, Pydantic, Zeep, Cryptography |

---

## 🔐 Connection configuration
```yaml
connection:
  wsdl_url: https://service.bloomberg.com/assets/dl/dlws.wsdl
  endpoint: https://dlws.bloomberg.com/datalicense/service
  cert:
    p12_path: ./secrets/bbg_client.p12
    p12_password: ${BBG_P12_PASSWORD}
```

---

## 📤 Output format
- Controlled by `output.uri` (`./file.csv` or later `s3://bucket/key.csv`)
- CSV columns:
  - `history`: `identifier`, `date`, `<fields>`
  - `data`: `identifier`, `<fields>`
  - `fundamentals_headers`: fixed metadata columns

---

## 🧱 Current focus / Next steps
1. Finalize payload mapping in `builder.py` using actual WSDL field names.
2. Implement real Zeep calls in `submit_request`, `get_response_by_id`, and `call_sync`.
3. Refine `normalize.py` extractors to match Zeep response structure.
4. (Optional) Add `S3Store` and CLI `--upload` mode.

---

## 🗝️ TL;DR for next prompt
> **Context:** Developing `bbg-dlws-workbench`, a Python 3.12 CLI to build and execute Bloomberg DLWS SOAP requests (`submitGetHistoryRequest`, `submitGetDataRequest`, `getFields`).  
> Config-driven via YAML, supports identifiers/fields from files, async polling, p12 cert auth, and outputs normalized CSV (`identifier`, `date`, `<fields>`).  
> Current modules: `builder.py`, `submitter.py`, `normalize.py`, etc.  
> Next goal: wire real Zeep payloads/responses to the WSDL structure.
