# src/bbg_dlws_workbench/cli.py
import typer
import yaml
from typing import List, Optional

from .config import AppConfig
from .store import resolve_store
from .identifiers.csv_loader import load_identifiers_from_csv
from .identifiers.chunker import chunk
from .identifiers.fields_loader import load_fields
from .soap.client import create_client
from .soap.submitter import submit_request, get_response_by_id, call_sync
from .soap.poller import Poller
from .transform.normalize import soap_to_rows
from .soap.registry import OP_HANDLERS
from .soap.builder import build_payload
from .soap.fields_criteria import build_fields_criteria
from .soap.fields_ops import get_fields
import logging, sys

app = typer.Typer(help="Bloomberg DLWS Workbench", no_args_is_help=True)

logging.basicConfig(
    level=logging.INFO,  # or DEBUG for full verbosity
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

@app.command("run")
def run(
        config: str = typer.Option(..., "-c", "--config", help="Path to YAML configuration file."),
        dry_run: bool = typer.Option(False, "--dry-run", help="Print payloads instead of sending."),
):
    """
    Build request(s) from config, then submit/poll/retrieve and write CSV.
    With --dry-run, only print the payloads per chunk.
    """
    # Load and validate config
    with open(config, "r", encoding="utf-8") as f:
        cfg = AppConfig.model_validate(yaml.safe_load(f))

    # Resolve kind & op params
    kind = cfg.request.kind
    op = OP_HANDLERS[kind]
    params = (
        cfg.request.history_params if kind == "history"
        else cfg.request.data_params if kind == "data"
        else cfg.request.fundamentals_params
    )

    # Prepare fields (inline or file)
    fields = load_fields(cfg.request.fields)

    # Prepare identifiers iterator (inline or CSV file)
    if kind == "fundamentals_headers":
        # fundamentals headers normally don't use identifiers; force single batch
        batches = [[{}]]
    else:
        if cfg.request.identifiers.source == "csv":
            csvcfg = cfg.request.identifiers.csv
            it = load_identifiers_from_csv(
                path=str(csvcfg.path),
                id_col=csvcfg.id_column,
                yk_col=csvcfg.yellow_key_column,
                type_col=csvcfg.type_column,
                extra_cols=csvcfg.extra_columns,
            )
        else:
            it = (
                {
                    "id": x["id"],
                    "yellow_key": x.get("yellow_key", ""),
                    "type": x.get("type", ""),
                    "extras": x.get("extras", {}),
                }
                for x in cfg.request.identifiers.inline
            )
        batches = (
            chunk(it, cfg.chunking.max_identifiers_per_request)
            if cfg.chunking.enabled
            else [list(it)]
        )

    # DRY RUN: print payloads and exit
    if dry_run:
        for idx, batch in enumerate(batches, start=1):
            payload = build_payload(
                kind=kind,
                fields=fields,
                identifiers_batch=([] if kind == "fundamentals_headers" else list(batch)),
                overrides=[o.model_dump() for o in cfg.request.overrides],
                params=params,
            )
            typer.echo(f"--- Chunk {idx} {kind} payload (dry-run) ---")
            typer.echo(str(payload))
        raise typer.Exit(code=0)

    # LIVE RUN
    store = resolve_store(cfg.output.uri)
    client = create_client(
        wsdl_url=str(cfg.connection.wsdl_url),
        p12_path=str(cfg.connection.cert.p12_path),
        p12_password=cfg.connection.cert.p12_password,
    )
    poller = Poller(
        attempts=cfg.polling.attempts,
        interval_s=cfg.polling.interval_seconds,
        per_attempt_timeout_s=cfg.polling.per_attempt_timeout_seconds,
    )

    append = cfg.output.append_mode
    for idx, batch in enumerate(batches, start=1):
        payload = build_payload(
            kind=kind,
            fields=fields,
            identifiers_batch=([] if kind == "fundamentals_headers" else list(batch)),
            overrides=[o.model_dump() for o in cfg.request.overrides],
            params=params,
        )

        # Submit / Call
        if op["async"]:
            response_id = submit_request(client, str(cfg.connection.endpoint), kind, payload)

            def fetch():
                return get_response_by_id(
                    client,
                    kind,
                    response_id,
                    timeout=cfg.polling.per_attempt_timeout_seconds,
                )

            soap_response = poller.poll(fetch)
        else:
            soap_response = call_sync(
                client,
                str(cfg.connection.endpoint),
                kind,
                payload,
                timeout=cfg.polling.per_attempt_timeout_seconds,
            )

        # Optionally save raw
        if cfg.output.include_raw_xml:
            suffix = f".chunk{idx}.xml" if idx > 1 else ".xml"
            store.write_text(cfg.output.uri + suffix, str(soap_response))

        # Normalize and write CSV
        rows = list(soap_to_rows(kind, soap_response, fields))
        store.write_rows_to_csv(cfg.output.uri, rows, append=append)
        append = True  # subsequent chunks append


@app.command("fields")
def fields(
        config: str = typer.Option(..., "-c", "--config", help="Path to YAML config (uses only the connection block)."),
        out: Optional[str] = typer.Option(None, "--out", help="Output CSV path (overrides output.uri if provided)."),
        category: List[str] = typer.Option([], "--category", "-C", help="DL category filter (repeatable)."),
        sector: List[str] = typer.Option([], "--sector", "-S", help="Market sector filter (repeatable)."),
        keyword: List[str] = typer.Option([], "--keyword", "-K", help="Keyword filter (repeatable)."),
        timeout: int = typer.Option(30, "--timeout", help="Per-request timeout (seconds)."),
):
    """
    Fetch Bloomberg field mnemonics & metadata via getFields(criteria=...).
    """
    # 1) Load config (we only need the connection block; output is optional)
    with open(config, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    cfg = AppConfig.model_validate(raw)

    # 2) Build Zeep client (p12 cert)
    client = create_client(
        wsdl_url=str(cfg.connection.wsdl_url),
        p12_path=str(cfg.connection.cert.p12_path),
        p12_password=cfg.connection.cert.p12_password,
    )

    # 3) Build criteria from CLI flags
    criteria = build_fields_criteria(categories=category, sectors=sector, keywords=keyword)

    # 4) Call getFields
    resp = get_fields(client, criteria=criteria or None, timeout=timeout)

    # 5) Normalize → rows (reuse 'fundamentals_headers' shape)
    rows = list(soap_to_rows("fundamentals_headers", resp, []))

    if not rows:
        typer.echo("No fields found with the given criteria.")
        raise typer.Exit(code=0)

    # 6) Decide output target
    uri = out or getattr(cfg.output, "uri", None) or "./output/fields.csv"

    # 7) Write CSV via your Store abstraction
    store = resolve_store(uri)
    store.write_rows_to_csv(uri, rows, append=False)

    typer.echo(f"Wrote {len(rows)} fields to: {uri}")


if __name__ == "__main__":
    app()
