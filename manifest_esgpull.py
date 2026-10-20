#!/usr/bin/env python3
"""Resolve and download an exact URL manifest with ESGpull's Python API.

The workflow is deliberately staged:

* ``audit`` validates the manifest without opening ESGpull.
* ``resolve`` enriches each URL with ESGF file metadata, checkpointing results
  in a sidecar SQLite database. It never changes the ESGpull database.
* ``download`` registers resolved files, links them to dashboard-compatible
  bookkeeping queries, and hands explicit ``File`` batches to ESGpull.

ESGF search is only a metadata lookup. A returned record is accepted only when
its HTTPServer URL is present verbatim in the manifest.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence
from urllib.parse import urlsplit


STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS manifest_url (
    url TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    dataset_id_hint TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    source TEXT,
    metadata_json TEXT,
    error TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS manifest_url_status ON manifest_url(status);
CREATE INDEX IF NOT EXISTS manifest_url_dataset
    ON manifest_url(hostname, dataset_id_hint, status);
"""

VALID_HOST_RE = re.compile(
    r"^(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)(?:\."
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)
# CMIP publication versions in these URLs are timestamp-like (vYYYYMMDD).
# Keeping this deliberately narrow avoids mistaking variables such as ``va``
# for the version directory.
VERSION_RE = re.compile(r"^v\d{4,}$")

# Tags recognized by c375_dashboard.py. The generated queries are deliberately
# untracked, so a future `esgpull update` cannot broaden the manifest.
INSTITUTION_TAG = {
    "BSC": "bsc",
    "CMCC": "test",
    "DWD": "dwd",
    "EC-Earth-Consortium": "bsc",
    "MOHC": "mohc",
    "NERSC": "uib",
    "NCC": "uib",
    "UIB": "uib",
}


@dataclass(frozen=True)
class ParsedURL:
    url: str
    hostname: str
    dataset_id: str | None
    error: str | None = None


def chunked(items: Iterable[str], size: int) -> Iterator[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def parse_manifest_url(value: str) -> ParsedURL:
    """Validate a URL and infer its CMIP dataset ID from its DRS path."""
    value = value.strip()
    parsed = urlsplit(value)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not hostname:
        return ParsedURL(value, hostname, None, "not an HTTP(S) URL")
    if not VALID_HOST_RE.fullmatch(hostname) or ".." in parsed.netloc:
        return ParsedURL(value, hostname, None, "malformed hostname")
    parts = [part for part in parsed.path.split("/") if part]
    try:
        server_idx = parts.index("fileServer")
    except ValueError:
        return ParsedURL(value, hostname, None, "missing /fileServer/ path")

    # Storage roots differ by node, so anchor on project and version instead.
    tail = parts[server_idx + 1 :]
    project_idx = next(
        (i for i, part in enumerate(tail) if part in {"CMIP6", "CMIP6Plus"}),
        None,
    )
    if project_idx is None:
        return ParsedURL(value, hostname, None, "CMIP project not found in path")
    drs = tail[project_idx:]
    version_idx = next(
        (i for i, part in enumerate(drs[:-1]) if VERSION_RE.fullmatch(part)),
        None,
    )
    if version_idx is None:
        return ParsedURL(value, hostname, None, "dataset version not found in path")
    dataset_parts = drs[: version_idx + 1]
    if len(dataset_parts) < 10:
        return ParsedURL(value, hostname, None, "incomplete CMIP dataset path")
    return ParsedURL(value, hostname, ".".join(dataset_parts))


def manifest_local_path(value: str) -> tuple[str, str]:
    """Return the ESGpull DRS directory and filename encoded in a URL."""
    parsed = parse_manifest_url(value)
    if parsed.error or parsed.dataset_id is None:
        raise ValueError(parsed.error or value)
    parts = [part for part in urlsplit(value).path.split("/") if part]
    project_idx = next(
        i for i, part in enumerate(parts) if part in {"CMIP6", "CMIP6Plus"}
    )
    version_idx = next(
        i
        for i in range(project_idx, len(parts) - 1)
        if VERSION_RE.fullmatch(parts[i])
    )
    return "/".join(parts[project_idx : version_idx + 1]), parts[-1]


def read_manifest(path: Path) -> tuple[list[ParsedURL], int]:
    records: list[ParsedURL] = []
    seen: set[str] = set()
    duplicates = 0
    with path.open(encoding="utf-8-sig") as handle:
        for raw in handle:
            value = raw.strip()
            if not value or value.startswith("#"):
                continue
            if value in seen:
                duplicates += 1
                continue
            seen.add(value)
            records.append(parse_manifest_url(value))
    return records, duplicates


def connect_state(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(STATE_SCHEMA)
    return conn


def seed_state(conn: sqlite3.Connection, records: Sequence[ParsedURL]) -> None:
    # The manifest is authoritative. Keep resumable metadata for URLs that are
    # still present, but discard checkpoint rows removed from a revised
    # manifest (including corrected malformed URLs).
    conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS current_manifest_url "
        "(url TEXT PRIMARY KEY)"
    )
    conn.execute("DELETE FROM current_manifest_url")
    conn.executemany(
        "INSERT INTO current_manifest_url(url) VALUES (?)",
        [(record.url,) for record in records],
    )
    conn.execute(
        "DELETE FROM manifest_url "
        "WHERE url NOT IN (SELECT url FROM current_manifest_url)"
    )
    conn.executemany(
        """
        INSERT OR IGNORE INTO manifest_url
            (url, hostname, dataset_id_hint, status, error)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                record.url,
                record.hostname,
                record.dataset_id,
                "invalid" if record.error else "pending",
                record.error,
            )
            for record in records
        ],
    )
    conn.commit()


def state_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        row["status"]: row["count"]
        for row in conn.execute(
            "SELECT status, COUNT(*) count FROM manifest_url GROUP BY status"
        )
    }


def print_counts(conn: sqlite3.Connection) -> None:
    counts = state_counts(conn)
    print("Manifest state: " + ", ".join(f"{k}={v:,}" for k, v in sorted(counts.items())))


def canonical_metadata(file: object) -> dict[str, object]:
    data = dict(file.asdict())  # type: ignore[attr-defined]
    status = data.get("status")
    if hasattr(status, "name"):
        data["status"] = status.name
    return data


def resolve_from_database(conn: sqlite3.Connection, esg: object) -> int:
    """Resolve manifest files already known to the ESGpull database.

    The manifest is authoritative for the source URL.  An existing record may
    therefore be reused either by exact URL or by its stable ESGF identity
    (dataset ID plus filename); in the latter case its trusted size/checksum
    metadata is retained while the manifest URL replaces the old replica URL.
    """
    pending_rows = conn.execute(
        """SELECT url, dataset_id_hint FROM manifest_url
           WHERE status = 'pending'"""
    ).fetchall()
    if not pending_rows:
        return 0
    pending_by_url = {row["url"]: row["url"] for row in pending_rows}
    pending_by_identity: dict[tuple[str, str], str] = {}
    ambiguous_identities: set[tuple[str, str]] = set()
    for row in pending_rows:
        dataset_id = row["dataset_id_hint"]
        filename = Path(urlsplit(row["url"]).path).name
        if not dataset_id or not filename:
            continue
        key = (dataset_id, filename)
        if key in pending_by_identity:
            ambiguous_identities.add(key)
        else:
            pending_by_identity[key] = row["url"]
    for key in ambiguous_identities:
        pending_by_identity.pop(key, None)

    from esgpull.models import File
    import sqlalchemy as sa

    count = 0
    stmt = sa.select(File).execution_options(yield_per=2_000)
    result = esg.db.session.scalars(stmt)  # type: ignore[attr-defined]
    updates: list[tuple[str, str, str]] = []
    for file in result:
        manifest_url = pending_by_url.get(file.url)
        source = "database-exact-url"
        if manifest_url is None:
            manifest_url = pending_by_identity.get((file.dataset_id, file.filename))
            source = "database-file-identity"
        if manifest_url is None:
            continue
        metadata = canonical_metadata(file)
        metadata["url"] = manifest_url
        updates.append(
            (
                json.dumps(metadata, separators=(",", ":")),
                source,
                manifest_url,
            )
        )
        pending_by_url.pop(manifest_url, None)
        pending_by_identity.pop((file.dataset_id, file.filename), None)
        count += 1
        if len(updates) >= 2_000:
            conn.executemany(
                """UPDATE manifest_url SET status='resolved', metadata_json=?,
                   source=?, error=NULL, updated_at=CURRENT_TIMESTAMP WHERE url=?""",
                updates,
            )
            conn.commit()
            updates.clear()
    if updates:
        conn.executemany(
            """UPDATE manifest_url SET status='resolved', metadata_json=?,
               source=?, error=NULL, updated_at=CURRENT_TIMESTAMP WHERE url=?""",
            updates,
        )
        conn.commit()
    return count


def normalize_search_url(url: str) -> str:
    # ESGpull normalizes HTTPServer records to HTTPS in File.serialize().
    return url.replace("http://", "https://", 1).rstrip("/")


def match_manifest_file(
    file: object,
    wanted_by_url: dict[str, str],
    wanted_by_identity: dict[tuple[str, str], str],
) -> tuple[str, str] | None:
    """Return the authoritative manifest URL and how it was matched."""
    manifest_url = wanted_by_url.get(normalize_search_url(file.url))  # type: ignore[attr-defined]
    if manifest_url is not None:
        return manifest_url, "esgf-search-exact-url"
    manifest_url = wanted_by_identity.get(
        (file.dataset_id, file.filename)  # type: ignore[attr-defined]
    )
    if manifest_url is not None:
        return manifest_url, "esgf-search-file-identity"
    return None


def resolve_from_esgf(
    conn: sqlite3.Connection,
    esg: object,
    dataset_batch: int,
    retries: int,
    limit_batches: int | None,
) -> tuple[int, int]:
    """Resolve pending URLs using batched exact-version dataset searches."""
    from esgpull.models import Query

    rows = conn.execute(
        """
        SELECT hostname, dataset_id_hint
        FROM manifest_url
        WHERE status='pending' AND dataset_id_hint IS NOT NULL
        GROUP BY hostname, dataset_id_hint
        ORDER BY hostname, dataset_id_hint
        """
    ).fetchall()
    grouped: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        grouped[row["hostname"]].append(row["dataset_id_hint"])

    resolved = searched = batch_number = 0
    for hostname, dataset_ids in grouped.items():
        for ids in chunked(dataset_ids, dataset_batch):
            if limit_batches is not None and batch_number >= limit_batches:
                return resolved, searched
            batch_number += 1
            placeholders = ",".join("?" for _ in ids)
            manifest_rows = conn.execute(
                f"""SELECT url, dataset_id_hint FROM manifest_url WHERE status='pending'
                     AND hostname=? AND dataset_id_hint IN ({placeholders})""",
                [hostname, *ids],
            ).fetchall()
            wanted_by_url = {
                normalize_search_url(row["url"]): row["url"]
                for row in manifest_rows
            }
            wanted_by_identity: dict[tuple[str, str], str] = {}
            ambiguous: set[tuple[str, str]] = set()
            for row in manifest_rows:
                key = (
                    row["dataset_id_hint"],
                    Path(urlsplit(row["url"]).path).name,
                )
                if key in wanted_by_identity:
                    ambiguous.add(key)
                else:
                    wanted_by_identity[key] = row["url"]
            for key in ambiguous:
                wanted_by_identity.pop(key, None)
            if not wanted_by_url:
                continue

            query = Query(
                options={"distrib": True, "latest": None, "replica": None, "retracted": False},
                selection={"dataset_id": ids, "data_node": hostname},
            )
            files = None
            last_error: BaseException | None = None
            for attempt in range(1, retries + 1):
                try:
                    # A timeout must not be mistaken for a confirmed no-match.
                    esg.context.noraise = False  # type: ignore[attr-defined]
                    files = esg.context.files(  # type: ignore[attr-defined]
                        query, max_hits=None, keep_duplicates=True
                    )
                    break
                except BaseException as exc:
                    last_error = exc
                    if attempt < retries:
                        time.sleep(min(5 * 2 ** (attempt - 1), 30))
            if files is None:
                message = f"ESGF lookup failed after {retries} attempts: {last_error}"
                conn.executemany(
                    """UPDATE manifest_url SET error=?, updated_at=CURRENT_TIMESTAMP
                       WHERE url=? AND status='pending'""",
                    [(message, original) for original in wanted_by_url.values()],
                )
                conn.commit()
                continue

            updates: list[tuple[str, str, str]] = []
            matched_urls: set[str] = set()
            for file in files:
                matched = match_manifest_file(
                    file, wanted_by_url, wanted_by_identity
                )
                if matched is None:
                    continue
                original, source = matched
                if original in matched_urls:
                    continue
                matched_urls.add(original)
                file.url = original
                updates.append(
                    (
                        json.dumps(canonical_metadata(file), separators=(",", ":")),
                        source,
                        original,
                    )
                )
            if updates:
                conn.executemany(
                    """UPDATE manifest_url SET status='resolved', metadata_json=?,
                       source=?, error=NULL, updated_at=CURRENT_TIMESTAMP WHERE url=?""",
                    updates,
                )
                conn.commit()
                resolved += len(updates)
            searched += len(wanted_by_url)
            print(
                f"Resolved {resolved:,}/{searched:,} searched URLs "
                f"({hostname}, metadata batch {batch_number:,})",
                flush=True,
            )
    return resolved, searched


def export_report(conn: sqlite3.Connection, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("status\tsource\turl\terror\n")
        for row in conn.execute(
            "SELECT status, source, url, error FROM manifest_url ORDER BY status, url"
        ):
            values = [row["status"] or "", row["source"] or "", row["url"], row["error"] or ""]
            handle.write("\t".join(value.replace("\t", " ").replace("\n", " ") for value in values) + "\n")


def export_invalid_urls(records: Sequence[ParsedURL], path: Path) -> int:
    invalid = [record for record in records if record.error]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("error\turl\tsuggested_url\n")
        for record in invalid:
            parsed = urlsplit(record.url)
            suggested = parsed._replace(
                netloc=re.sub(r"\.{2,}", ".", parsed.netloc)
            ).geturl()
            handle.write(f"{record.error}\t{record.url}\t{suggested}\n")
    return len(invalid)


def dataset_facets(dataset_id: str) -> dict[str, str] | None:
    parts = dataset_id.split(".")
    if len(parts) < 10 or parts[0] not in {"CMIP6", "CMIP6Plus"}:
        return None
    return {
        "project": parts[0],
        "activity_id": parts[1],
        "institution_id": parts[2],
        "source_id": parts[3],
        "experiment_id": parts[4],
        "member_id": parts[5],
        "table_id": parts[6],
        "variable_id": parts[7],
        "grid_label": parts[8],
        "version": parts[9],
    }


def bookkeeping_key(metadata: dict[str, object]) -> tuple[str, str, str, str] | None:
    facets = dataset_facets(str(metadata["dataset_id"]))
    if facets is None:
        return None
    tag = INSTITUTION_TAG.get(facets["institution_id"])
    if tag is None:
        return None
    member = facets["member_id"]
    sub_experiment = member.split("-", 1)[0] if member.startswith("s") else ""
    return tag, facets["project"], facets["experiment_id"], sub_experiment


def ensure_bookkeeping_queries(esg: object, metadata_rows: Iterable[dict[str, object]]) -> dict[tuple[str, str, str, str], object]:
    """Create untracked provenance queries understood by c375_dashboard.py."""
    from esgpull.models import Query

    keys = sorted({key for row in metadata_rows if (key := bookkeeping_key(row)) is not None})
    result: dict[tuple[str, str, str, str], object] = {}
    for tag, project, experiment, sub_experiment in keys:
        selection: dict[str, str] = {
            "project": project,
            "experiment_id": experiment,
            "query": "manifest_exact_url_provenance",
        }
        if sub_experiment:
            selection["sub_experiment_id"] = sub_experiment
        query = Query(tracked=False, tags=tag, selection=selection)
        query.compute_sha()
        merged = esg.db.merge(query, commit=True)  # type: ignore[attr-defined]
        result[(tag, project, experiment, sub_experiment)] = merged
    return result


def iter_resolved_metadata(conn: sqlite3.Connection) -> Iterator[dict[str, object]]:
    for row in conn.execute(
        "SELECT metadata_json FROM manifest_url WHERE status='resolved' ORDER BY url"
    ):
        yield json.loads(row["metadata_json"])


async def register_and_download(
    conn: sqlite3.Connection,
    esg: object,
    batch_size: int,
    register_only: bool,
    limit_files: int | None,
) -> tuple[int, int, int]:
    from esgpull.models import Dataset, File, FileStatus
    import sqlalchemy as sa

    # Use repeatable SQLite iterators instead of retaining hundreds of
    # thousands of metadata dictionaries in memory.
    queries = ensure_bookkeeping_queries(esg, iter_resolved_metadata(conn))
    completed = failed = registered = 0
    queue: list[object] = []

    async def flush() -> None:
        nonlocal completed, failed
        if not queue or register_only:
            queue.clear()
            return
        done, errors = await esg.download(list(queue), use_db=True, show_progress=True)
        completed += len(done)
        failed += len(errors)
        queue.clear()

    dataset_counts = Counter(
        str(row["dataset_id"]) for row in iter_resolved_metadata(conn)
    )
    for row in iter_resolved_metadata(conn):
        if limit_files is not None and registered >= limit_files:
            break
        dataset_id = str(row["dataset_id"])
        dataset = esg.db.session.get(Dataset, dataset_id)
        if dataset is None:
            esg.db.session.add(Dataset(dataset_id=dataset_id, total_files=dataset_counts[dataset_id]))

        incoming = File.fromdict(row)  # type: ignore[arg-type]
        incoming.compute_sha()
        existing = esg.db.session.get(File, incoming.sha)
        if existing is None:
            existing = esg.db.session.scalar(sa.select(File).where(File.file_id == incoming.file_id))
        if existing is None:
            existing = incoming
            esg.db.session.add(existing)
        else:
            if existing.checksum != incoming.checksum:
                raise RuntimeError(f"checksum conflict for {incoming.file_id}")
            existing.url = incoming.url

        key = bookkeeping_key(row)
        if key is None:
            raise RuntimeError(f"cannot map {dataset_id!r} to a C375 dashboard query")
        query = queries[key]
        esg.db.session.flush()
        esg.db.link(query, existing)
        esg.db.session.commit()
        registered += 1

        if existing.status != FileStatus.Done:
            queue.append(existing)
        if len(queue) >= batch_size:
            await flush()
            print(
                f"Registered {registered:,}; completed {completed:,}; failed {failed:,}",
                flush=True,
            )
    await flush()
    return registered, completed, failed


def direct_metadata(url: str, size: int, checksum: str) -> dict[str, object]:
    parsed = parse_manifest_url(url)
    if parsed.dataset_id is None or parsed.error:
        raise ValueError(parsed.error or url)
    local_path, filename = manifest_local_path(url)
    dataset_master, version = parsed.dataset_id.rsplit(".", 1)
    return {
        "file_id": f"{parsed.dataset_id}.{filename}",
        "dataset_id": parsed.dataset_id,
        "master_id": f"{dataset_master}.{filename}",
        "url": url,
        "version": version,
        "filename": filename,
        "local_path": local_path,
        "data_node": parsed.hostname,
        "checksum": checksum,
        "checksum_type": "SHA256",
        "size": size,
        "status": "Done",
    }


def hash_path(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(8 << 20):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


async def download_manifest_url(
    client: object,
    semaphore: asyncio.Semaphore,
    data_root: Path,
    url: str,
) -> tuple[str, dict[str, object] | None, str | None]:
    """Download one authoritative URL and compute its local SHA-256."""
    import aiofiles

    local_path, filename = manifest_local_path(url)
    destination = data_root / local_path / filename
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file():
        size, checksum = await asyncio.to_thread(hash_path, destination)
        return url, direct_metadata(url, size, checksum), None

    partial = destination.with_name(f".{filename}.manifest.part")
    try:
        async with semaphore:
            digest = hashlib.sha256()
            size = 0
            async with client.stream("GET", url) as response:  # type: ignore[attr-defined]
                response.raise_for_status()
                async with aiofiles.open(partial, "wb") as handle:
                    async for chunk in response.aiter_bytes(8 << 20):
                        await handle.write(chunk)
                        size += len(chunk)
                        digest.update(chunk)
            expected = response.headers.get("content-length")
            if expected is not None and int(expected) != size:
                raise RuntimeError(
                    f"size mismatch: received {size}, Content-Length was {expected}"
                )
            os.replace(partial, destination)
            return url, direct_metadata(url, size, digest.hexdigest()), None
    except BaseException as exc:
        return url, None, f"{type(exc).__name__}: {exc}"


def register_direct_batch(
    conn: sqlite3.Connection,
    esg: object,
    metadata_rows: list[dict[str, object]],
) -> int:
    from esgpull.models import Dataset, File, FileStatus
    import sqlalchemy as sa

    if not metadata_rows:
        return 0
    queries = ensure_bookkeeping_queries(esg, metadata_rows)
    for metadata in metadata_rows:
        incoming = File.fromdict(metadata)  # type: ignore[arg-type]
        incoming.compute_sha()
        dataset = esg.db.session.get(Dataset, incoming.dataset_id)
        if dataset is None:
            total_files = conn.execute(
                "SELECT COUNT(*) FROM manifest_url WHERE dataset_id_hint=?",
                (incoming.dataset_id,),
            ).fetchone()[0]
            esg.db.session.add(
                Dataset(dataset_id=incoming.dataset_id, total_files=total_files)
            )
        existing = esg.db.session.get(File, incoming.sha)
        if existing is None:
            existing = esg.db.session.scalar(
                sa.select(File).where(File.file_id == incoming.file_id)
            )
        if existing is None:
            incoming.status = FileStatus.Done
            existing = incoming
            esg.db.session.add(existing)
        else:
            if existing.checksum != incoming.checksum:
                raise RuntimeError(
                    f"checksum conflict for existing file {incoming.file_id}"
                )
            existing.url = incoming.url
            existing.size = incoming.size
            existing.local_path = incoming.local_path
            existing.status = FileStatus.Done
        key = bookkeeping_key(metadata)
        if key is None:
            raise RuntimeError(f"cannot map {incoming.dataset_id!r} to dashboard")
        esg.db.session.flush()
        esg.db.link(queries[key], existing)
    esg.db.session.commit()
    return len(metadata_rows)


async def direct_download_pending(
    conn: sqlite3.Connection,
    esg: object,
    concurrency: int,
    timeout: float,
    batch_size: int,
    limit_files: int | None,
) -> tuple[int, int]:
    import httpx

    sql = "SELECT url FROM manifest_url WHERE status='pending' ORDER BY url"
    params: tuple[object, ...] = ()
    if limit_files is not None:
        sql += " LIMIT ?"
        params = (limit_files,)
    urls = [row["url"] for row in conn.execute(sql, params)]
    semaphore = asyncio.Semaphore(concurrency)
    completed = failed = 0
    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(timeout),
        limits=limits,
    ) as client:
        for url_batch in chunked(urls, batch_size):
            results = await asyncio.gather(
                *[
                    download_manifest_url(
                        client, semaphore, esg.config.paths.data, url
                    )
                    for url in url_batch
                ]
            )
            successful: list[dict[str, object]] = []
            for url, metadata, error in results:
                if metadata is None:
                    failed += 1
                    conn.execute(
                        """UPDATE manifest_url SET error=?,
                           updated_at=CURRENT_TIMESTAMP WHERE url=?""",
                        (error, url),
                    )
                else:
                    successful.append(metadata)
                    conn.execute(
                        """UPDATE manifest_url SET status='resolved',
                           source='manifest-direct', metadata_json=?, error=NULL,
                           updated_at=CURRENT_TIMESTAMP WHERE url=?""",
                        (json.dumps(metadata, separators=(",", ":")), url),
                    )
            conn.commit()
            completed += register_direct_batch(conn, esg, successful)
            print(
                f"Manifest-direct: completed {completed:,}; failed {failed:,}",
                flush=True,
            )
    return completed, failed


def reconcile_manifest(
    conn: sqlite3.Connection,
    esg: object,
    report_path: Path,
    apply: bool,
) -> Counter[str]:
    """Make database membership and preferred URLs follow the manifest.

    Only non-downloaded queue entries outside the manifest are deleted. Done
    files are retained on disk and in the database, even when they are outside
    the manifest. This keeps pruning immediate but non-destructive to storage.
    """
    from esgpull.models import Dataset, File, FileStatus
    from esgpull.models.query import query_file_proxy
    import sqlalchemy as sa

    counts = state_counts(conn)
    unresolved = sum(value for key, value in counts.items() if key != "resolved")
    if unresolved:
        raise RuntimeError(
            f"manifest is not fully resolved ({unresolved:,} pending/invalid URLs); "
            "reconciliation would not be a safe source-of-truth operation"
        )

    metadata_rows = list(iter_resolved_metadata(conn))
    dataset_counts = Counter(str(row["dataset_id"]) for row in metadata_rows)
    manifest_shas: set[str] = set()
    for metadata in metadata_rows:
        incoming = File.fromdict(metadata)  # type: ignore[arg-type]
        incoming.compute_sha()
        manifest_shas.add(incoming.sha)

    actions: Counter[str] = Counter()
    surplus_shas: list[str] = []
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8", newline="") as report:
        report.write("action\tfile_sha\told_status\told_url\tmanifest_url\tfile_id\n")

        # Pass one is a complete, read-only preflight. It produces the same
        # report in dry-run and apply modes and detects conflicts before any
        # manifest file or queue record is changed.
        for metadata in metadata_rows:
            incoming = File.fromdict(metadata)  # type: ignore[arg-type]
            incoming.compute_sha()
            existing = esg.db.session.get(File, incoming.sha)
            if existing is None:
                existing = esg.db.session.scalar(
                    sa.select(File).where(File.file_id == incoming.file_id)
                )
            old_status = "" if existing is None else existing.status.value
            old_url = "" if existing is None else existing.url
            if existing is None:
                action = "insert_and_queue"
            elif existing.checksum != incoming.checksum:
                action = "checksum_conflict"
            elif existing.status == FileStatus.Done and existing.url == incoming.url:
                action = "keep_done"
            elif existing.status == FileStatus.Done:
                action = "replace_url_keep_done"
            elif existing.url != incoming.url:
                action = "replace_url_and_queue"
            else:
                action = "queue_manifest_file"
            actions[action] += 1
            report.write(
                "\t".join(
                    [action, incoming.sha, old_status, old_url, incoming.url, incoming.file_id]
                )
                + "\n"
            )

        # The manifest controls the pending queue. Files already downloaded are
        # intentionally not deleted here; only surplus queued work is removed.
        queued = esg.db.session.scalars(
            sa.select(File).where(File.status == FileStatus.Queued)
        )
        for existing in queued:
            if existing.sha in manifest_shas:
                continue
            surplus_shas.append(existing.sha)
            action = "remove_surplus_queued"
            actions[action] += 1
            report.write(
                "\t".join(
                    [action, existing.sha, existing.status.value, existing.url, "", existing.file_id]
                )
                + "\n"
            )
        esg.db.session.rollback()

    if not apply:
        return actions
    if actions["checksum_conflict"]:
        raise RuntimeError(
            f"refusing to apply: {actions['checksum_conflict']:,} checksum conflicts"
        )

    queries = ensure_bookkeeping_queries(esg, metadata_rows)
    for metadata in metadata_rows:
        incoming = File.fromdict(metadata)  # type: ignore[arg-type]
        incoming.compute_sha()
        existing = esg.db.session.get(File, incoming.sha)
        if existing is None:
            existing = esg.db.session.scalar(
                sa.select(File).where(File.file_id == incoming.file_id)
            )
        dataset_id = incoming.dataset_id
        dataset = esg.db.session.get(Dataset, dataset_id)
        if dataset is None:
            esg.db.session.add(
                Dataset(dataset_id=dataset_id, total_files=dataset_counts[dataset_id])
            )
        if existing is None:
            incoming.status = FileStatus.Queued
            existing = incoming
            esg.db.session.add(existing)
        else:
            existing.url = incoming.url
            if existing.status != FileStatus.Done:
                existing.status = FileStatus.Queued
        key = bookkeeping_key(metadata)
        if key is None:
            raise RuntimeError(f"cannot map {dataset_id!r} to a dashboard query")
        esg.db.session.flush()
        esg.db.link(queries[key], existing)

    for sha in surplus_shas:
        existing = esg.db.session.get(File, sha)
        if existing is None or existing.status != FileStatus.Queued:
            continue
        esg.db.session.execute(
            sa.delete(query_file_proxy).where(query_file_proxy.c.file_sha == sha)
        )
        esg.db.session.delete(existing)
    esg.db.session.commit()
    return actions


def make_esgpull(path: Path) -> object:
    from esgpull.esgpull import Esgpull
    from esgpull.tui import Verbosity

    esg = Esgpull(path=path, verbosity=Verbosity.Detail, safe=True, load_db=True)
    # Wait out short-lived writers rather than failing immediately. A common
    # external flock should still be used to serialize long-running jobs.
    from sqlalchemy import text

    esg.db.session.execute(text("PRAGMA busy_timeout = 60000"))
    return esg


def command_audit(args: argparse.Namespace) -> int:
    records, duplicates = read_manifest(args.manifest)
    errors = Counter(record.error for record in records if record.error)
    hosts = Counter(record.hostname for record in records)
    print(f"Unique manifest URLs: {len(records):,}")
    print(f"Duplicate lines ignored: {duplicates:,}")
    print(f"Valid URLs: {sum(1 for record in records if not record.error):,}")
    print(f"Invalid URLs: {sum(errors.values()):,}")
    for error, count in sorted(errors.items()):
        print(f"  {error}: {count:,}")
    print("Hostnames:")
    for hostname, count in hosts.most_common():
        print(f"  {hostname or '<missing>'}: {count:,}")
    invalid_count = export_invalid_urls(records, args.invalid_report)
    if invalid_count:
        print(f"Wrote {invalid_count:,} malformed URLs to {args.invalid_report}")
    return 0 if not errors else 2


def command_resolve(args: argparse.Namespace) -> int:
    records, duplicates = read_manifest(args.manifest)
    conn = connect_state(args.state)
    seed_state(conn, records)
    print(f"Loaded {len(records):,} unique URLs ({duplicates:,} duplicate lines ignored).")
    print_counts(conn)
    esg = make_esgpull(args.esgpull_root)
    count = resolve_from_database(conn, esg)
    print(f"Resolved {count:,} URLs from the existing ESGpull database.")
    print_counts(conn)
    if not args.database_only:
        resolved, searched = resolve_from_esgf(
            conn,
            esg,
            dataset_batch=args.dataset_batch,
            retries=args.retries,
            limit_batches=args.limit_batches,
        )
        print(f"Resolved {resolved:,} of {searched:,} searched URLs from ESGF metadata.")
    export_report(conn, args.report)
    print_counts(conn)
    print(f"Wrote audit report: {args.report}")
    return 0


def command_download(args: argparse.Namespace) -> int:
    records, duplicates = read_manifest(args.manifest)
    conn = connect_state(args.state)
    seed_state(conn, records)
    print(
        f"Synchronized checkpoint with {len(records):,} manifest URLs "
        f"({duplicates:,} duplicate lines ignored)."
    )
    counts = state_counts(conn)
    if not counts:
        raise SystemExit("State database is empty; run the resolve command first.")
    invalid = counts.get("invalid", 0)
    pending = counts.get("pending", 0)
    if invalid:
        raise SystemExit(
            f"Refusing to continue with {invalid:,} invalid URLs; correct the manifest first."
        )
    if pending and not args.direct_fallback and not args.allow_partial:
        raise SystemExit(
            f"Refusing to continue with {pending:,} unresolved URLs; inspect the "
            "report, pass --direct-fallback, or explicitly pass --allow-partial."
        )
    esg = make_esgpull(args.esgpull_root)
    registered, completed, failed = asyncio.run(
        register_and_download(
            conn,
            esg,
            batch_size=args.batch_size,
            register_only=args.register_only,
            limit_files=args.limit_files,
        )
    )
    action = "Registered" if args.register_only else "Processed"
    print(f"{action} {registered:,} files; downloaded {completed:,}; failed {failed:,}.")
    direct_completed = direct_failed = 0
    if args.direct_fallback:
        if args.register_only:
            raise SystemExit("--direct-fallback cannot be combined with --register-only")
        direct_completed, direct_failed = asyncio.run(
            direct_download_pending(
                conn,
                esg,
                concurrency=args.direct_concurrency,
                timeout=args.http_timeout,
                batch_size=args.direct_batch_size,
                limit_files=args.direct_limit_files,
            )
        )
        print(
            f"Manifest-direct completed {direct_completed:,}; "
            f"failed {direct_failed:,}."
        )
    return 1 if failed or direct_failed else 0


def command_reconcile(args: argparse.Namespace) -> int:
    records, duplicates = read_manifest(args.manifest)
    conn = connect_state(args.state)
    seed_state(conn, records)
    print(
        f"Synchronized checkpoint with {len(records):,} manifest URLs "
        f"({duplicates:,} duplicate lines ignored)."
    )
    esg = make_esgpull(args.esgpull_root)
    actions = reconcile_manifest(
        conn, esg, report_path=args.report, apply=args.apply
    )
    mode = "Applied" if args.apply else "Planned"
    print(f"{mode} manifest reconciliation:")
    for action, count in sorted(actions.items()):
        print(f"  {action}: {count:,}")
    print(f"Wrote reconciliation report: {args.report}")
    if not args.apply:
        print("No ESGpull database changes were made. Re-run with --apply after review.")
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--manifest", type=Path, default=Path("manifest.txt"))
    result.add_argument(
        "--state", type=Path, default=Path("outputs/manifest_state.sqlite")
    )
    sub = result.add_subparsers(dest="command", required=True)

    audit = sub.add_parser("audit", help="validate without ESGpull or network access")
    audit.add_argument(
        "--invalid-report",
        type=Path,
        default=Path("outputs/manifest_invalid_urls.tsv"),
    )
    audit.set_defaults(func=command_audit)

    resolve = sub.add_parser("resolve", help="resolve exact URLs to trusted metadata")
    resolve.add_argument("--esgpull-root", type=Path, required=True)
    resolve.add_argument("--database-only", action="store_true")
    resolve.add_argument("--dataset-batch", type=int, default=20)
    resolve.add_argument("--retries", type=int, default=4)
    resolve.add_argument("--limit-batches", type=int, help="small smoke-test limit")
    resolve.add_argument(
        "--report", type=Path, default=Path("outputs/manifest_resolution.tsv")
    )
    resolve.set_defaults(func=command_resolve)

    reconcile = sub.add_parser(
        "reconcile", help="plan or apply manifest-as-source-of-truth database changes"
    )
    reconcile.add_argument("--esgpull-root", type=Path, required=True)
    reconcile.add_argument(
        "--report",
        type=Path,
        default=Path("outputs/manifest_reconciliation.tsv"),
    )
    reconcile.add_argument(
        "--apply",
        action="store_true",
        help="apply the reviewed plan; default is a read-only dry run",
    )
    reconcile.set_defaults(func=command_reconcile)

    download = sub.add_parser("download", help="register and download resolved URLs")
    download.add_argument("--esgpull-root", type=Path, required=True)
    download.add_argument("--batch-size", type=int, default=100)
    download.add_argument("--register-only", action="store_true")
    download.add_argument("--allow-partial", action="store_true")
    download.add_argument("--limit-files", type=int, help="small smoke-test limit")
    download.add_argument(
        "--direct-fallback",
        action="store_true",
        help="download pending URLs directly and register computed SHA-256 metadata",
    )
    download.add_argument("--direct-concurrency", type=int, default=8)
    download.add_argument("--direct-batch-size", type=int, default=100)
    download.add_argument("--direct-limit-files", type=int)
    download.add_argument("--http-timeout", type=float, default=120.0)
    download.set_defaults(func=command_download)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
