#!/usr/bin/env python3
"""
Production-grade S3 benchmarking harness using MinIO CLI (mc).

Design:
- Deterministic dataset under dataset.data_path
- Stable bucket name = provider.bucket_prefix (created if missing, else reused)
- Run-scoped object prefix = run_id/ (no bucket churn)
- Provider-agnostic key mapping from local files → object keys via KeyBuilder
- HEAD targets derived from local dataset (not mc ls), eliminating ambiguity
- NDJSON metrics at <dataset.data_path>/<provider>.ndjson (manifest SHA-256 linked)
"""
from __future__ import annotations

import json
import subprocess
import tempfile
import time
import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import tomli
from dotenv import dotenv_values
import structlog

logger = structlog.get_logger()


@dataclass(frozen=True)
class KeyBuilder:
    """Maps local dataset files to provider object keys under a run-scoped prefix."""
    run_prefix: str            # e.g., 20251020T142233Z
    base_path: Path            # dataset root (absolute)

    def object_key(self, local_file: Path) -> str:
        """Return object key relative to bucket (run_prefix/relative_path)."""
        rel = local_file.relative_to(self.base_path).as_posix()
        return f"{self.run_prefix}/{rel}"


class S3Harness:
    def __init__(
        self,
        config_file: str = "config.toml",
        env_file: str = ".env",
        provider_override: Optional[str] = None,
    ) -> None:
        # ---- config ----
        with open(config_file, "rb") as f:
            cfg = tomli.load(f)
        self.cfg = cfg
        self.dataset = cfg["dataset"]
        self.provider = cfg["provider"].copy()
        self.test = cfg["test"].copy()
        if provider_override:
            self.provider["name"] = provider_override

        # ---- credentials ----
        env = dotenv_values(env_file)
        prefix = {
            "impossible_cloud": "IC_",
            "aws": "AWS_",
            "akave": "AKAVE_",
        }.get(self.provider["name"], "S3_")
        self.access_key = env.get(f"{prefix}ACCESS_KEY")
        self.secret_key = env.get(f"{prefix}SECRET_KEY")
        self.session_token = env.get(f"{prefix}SESSION_TOKEN")
        if not self.access_key or not self.secret_key:
            raise RuntimeError(
                f"Missing credentials for provider {self.provider['name']} with prefix {prefix} in {env_file}"
            )

        # ---- dataset root ----
        self.data_path = Path(self.dataset["data_path"]).resolve()
        self.manifest_path = self.data_path / "manifest.json"
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"manifest.json not found at {self.manifest_path}")
        self.manifest_sha256 = self._sha256_file(self.manifest_path)

        # ---- metrics output ----
        self.provider_name = self.provider["name"]
        self.ndjson_path = self.data_path / f"{self.provider_name}.ndjson"

        # ---- runtime naming ----
        self.run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.bucket = self.provider.get("bucket_prefix", "ic-bench")  # stable bucket
        self.mc_alias = "bench"

        # key builder (provider-agnostic)
        self.kb = KeyBuilder(run_prefix=self.run_id, base_path=self.data_path)

        # ---- stats ----
        self.total_bytes = sum(p.stat().st_size for p in self.data_path.rglob("*") if p.is_file())
        self.total_files = sum(1 for _ in self.data_path.rglob("*") if _.is_file())

        # ---- scratch ----
        self.tmpdir = Path(tempfile.mkdtemp(prefix="s3bench_"))

    # ---------------- utils ----------------
    def _sha256_file(self, path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def _run(self, *cmd: str, timeout: Optional[int] = None) -> Tuple[int, float, str, str]:
        start = time.perf_counter()
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        dur_ms = (time.perf_counter() - start) * 1000.0
        return proc.returncode, dur_ms, proc.stdout, proc.stderr

    def _log_event(
        self,
        op: str,
        iteration: int,
        attempt: int,
        rc: int,
        dur_ms: float,
        extra: Dict[str, Any] | None = None,
    ) -> None:
        event = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "run_id": self.run_id,
            "provider": self.provider_name,
            "bucket": self.bucket,
            "prefix": self.kb.run_prefix,
            "op": op,
            "iteration": iteration,
            "attempt": attempt,
            "duration_ms": round(dur_ms, 3),
            "exit_code": rc,
            "bytes": self.total_bytes if op in {"PUT", "GET", "DELETE"} else 0,
            "files": self.total_files,
            "manifest_sha256": self.manifest_sha256,
            "seed": self.dataset.get("seed"),
        }
        if extra:
            event.update(extra)
        with open(self.ndjson_path, "a") as f:
            json.dump(event, f)
            f.write("\n")

    # ---------------- setup ----------------
    def setup(self) -> None:
        logger.info("setup_start", endpoint=self.provider["endpoint"], region=self.provider["region"], bucket=self.bucket)
        # alias
        cmd = [
            "mc",
            "alias",
            "set",
            self.mc_alias,
            self.provider["endpoint"],
            self.access_key,
            self.secret_key,
            "--api",
            "s3v4",
        ]
        if self.session_token:
            cmd += ["--session-token", self.session_token]
        rc, _, out, err = self._run(*cmd, timeout=self.test["timeout_seconds"])
        if rc != 0:
            raise RuntimeError(
                f"mc alias set failed: rc={rc} stderr_tail={err[-400:]} stdout_tail={out[-200:]}"
            )

        # ensure bucket exists (create only if missing)
        if not self._bucket_exists():
            rc, _, out, err = self._run(
                "mc",
                "mb",
                f"{self.mc_alias}/{self.bucket}",
                "--region",
                self.provider["region"],
                timeout=self.test["timeout_seconds"],
            )
            already = "already exists, and you own it" in (err or "")
            if rc != 0 and not already:
                raise RuntimeError(
                    f"mc mb failed for {self.bucket}: rc={rc} stderr_tail={err[-400:]} stdout_tail={out[-200:]}"
                )
            logger.info("bucket_created_or_exists", bucket=self.bucket)
        else:
            logger.info("bucket_exists", bucket=self.bucket)

    def _bucket_exists(self) -> bool:
        rc, _, _, _ = self._run(
            "mc", "ls", f"{self.mc_alias}/{self.bucket}", "--json", timeout=self.test["timeout_seconds"]
        )
        if rc == 0:
            return True
        rc2, _, _, _ = self._run("mc", "ls", f"{self.mc_alias}/{self.bucket}", timeout=self.test["timeout_seconds"])
        return rc2 == 0

    # ---------------- warmup ----------------
    def warmup(self) -> None:
        n = int(self.test.get("warmup_operations", 0))
        if n <= 0:
            return
        logger.info("warmup_start", count=n)
        target = f"{self.mc_alias}/{self.bucket}/{self.kb.run_prefix}/"
        for w in range(1, n + 1):
            self._run("mc", "cp", "--recursive", f"{self.data_path}/", target, timeout=self.test["timeout_seconds"])
            self._run("mc", "rm", "--recursive", "--force", target, timeout=self.test["timeout_seconds"])
            logger.info("warmup_done", index=w)
        logger.info("warmup_complete")

    # ---------------- core ops ----------------
    def _retry(self, op: str, iteration: int, cmd: List[str], fatal: bool = True) -> int:
        retries = int(self.test.get("retry_attempts", 0))
        last_rc = 0
        for attempt in range(1, retries + 2):
            rc, dur, out, err = self._run(*cmd, timeout=self.test["timeout_seconds"])
            self._log_event(op, iteration, attempt, rc, dur, extra={"stdout_tail": out[-200:], "stderr_tail": err[-200:]})
            if rc == 0:
                return 0
            last_rc = rc
            time.sleep(attempt)
        if fatal:
            raise RuntimeError(f"{op} failed after {retries} retries")
        logger.info("nonfatal_op_failed", op=op, iteration=iteration, rc=last_rc)
        return last_rc

    def _iter_local_files(self, limit: int | None = None) -> List[Path]:
        files: List[Path] = []
        for p in self.data_path.rglob("*"):
            if p.is_file():
                files.append(p)
                if limit and len(files) >= limit:
                    break
        return files

    def run(self) -> None:
        iters = int(self.test.get("iterations", 1))
        target = f"{self.mc_alias}/{self.bucket}/{self.kb.run_prefix}/"

        for i in range(1, iters + 1):
            logger.info("iteration_start", i=i, total=iters, op="PUT")
            self._retry("PUT", i, ["mc", "cp", "--recursive", f"{self.data_path}/", target], fatal=True)

            logger.info("iteration_progress", i=i, op="LIST")
            self._retry("LIST", i, ["mc", "ls", "--recursive", "--json", target], fatal=True)

            # HEAD targets derived from local dataset → exact object keys
            head_files = self._iter_local_files(limit=10)
            for lf in head_files:
                key = self.kb.object_key(lf)
                stat_path = f"{self.mc_alias}/{self.bucket}/{key}"
                self._retry("HEAD", i, ["mc", "stat", "--json", stat_path], fatal=False)

            logger.info("iteration_progress", i=i, op="GET")
            dl_dir = self.tmpdir / f"dl_{i}"
            dl_dir.mkdir(parents=True, exist_ok=True)
            self._retry("GET", i, ["mc", "cp", "--recursive", target, str(dl_dir)], fatal=True)
            for p in dl_dir.rglob("*"):
                if p.is_file():
                    p.unlink()

            logger.info("iteration_progress", i=i, op="DELETE")
            self._retry("DELETE", i, ["mc", "rm", "--recursive", "--force", target], fatal=True)
            logger.info("iteration_complete", i=i)

    # ---------------- teardown ----------------
    def teardown(self) -> None:
        for p in self.tmpdir.rglob("*"):
            if p.is_file():
                p.unlink()
        try:
            self.tmpdir.rmdir()
        except Exception:
            pass
        logger.info(
            "run_complete",
            provider=self.provider_name,
            bucket=self.bucket,
            prefix=self.kb.run_prefix,
            ndjson=str(self.ndjson_path),
        )


__all__ = ["S3Harness"]
