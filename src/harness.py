#!/usr/bin/env python3
"""S3 Bench Harness — v1.6.4
Fix: use correct `mc alias set --path on` (not bare --path).
Keeps: S3v4, path‑style for IP endpoints, explicit --region on mb, fail‑fast on errors, NDJSON error/attempts.
"""
from __future__ import annotations

import json
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Optional

import tomli
from credentials import CredentialResolver

@dataclass(frozen=True)
class Provider:
    id: str
    namespace: str
    endpoint: str
    region: str
    bucket: str
    insecure_ssl: bool = False
    profile: Optional[str] = None

class S3Harness:
    def __init__(self, *, config_file: str = "config.toml", provider_override: Optional[str] = None, profile: Optional[str] = None):
        self.config_path = Path(config_file)
        with open(self.config_path, "rb") as f:
            cfg = tomli.load(f)
        self.cfg = cfg
        self.dataset = cfg["dataset"]
        self.test = cfg.get("test", {})
        self.data_path = Path(self.dataset["data_path"]).resolve()
        self.data_path.mkdir(parents=True, exist_ok=True)

        prov = self._resolve_provider(cfg, provider_override)
        self.provider = Provider(
            id=prov["id"],
            namespace=prov["namespace"],
            endpoint=prov["endpoint"],
            region=prov.get("region", "us-east-1"),
            bucket=prov.get("bucket") or prov.get("bucket_prefix", "bench"),
            insecure_ssl=bool(prov.get("insecure_ssl", False)),
            profile=prov.get("profile"),
        )

        eff_profile = profile or self.provider.profile
        self.creds = CredentialResolver().resolve(namespace=self.provider.namespace, profile=eff_profile)
        self.alias = f"alias_{self.provider.id}"

        manifest = self.data_path / "manifest.json"
        if not manifest.exists():
            raise FileNotFoundError(f"manifest.json not found at {manifest}")

    # ----- helpers -----
    def _run(self, cmd: List[str], *, timeout: Optional[int] = None) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)

    def _mc(self, *args: str, timeout: Optional[int] = None) -> subprocess.CompletedProcess:
        cmd = ["mc", *args]
        if self.provider.insecure_ssl and "--insecure" not in cmd:
            cmd.append("--insecure")
        return self._run(cmd, timeout=timeout)

    def _alias_set(self) -> None:
        # Force S3v4 and PATH style for IP endpoints: `--path on`
        args = [
            "alias", "set", self.alias,
            self.provider.endpoint, self.conversion_safe(self.creds.access_key), self.conversion_safe(self.creds.secret_key),
            "--api", "S3v4", "--path", "on"
        ]
        if self.provider.insecure_ssl:
            args.append("--insecure")
        r = self._mc(*args)
        if r.returncode != 0:
            raise RuntimeError(f"mc alias set failed: {r.stderr.strip()}")

    def _alias_rm(self) -> None:
        self._mc("alias", "remove", self.alias)

    def _mb(self) -> None:
        # Explicit region helps some S3‑compatible services
        r = self._mc("mb", f"{self.alias}/{self.provider.bucket}", "--region", self.provider.region)
        if r.returncode != 0:
            s = (r.stderr or "").lower()
            benign = ("already exists" in s) or ("already owned by you" in s) or ("bucketalreadyownedbyyou" in s)
            if not benign:
                raise RuntimeError(f"Bucket create failed for {self.provider.bucket}: {r.stderr.strip()}")

    def _put(self, src: Path, key: str, timeout: int) -> subprocess.CompletedProcess:
        return self._mc("cp", str(src), f"{self.alias}/{self.provider.bucket}/{key}", timeout=timeout)

    def _get(self, key: str, timeout: int) -> subprocess.CompletedProcess:
        with tempfile.TemporaryDirectory() as td:
            dest = Path(td) / Path(key).name
            return self._mc("cp", f"{self.alias}/{self.provider.bucket}/{key}", str(dest), timeout=timeout)

    def _list(self, prefix: str, timeout: int) -> subprocess.CompletedProcess:
        return self._mc("ls", f"{self.alias}/{self.provider.bucket}/{prefix}", timeout=timeout)

    def _head(self, key: str, timeout: int) -> subprocess.CompletedProcess:
        return self._mc("stat", f"{self.alias}/{self.provider.bucket}/{key}", timeout=timeout)

    def _delete(self, key: str, timeout: int) -> subprocess.ProgressBar:
        return self._mc("rm", f"{self.alias}/{self.provider.bucket}/{key}", timeout=timeout)

    # ensure secrets with special chars are passed safely
    @staticmethod
    def conversion_safe(s: str) -> str:
        return s

    # ----- run -----
    def run(self) -> int:
        iterations = int(self.test.get("iterations", 1))
        ops: List[str] = list(self.test.get("operations", ["PUT", "GET", "LIST", "HEAD", "DELETE"]))
        warmups = int(self.test.get("warmup_operations", 0))
        retries = int(self.test.get("retry_attempts", 0))
        timeout = int(self.test.get("timeout_seconds", 300))
        cleanup = bool(self.test.get("cleanup_after_run", True))

        ndjson_path = self.data_path / f"{self.provider.id}.ndjson"
        self._alias_set()
        try:
            self._mb()
            manifest = json.loads((self.data_path / "manifest.json").read_text())
            files = manifest.get("files", [])
            records = [(f["path"], int(f["size"]), self.data_path / f["path"]) for f in files]

            def do(op: str, iteration: int, key: str, size: int, src: Path) -> Dict[str, Any]:
                attempt = 0
                start_ns = time.perf_counter_ns()
                last: Optional[subprocess.CompletedProcess] = None
                rc = 1
                while attempt <= retries:
                    if op == "PUT":
                        last = self._put(src, key, timeout)
                    elif op == "GET":
                        last = self._get(key, timeout)
                    elif op == "LIST":
                        last = self._list(Path(key).parent.as_posix(), timeout)
                    elif op == "HEAD":
                        last = self._head(key, timeout)
                    elif op == "DELETE":
                        last = self._delete(key, timeout)
                    else:
                        return {"provider": self.provider.id, "op": op, "iteration": iteration, "duration_ms": 0.0, "bytes": 0, "exit_code": 1, "attempts": attempt+1, "error": f"unknown op: {op}"}
                    rc = last.returncode
                    if rc == 0:
                        break
                    attempt += 1
                dur_ms = (time.perf_counter_ns() - start_ns) / 1e6
                err = (last.stderr or "") if last else ""
                err_last = err.strip().splitlines()[-1] if err.strip().splitlines() else ""
                return {
                    "provider": self.provider.id,
                    "op": op,
                    "iteration": iteration,
                    "duration_ms": dur_ms,
                    "bytes": size if op in ("PUT", "GET") else 0,
                    "exit_code": rc,
                    "attempts": attempt + 1,
                    "error": err_last,
                }

            for _ in range(warmups):
                for op in ops:
                    for key, size, src in records[:1]:
                        _ = do(op, 0, key, size, src)

            with ndjson_path.open("w") as out:
                for it in range(1, iterations + 1):
                    for op in ops:
                        for key, size, src in records:
                            rec = do(op, it, key, size, src)
                            out.write(json.dumps(rec) + "\n")

            if cleanup:
                for key, _, _ in records:
                    self._delete(key, timeout)

            return 0
        finally:
            self._alias_rm()

    def _resolve_provider(self, cfg: Dict[str, Any], override: Optional[str]) -> Dict[str, Any]:
        if "providers" in cfg and isinstance(cfg["providers"], list):
            items: List[Dict[str, Any]] = cfg["providers"]
            if override:
                for p in items:
                    if p.get("id") == override:
                        return p
                raise RuntimeError(f"Unknown provider id: {override}")
            return items[0]
        prov = cfg.get("provider")
        if not isinstance(prov, dict):
            raise RuntimeError("config.toml missing [provider] or [[providers]]")
        if "namespace" not in prov:
            raise RuntimeError("provider missing 'namespace' in config.toml")
        return prov
