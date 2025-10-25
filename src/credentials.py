#!/usr/bin/env python3
"""Credential resolution (v1.6.0)
Provider-namespace credentials using ONLY environment variables (.env exported by Ansible).
Order: --profile/AWS_PROFILE → <NAMESPACE>_ACCESS_KEY/SECRET_KEY → AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY.
"""
from __future__ import annotations

import os
import configparser
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Mapping


@dataclass(frozen=True)
class Credentials:
    access_key: str
    secret_key: str
    session_token: Optional[str] = None


class CredentialResolver:
    def __init__(self, env: Optional[Mapping[str, str]] = None) -> None:
        self._env = dict(os.environ) if env is None else dict(env)

    def _from_profile(self, profile: Optional[str]) -> Optional[Credentials]:
        if not profile:
            return None
        creds_path = Path(os.path.expanduser("~/.aws/credentials"))
        cfg_path = Path(os.path.expanduser("~/.aws/config"))
        cp = configparser.ConfigParser()
        if creds_path.exists():
            cp.read(creds_path)
        if cfg_path.exists():
            cp.read(cfg_path)
        for sect in (profile, f"profile {profile}"):
            if cp.has_section(sect):
                ak = cp.get(sect, "aws_access_key_id", fallback=None)
                sk = cp.get(sect, "aws_secret_access_key", fallback=None)
                st = cp.get(sect, "aws_session_token", fallback=None)
                if ak and sk:
                    return Credentials(ak, sk, st)
        return None

    def _from_namespace_env(self, ns: str) -> Optional[Credentials]:
        ak = self._env.get(f"{ns}_ACCESS_KEY") or self._env.get(f"{ns}_ACCESS_KEY_ID")
        sk = self._env.get(f"{ns}_SECRET_KEY") or self._env.get(f"{ns}_SECRET_ACCESS_KEY")
        st = self._env.get(f"{ns}_SESSION_TOKEN")
        if ak and sk:
            return Credentials(ak, sk, st)
        return None

    def _from_aws_env(self) -> Optional[Credentials]:
        ak = self._env.get("AWS_ACCESS_KEY_ID") or self._env.get("AWS_ACCESS_KEY")
        sk = self._env.get("AWS_SECRET_ACCESS_KEY") or self._env.get("AWS_SECRET_KEY")
        st = self._env.get("AWS_SESSION_TOKEN")
        if ak and sk:
            return Credentials(ak, sk, st)
        return None

    def resolve(self, *, namespace: str, profile: Optional[str]) -> Credentials:
        c = self._from_profile(profile)
        if c:
            return c
        c = self._from_namespace_env(namespace)
        if c:
            return c
        c = self._from_aws_env()
        if c:
            return c
        raise RuntimeError(
            f"Missing credentials for namespace '{namespace}'. Set {namespace}_ACCESS_KEY and {namespace}_SECRET_KEY, or provide --profile / AWS_PROFILE."
        )
