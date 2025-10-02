#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Coletor de commits por usuário (autor) agregados por dia (UTC),
gravando 1 arquivo canônico por dia/usuário em S3:
  s3://<bucket>/<prefix>/<user_key>/dt=YYYY-MM-DD/commits-<org>-<YYYY-MM-DD>.jsonl.gz

- Dedup na origem por SHA (dentro do run) e ao mesclar com o arquivo existente.
- Sem concorrência: sobrescreve o arquivo diário apenas quando houver novidades.
"""

import os
import sys
import json
import time
import io
import gzip
import shutil
import requests
from datetime import datetime, timezone
from collections import defaultdict
from urllib.parse import urlencode

import boto3
from botocore.exceptions import ClientError

# -------- Config via ENV --------
GITHUB_API   = os.getenv("GITHUB_API", "https://api.github.com")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GH_ORG        = os.getenv("GH_ORG")             # org
SINCE        = os.getenv("SINCE")             # ISO 8601 (ex: 2025-02-01T00:00:00Z)
UNTIL        = os.getenv("UNTIL")             # ISO 8601 (ex: 2025-02-15T00:00:00Z)
REPO_FILTER  = os.getenv("REPO_FILTER", "")   # substring opcional para filtrar repos
MAX_REPOS = int(os.getenv("MAX_REPOS", "0"))  # 0 = sem limite (útil p/ teste)

# S3 (modo arquivo único por dia)
S3_BUCKET    = os.getenv("S3_BUCKET")         # ex: my-bucket
S3_PREFIX    = os.getenv("S3_PREFIX", "github/commits")  # ex: github/commits

# -------- Validações básicas --------
missing = []
for var in ("GITHUB_TOKEN", "GH_ORG", "SINCE", "UNTIL", "S3_BUCKET", "S3_PREFIX"):
    if not globals().get(var):
        missing.append(var)
if missing:
    print(f"[ERRO] Missing env: {', '.join(missing)}", file=sys.stderr)
    sys.exit(2)

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
})

def gh_paged(url, params=None):
    params = params or {}
    per_page = 100
    page = 1
    while True:
        qp = params.copy()
        qp.update({"per_page": per_page, "page": page})
        r = session.get(url, params=qp, timeout=60)
        # backoff simples para secondary rate limit
        if r.status_code == 403 and "secondary rate limit" in r.text.lower():
            time.sleep(15)
            continue
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        yield from data
        if len(data) < per_page:
            break
        page += 1

def list_repos(owner):
    url = f"{GITHUB_API}/orgs/{owner}/repos"
    for repo in gh_paged(url, {"type": "all", "sort": "full_name"}):
        name = repo["name"]
        if REPO_FILTER and REPO_FILTER not in name:
            continue
        yield name

def list_branches(owner, repo):
    url = f"{GITHUB_API}/repos/{owner}/{repo}/branches"
    for br in gh_paged(url):
        yield br["name"]

def list_commits(owner, repo, sha, since, until):
    url = f"{GITHUB_API}/repos/{owner}/{repo}/commits"
    for c in gh_paged(url, {"sha": sha, "since": since, "until": until}):
        yield c

def normalize_user_key(commit):
    """
    Chave de usuário para agregação:
    - Preferimos author.login; fallback para e-mail do author do commit.
    - Se nada existir, gera um marcador 'unknown:<hash curto>'.
    """
    author = commit.get("author")  # user object (pode ser None)
    commit_author = commit.get("commit", {}).get("author", {})  # metadados do commit
    login = author["login"] if author and author.get("login") else None
    email = (commit_author.get("email") or "").lower()

    if login:
        return f"github:{login}"
    if email:
        return f"email:{email}"
    # fallback
    raw = json.dumps(commit.get("commit", {}), sort_keys=True)
    import hashlib
    return "unknown:" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]

def commit_utc_date(commit):
    dt = commit.get("commit", {}).get("author", {}).get("date")
    if not dt:
        return "unknown-date"
    try:
        d = datetime.fromisoformat(dt.replace("Z","+00:00")).astimezone(timezone.utc)
        return d.strftime("%Y-%m-%d")
    except Exception:
        return "unknown-date"

# -------- S3 helpers (arquivo único por dia) --------
s3 = boto3.client("s3")

def s3_key_daily(user_key, day_str):
    # arquivo canônico comprimido
    return f"{S3_PREFIX}/{user_key}/dt={day_str}/commits-{GH_ORG}-{day_str}.jsonl.gz"

def load_daily_records(user_key, day_str):
    """
    Lê o arquivo diário do S3 (se existir) e retorna dict[sha] = record.
    """
    key = s3_key_daily(user_key, day_str)
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = gzip.decompress(obj["Body"].read()).decode("utf-8")
        d = {}
        for line in data.splitlines():
            if not line.strip():
                continue
            try:
                doc = json.loads(line)
                sha = doc.get("sha")
                if sha:
                    d[sha] = doc
            except Exception:
                continue
        return d
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return {}
        raise

def save_daily_records(user_key, day_str, recs_by_sha):
    """
    Sobrescreve o arquivo diário em S3 com os registros (JSONL gzip).
    """
    key = s3_key_daily(user_key, day_str)
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="w") as gz:
        # opcional: ordenar para estabilidade (por repo+sha)
        for sha, r in sorted(recs_by_sha.items(), key=lambda kv: (kv[1].get("repo",""), kv[0])):
            gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
    buf.seek(0)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/gzip",
        ContentEncoding="gzip"
    )

# -------- Exec principal --------
def main():
    repos = list(list_repos(GH_ORG))
    if MAX_REPOS > 0:
        repos = repos[:MAX_REPOS]

    total_seen = 0

    # bucket[user_key][day] = [records...]
    bucket = defaultdict(lambda: defaultdict(list))

    for repo in repos:
        print(f"[INFO] Repo: {repo}", file=sys.stderr)
        seen_shas = set()
        # listar branches
        try:
            branches = list(list_branches(GH_ORG, repo))
        except requests.HTTPError as e:
            print(f"[WARN] branches fail {repo}: {e}", file=sys.stderr)
            continue

        for br in branches:
            try:
                for c in list_commits(GH_ORG, repo, br, SINCE, UNTIL):
                    sha = c.get("sha")
                    if not sha or sha in seen_shas:
                        continue
                    seen_shas.add(sha)

                    user_key = normalize_user_key(c)
                    day = commit_utc_date(c)

                    record = {
                        "sha": sha,
                        "repo": f"{GH_ORG}/{repo}",
                        "branch": br,
                        "author_login": (c.get("author") or {}).get("login"),
                        "author_email": (c.get("commit", {}).get("author", {}) or {}).get("email"),
                        "author_name": (c.get("commit", {}).get("author", {}) or {}).get("name"),
                        "committer_login": (c.get("committer") or {}).get("login"),
                        "committer_email": (c.get("commit", {}).get("committer", {}) or {}).get("email"),
                        "committer_name": (c.get("commit", {}).get("committer", {}) or {}).get("name"),
                        "message": (c.get("commit") or {}).get("message"),
                        "timestamp": (c.get("commit", {}).get("author", {}) or {}).get("date"),
                        "url": c.get("html_url"),
                        "is_merge": True if (c.get("parents") and len(c["parents"]) > 1) else False,
                        "verified": (c.get("commit", {}).get("verification", {}) or {}).get("verified", False),
                    }
                    bucket[user_key][day].append(record)
                    total_seen += 1
            except requests.HTTPError as e:
                print(f"[WARN] commits fail {repo}@{br}: {e}", file=sys.stderr)
                continue

    # Merge por user_key/dia com arquivo diário no S3
    touched = 0
    for user_key, days in bucket.items():
        for day, recs in days.items():  # day = 'YYYY-MM-DD'
            existing = load_daily_records(user_key, day)
            before = len(existing)

            # mescla: mantém primeiro o que existe, depois adiciona novos shas
            merged = dict(existing)
            for r in recs:
                sha = r.get("sha")
                if sha and sha not in merged:
                    merged[sha] = r

            if len(merged) != before:
                save_daily_records(user_key, day, merged)
                touched += 1

    print(f"[DONE] Commits únicos vistos nesta coleta: {total_seen}", file=sys.stderr)
    print(f"[DONE] Arquivos diários atualizados em S3: {touched}", file=sys.stderr)

if __name__ == "__main__":
    main()
