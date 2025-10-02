#!/usr/bin/env python3
import os, sys, json, time, hashlib
from datetime import datetime, timezone
from urllib.parse import urlencode
import requests
from collections import defaultdict

GITHUB_API = os.getenv("GITHUB_API", "https://api.github.com")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GH_ORG = os.getenv("GH_ORG")  # org
SINCE = os.getenv("SINCE")  # ISO 8601
UNTIL = os.getenv("UNTIL")  # ISO 8601
REPO_FILTER = os.getenv("REPO_FILTER", "")  # opcional: regex simples (contém)
OUTDIR = os.getenv("OUTDIR", "out")  # pasta local temporária antes de subir ao S3
MAX_REPOS = int(os.getenv("MAX_REPOS", "0"))  # 0 = sem limite (útil p/ testes)

if not GITHUB_TOKEN or not GH_ORG or not SINCE or not UNTIL:
    print("Missing env: GITHUB_TOKEN, GH_ORG, SINCE, UNTIL", file=sys.stderr)
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
        if r.status_code == 403 and "secondary rate limit" in r.text.lower():
            # backoff básico
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
    # Importante: buscar por branch (sha) e janela de tempo
    for c in gh_paged(url, {"sha": sha, "since": since, "until": until}):
        yield c

def normalize_user_key(commit):
    """
    Retorna uma chave de usuário para agregação:
    - Preferimos author.login quando disponível (commit feito por conta GitHub)
    - Se não houver, usamos o e-mail do autor (podendo ser 'user@users.noreply.github.com')
    """
    author = commit.get("author")  # user object (pode ser None)
    commit_author = commit.get("commit", {}).get("author", {})  # metadados do commit
    login = author["login"] if author and author.get("login") else None
    email = (commit_author.get("email") or "").lower()

    if login:
        return f"github:{login}"
    if email:
        return f"email:{email}"
    # fallback extremo: hash do nome+msg
    raw = json.dumps(commit.get("commit", {}), sort_keys=True)
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

def main():
    os.makedirs(OUTDIR, exist_ok=True)
    repos = list(list_repos(GH_ORG))
    if MAX_REPOS > 0:
        repos = repos[:MAX_REPOS]

    total_seen = 0
    for repo in repos:
        print(f"[INFO] Repo: {repo}", file=sys.stderr)
        seen_shas = set()
        # listar branches
        try:
            branches = list(list_branches(GH_ORG, repo))
        except requests.HTTPError as e:
            print(f"[WARN] branches fail {repo}: {e}", file=sys.stderr)
            continue

        # agregador: user_key -> date (YYYY-MM-DD) -> [records]
        bucket = defaultdict(lambda: defaultdict(list))

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
                        "added": c.get("stats", {}).get("additions"),  # geralmente ausente aqui
                        "removed": c.get("stats", {}).get("deletions"),
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

        # escreve arquivos por user_key/day
        part = int(time.time())
        for user_key, days in bucket.items():
            safe_user = user_key.replace("/", "_")
            for day, recs in days.items():
                subdir = os.path.join(OUTDIR, safe_user, f"dt={day}")
                os.makedirs(subdir, exist_ok=True)
                outpath = os.path.join(subdir, f"commits-{day}-part-{part}.jsonl")
                with open(outpath, "w", encoding="utf-8") as f:
                    for r in recs:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"[DONE] Commits únicos coletados: {total_seen}", file=sys.stderr)

if __name__ == "__main__":
    main()
