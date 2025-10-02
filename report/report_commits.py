#!/usr/bin/env python3
import os, sys, csv, json, subprocess, tempfile, shutil
from datetime import datetime, timedelta

BUCKET = os.getenv("S3_BUCKET")
PREFIX = os.getenv("S3_PREFIX", "github/commits")
START = os.getenv("START_DATE")   # YYYY-MM-DD
END = os.getenv("END_DATE")       # YYYY-MM-DD (exclusive end OK; vamos incluir o dia)
USER_KEY = os.getenv("USER_KEY", "").strip()  # ex: github:joao ou email:foo@bar
OUT_CSV = os.getenv("OUT_CSV", "report.csv")

if not BUCKET or not START or not END:
    print("Missing env: S3_BUCKET, START_DATE, END_DATE", file=sys.stderr)
    sys.exit(2)

def daterange(start_date, end_date_inclusive):
    cur = start_date
    while cur <= end_date_inclusive:
        yield cur
        cur += timedelta(days=1)

def run(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        # aws s3 cp em caminho inexistente retorna !=0; seguimos adiante
        pass
    return res

def list_users():
    # Lista "diretórios" de usuários: s3://bucket/prefix/<user>/
    # Saída do aws s3 ls: linhas com "                           PRE github:joao/"
    cmd = f'aws s3 ls "s3://{BUCKET}/{PREFIX}/"'
    res = run(cmd)
    users = []
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.endswith("/") and "PRE " in line:
            u = line.split("PRE ", 1)[1].rstrip("/")
            users.append(u)
    return users

def download_day_for_user(tmpdir, user_key, day):
    s3path = f"s3://{BUCKET}/{PREFIX}/{user_key}/dt={day.strftime('%Y-%m-%d')}/"
    locdir = os.path.join(tmpdir, user_key.replace("/", "_"), f"dt={day.strftime('%Y-%m-%d')}")
    os.makedirs(locdir, exist_ok=True)
    cmd = f'aws s3 cp "{s3path}" "{locdir}/" --recursive --only-show-errors'
    run(cmd)
    return locdir

def main():
    start_date = datetime.strptime(START, "%Y-%m-%d").date()
    end_date = datetime.strptime(END, "%Y-%m-%d").date()

    tmpdir = tempfile.mkdtemp(prefix="commits_dl_")
    rows = []

    try:
        users = [USER_KEY] if USER_KEY else list_users()

        for u in users:
            for day in daterange(start_date, end_date):
                locdir = download_day_for_user(tmpdir, u, day)
                # ler todos jsonl do diretório
                for root, _, files in os.walk(locdir):
                    for fn in files:
                        if not fn.endswith(".jsonl"):
                            continue
                        fp = os.path.join(root, fn)
                        with open(fp, "r", encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    doc = json.loads(line)
                                except Exception:
                                    continue
                                # montar linha do CSV
                                timestamp = (doc.get("timestamp") or "")
                                # date no CSV = data UTC do commit (YYYY-MM-DD)
                                date_utc = ""
                                if timestamp:
                                    try:
                                        dt = datetime.fromisoformat(timestamp.replace("Z","+00:00"))
                                        date_utc = dt.strftime("%Y-%m-%d")
                                    except Exception:
                                        pass
                                user_login = doc.get("author_login") or ""
                                user_email = doc.get("author_email") or ""
                                # inferir user_key do registro
                                user_key_from_record = f"github:{user_login}" if user_login else (f"email:{user_email.lower()}" if user_email else "")
                                rows.append({
                                    "date": date_utc,
                                    "user_key": user_key_from_record,
                                    "repo": doc.get("repo") or "",
                                    "branch": doc.get("branch") or "",
                                    "sha": doc.get("sha") or "",
                                    "message": (doc.get("message") or "").replace("\n", " ").strip(),
                                    "is_merge": "true" if doc.get("is_merge") else "false",
                                    "verified": "true" if (doc.get("verified") is True) else "false",
                                    "url": doc.get("url") or "",
                                })
    finally:
        # manter tmp para debug? por padrão, limpamos:
        shutil.rmtree(tmpdir, ignore_errors=True)

    # escrever CSV
    cols = ["date","user_key","repo","branch","sha","message","is_merge","verified","url"]
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        # opcional: ordenar por usuario, date, repo
        rows.sort(key=lambda r: (r["user_key"], r["date"], r["repo"]))
        w.writerows(rows)

    print(f"[OK] CSV gerado: {OUT_CSV} ({len(rows)} linhas)")

if __name__ == "__main__":
    main()
