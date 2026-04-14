import os
import time
import csv
import math
import random
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from requests.exceptions import ConnectionError, Timeout, HTTPError

# =========================
# CONFIG
# =========================
START_YEAR = 2019
END_YEAR   = 2025  # inclusive
OUT_DIR    = "monthly_repo"
INPUT_CSV  = "repo_list.csv"

TOKEN = os.getenv("GITHUB_TOKEN")
if not TOKEN:
    raise RuntimeError("Missing GITHUB_TOKEN environment variable")

BASE = "https://api.github.com"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

# Safer throttle for big crawls
SLEEP_BETWEEN_REQUESTS = 0.8  # seconds

# Caps to avoid exploding API usage
MAX_COMMITS_FOR_CONTRIB = 1000  # per repo-month (contributors derived from commits list)

# =========================
# HELPERS
# =========================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def safe_filename(full_name: str) -> str:
    # owner/repo -> owner__repo
    return full_name.replace("/", "__")

def read_repo_list(path: str):
    """Read repo full_name from CSV column: full_name"""
    repos = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            full = (row.get("full_name") or "").strip()
            if full and "/" in full:
                repos.append(full)
    return repos

def month_range(start_year: int, end_year: int):
    """Yield (year, month, start_date_str, end_date_str, month_end_dt_utc) for each month."""
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            start_dt = datetime(y, m, 1, tzinfo=timezone.utc)
            if m == 12:
                next_dt = datetime(y + 1, 1, 1, tzinfo=timezone.utc)
            else:
                next_dt = datetime(y, m + 1, 1, tzinfo=timezone.utc)
            end_dt = next_dt - timedelta(days=1)
            yield (
                y,
                m,
                start_dt.strftime("%Y-%m-%d"),
                end_dt.strftime("%Y-%m-%d"),
                end_dt,
            )

def month_end_iso(end_dt: datetime) -> str:
    """Month-end as ISO8601 Z, end of day UTC."""
    dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def iso_start(date_str: str) -> str:
    return f"{date_str}T00:00:00Z"

def iso_end(date_str: str) -> str:
    return f"{date_str}T23:59:59Z"


# =========================
# ROBUST HTTP GET (retry/backoff)
# =========================
def gh_get(url, params=None, headers=None, max_retries=7):
    """
    Robust GET:
    - retries on network drops (ConnectionError/Timeout)
    - handles primary rate limit via X-RateLimit-Reset
    - handles secondary rate limit / abuse with longer backoff
    """
    hdrs = headers if headers is not None else HEADERS

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=hdrs, params=params, timeout=30)

            # Primary rate limit
            if r.status_code == 403 and "rate limit" in r.text.lower():
                reset = int(r.headers.get("X-RateLimit-Reset", time.time() + 60))
                sleep_s = max(10, reset - int(time.time()))
                print(f"[Rate limit] sleeping {sleep_s}s")
                time.sleep(sleep_s)
                continue

            # Secondary rate limit / abuse detection
            if r.status_code == 403 and ("secondary rate limit" in r.text.lower() or "abuse" in r.text.lower()):
                sleep_s = 30 * attempt + random.randint(0, 10)
                print(f"[Secondary limit] sleeping {sleep_s}s")
                time.sleep(sleep_s)
                continue

            # Temporary server errors
            if r.status_code in (500, 502, 503, 504):
                sleep_s = 5 * attempt + random.randint(0, 5)
                print(f"[Server {r.status_code}] retry {attempt}/{max_retries}, sleeping {sleep_s}s")
                time.sleep(sleep_s)
                continue

            r.raise_for_status()
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return r

        except (ConnectionError, Timeout) as e:
            if attempt == max_retries:
                raise
            sleep_s = (2 ** attempt) + random.randint(0, 3)
            print(f"[Connection] {type(e).__name__} retry {attempt}/{max_retries}, sleeping {sleep_s}s")
            time.sleep(sleep_s)

        except HTTPError:
            raise

    raise RuntimeError("Unreachable")


# =========================
# API WRAPPERS
# =========================
def get_repo_meta(full_name: str) -> dict:
    owner, repo = full_name.split("/", 1)
    url = f"{BASE}/repos/{owner}/{repo}"
    return gh_get(url).json()

def search_total_count(query: str) -> int:
    """Use GitHub Search Issues API to get total_count quickly."""
    url = f"{BASE}/search/issues"
    params = {"q": query, "per_page": 1}
    data = gh_get(url, params=params).json()
    return int(data.get("total_count", 0))


# =========================
# MONTHLY METRICS (Search Issues)
# =========================
def number_of_open_issues(repo_full: str, start_date: str, end_date: str) -> int:
    q = f"repo:{repo_full} type:issue created:{start_date}..{end_date}"
    return search_total_count(q)

def number_of_closed_issues(repo_full: str, start_date: str, end_date: str) -> int:
    q = f"repo:{repo_full} type:issue closed:{start_date}..{end_date}"
    return search_total_count(q)

def number_of_open_PRs(repo_full: str, start_date: str, end_date: str) -> int:
    q = f"repo:{repo_full} type:pr created:{start_date}..{end_date}"
    return search_total_count(q)

def number_of_closed_PRs(repo_full: str, start_date: str, end_date: str) -> int:
    q = f"repo:{repo_full} type:pr closed:{start_date}..{end_date}"
    return search_total_count(q)

def number_of_merged_PRs(repo_full: str, start_date: str, end_date: str) -> int:
    q = f"repo:{repo_full} is:pr is:merged merged:{start_date}..{end_date}"
    return search_total_count(q)


# =========================
# COMMITS COUNT PER MONTH (Search Commits)
# =========================
def number_of_commits(repo_full: str, start_date: str, end_date: str) -> int:
    """
    /search/commits sometimes requires special accept headers.
    """
    url = f"{BASE}/search/commits"
    headers = dict(HEADERS)
    headers["Accept"] = "application/vnd.github+json"
    q = f"repo:{repo_full} committer-date:{start_date}..{end_date}"
    params = {"q": q, "per_page": 1}
    data = gh_get(url, params=params, headers=headers).json()
    return int(data.get("total_count", 0))


# =========================
# CONTRIBUTORS (from commits list, capped)
# =========================
def list_commits_in_month(repo_full: str, start_date: str, end_date: str, max_items=1000):
    """
    Fetch commits in date range via /repos/{owner}/{repo}/commits?since&until
    and return commit objects (capped).
    """
    owner, repo = repo_full.split("/", 1)
    url = f"{BASE}/repos/{owner}/{repo}/commits"
    commits = []
    page = 1
    per_page = 100

    since = iso_start(start_date)
    until = iso_end(end_date)

    while len(commits) < max_items:
        params = {"since": since, "until": until, "per_page": per_page, "page": page}
        data = gh_get(url, params=params).json()
        if not data:
            break
        commits.extend(data)
        if len(data) < per_page:
            break
        page += 1
        if page > math.ceil(max_items / per_page) + 2:
            break

    return commits[:max_items]

def compute_contributors(repo_full: str, start_date: str, end_date: str):
    """
    Returns:
      contributors_set: set[str] (login or email fallback)
      truncated: bool
    """
    commits = list_commits_in_month(repo_full, start_date, end_date, max_items=MAX_COMMITS_FOR_CONTRIB)
    truncated = len(commits) >= MAX_COMMITS_FOR_CONTRIB

    contributors = set()

    for c in commits:
        author = c.get("author") or {}
        login = author.get("login")

        commit_obj = c.get("commit") or {}
        author_obj = commit_obj.get("author") or {}
        committer_obj = commit_obj.get("committer") or {}

        email = (author_obj.get("email") or committer_obj.get("email") or "").strip()
        ident = (login or email or "").strip()
        if ident:
            contributors.add(ident)

    return contributors, truncated


# =========================
# LAST COMMIT DATE UNTIL MONTH END
# =========================
def last_commit_date_until(repo_full: str, month_end_dt: datetime):
    owner, repo = repo_full.split("/", 1)
    url = f"{BASE}/repos/{owner}/{repo}/commits"
    params = {"until": month_end_iso(month_end_dt), "per_page": 1}
    data = gh_get(url, params=params).json()
    if not data:
        return None

    commit = data[0].get("commit", {}) or {}
    committer = commit.get("committer", {}) or {}
    dt_str = committer.get("date")
    if not dt_str:
        return None

    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# =========================
# FEATURE ENGINEERING (NO 6m COLUMNS)
# =========================
def add_features(df: pd.DataFrame, created_at_iso: str, stars_snapshot: int, forks_snapshot: int) -> pd.DataFrame:
    """
    Output schema (monthly, no issue_comments):
      date
      repo_age_months
      stars
      forks
      days_since_last_commit

      number_of_contributors
      number_of_commits
      number_of_new_contributors
      number_of_open_PRs
      number_of_closed_PRs
      number_of_merged_PRs
      number_of_open_issues
      number_of_closed_issues

      activity_status: "active"/"inactive"
      health_label: "healthy"/"unhealthy"
    """
    created_dt = datetime.strptime(created_at_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    def months_between_inclusive(a: datetime, b: datetime) -> int:
        return (b.year - a.year) * 12 + (b.month - a.month) + 1

    df["repo_age_months"] = df["month_end_dt"].apply(
        lambda d: 0 if d < created_dt else max(1, months_between_inclusive(created_dt, d))
    )

    # Drop pre-creation months
    df = df[df["repo_age_months"] >= 1].copy()

    # Date label (e.g., Jan-19)
    df["date"] = df.apply(lambda r: datetime(int(r["year"]), int(r["month"]), 1).strftime("%b-%y"), axis=1)

    # days_since_last_commit
    def days_since(row):
        lcd = row["last_commit_dt"]
        if lcd is None or pd.isna(lcd):
            return None
        d = int((row["month_end_dt"] - lcd).days)
        return max(0, d)

    df["days_since_last_commit"] = df.apply(days_since, axis=1)

    # stars/forks snapshot
    df["stars"] = stars_snapshot
    df["forks"] = forks_snapshot

    # activity_status: active if any activity happened this month
    def activity_status(row):
        active = (
            (row["number_of_commits"] > 0) or
            (row["number_of_open_PRs"] > 0) or
            (row["number_of_closed_PRs"] > 0) or
            (row["number_of_merged_PRs"] > 0) or
            (row["number_of_open_issues"] > 0) or
            (row["number_of_closed_issues"] > 0)
        )
        return "active" if active else "inactive"

    df["activity_status"] = df.apply(activity_status, axis=1)

    # health_label: simple rule-based label (you can change later)
    def health_rule(row):
        opened = row["number_of_open_issues"]
        closed = row["number_of_closed_issues"]
        close_rate = 1.0 if opened == 0 else (closed / max(1.0, opened))
        healthy = (row["activity_status"] == "active") and (row["number_of_contributors"] >= 2) and (close_rate >= 0.5)
        return "healthy" if healthy else "unhealthy"

    df["health_label"] = df.apply(health_rule, axis=1)

    out_cols = [
        "date",
        "repo_age_months",
        "stars",
        "forks",
        "days_since_last_commit",

        "number_of_contributors",
        "number_of_commits",
        "number_of_new_contributors",
        "number_of_open_PRs",
        "number_of_closed_PRs",
        "number_of_merged_PRs",
        "number_of_open_issues",
        "number_of_closed_issues",

        "activity_status",
        "health_label",
    ]
    return df[out_cols].reset_index(drop=True)


# =========================
# BUILD ONE REPO CSV
# =========================
def build_monthly_csv_for_repo(repo_full: str):
    meta = get_repo_meta(repo_full)

    stars = int(meta.get("stargazers_count", 0))
    forks = int(meta.get("forks_count", 0))
    created_at = meta.get("created_at")
    if not created_at:
        print(f"SKIP {repo_full} (missing created_at)")
        return

    created_dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    archived = bool(meta.get("archived", False))
    disabled = bool(meta.get("disabled", False))
    is_fork = bool(meta.get("fork", False))
    if archived or disabled or is_fork:
        print(f"SKIP {repo_full} (archived/disabled/fork)")
        return

    rows = []
    seen_contributors = set()

    for (y, m, start_d, end_d, month_end_dt) in month_range(START_YEAR, END_YEAR):

        if month_end_dt < created_dt:
            rows.append({
                "year": y,
                "month": m,
                "month_end_dt": month_end_dt,
                "last_commit_dt": None,

                "number_of_open_issues": 0,
                "number_of_closed_issues": 0,
                "number_of_open_PRs": 0,
                "number_of_closed_PRs": 0,
                "number_of_merged_PRs": 0,

                "number_of_commits": 0,
                "number_of_contributors": 0,
                "number_of_new_contributors": 0,
            })
            continue

        # Issues + PR (fast total_count)
        n_open_issues = number_of_open_issues(repo_full, start_d, end_d)
        n_closed_issues = number_of_closed_issues(repo_full, start_d, end_d)
        n_open_pr = number_of_open_PRs(repo_full, start_d, end_d)
        n_closed_pr = number_of_closed_PRs(repo_full, start_d, end_d)
        n_merged_pr = number_of_merged_PRs(repo_full, start_d, end_d)

        # Commits count (search/commits)
        try:
            n_commits = number_of_commits(repo_full, start_d, end_d)
        except Exception as e:
            print(f"  [WARN] commits search failed for {repo_full} {y}-{m:02d}: {e}")
            n_commits = 0

        # Contributors (from commit list; capped)
        try:
            contribs, truncated = compute_contributors(repo_full, start_d, end_d)
            if truncated:
                print(f"  [WARN] contributors truncated for {repo_full} {y}-{m:02d} (>{MAX_COMMITS_FOR_CONTRIB} commits)")
        except Exception as e:
            print(f"  [WARN] contributors fetch failed for {repo_full} {y}-{m:02d}: {e}")
            contribs = set()

        new_contribs = contribs - seen_contributors
        seen_contributors |= contribs

        # Last commit date until month end
        try:
            lcd = last_commit_date_until(repo_full, month_end_dt)
        except Exception as e:
            print(f"  [WARN] last commit fetch failed for {repo_full} {y}-{m:02d}: {e}")
            lcd = None

        rows.append({
            "year": y,
            "month": m,
            "month_end_dt": month_end_dt,
            "last_commit_dt": lcd,

            "number_of_open_issues": int(n_open_issues),
            "number_of_closed_issues": int(n_closed_issues),
            "number_of_open_PRs": int(n_open_pr),
            "number_of_closed_PRs": int(n_closed_pr),
            "number_of_merged_PRs": int(n_merged_pr),

            "number_of_commits": int(n_commits),
            "number_of_contributors": int(len(contribs)),
            "number_of_new_contributors": int(len(new_contribs)),
        })

    df_raw = pd.DataFrame(rows).sort_values(["year", "month"]).reset_index(drop=True)

    df_feat = add_features(
        df_raw,
        created_at_iso=created_at,
        stars_snapshot=stars,
        forks_snapshot=forks,
    )

    ensure_dir(OUT_DIR)
    out_path = os.path.join(OUT_DIR, f"{safe_filename(repo_full)}_monthly.csv")
    df_feat.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path} rows={len(df_feat)}")


# =========================
# MAIN
# =========================
def main():
    repos = read_repo_list(INPUT_CSV)
    print("Total repos:", len(repos))

    ensure_dir(OUT_DIR)

    for i, repo_full in enumerate(repos, 1):
        print(f"\n[{i}/{len(repos)}] {repo_full}")

        out_path = os.path.join(OUT_DIR, f"{safe_filename(repo_full)}_monthly.csv")
        if os.path.exists(out_path):
            print(f"SKIP (already done): {repo_full}")
            continue

        try:
            build_monthly_csv_for_repo(repo_full)
        except Exception as e:
            print(f"ERROR {repo_full}: {e}")

if __name__ == "__main__":
    main()
