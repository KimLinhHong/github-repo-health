import os, re, time, requests, pandas as pd

TOKEN = os.getenv("GITHUB_TOKEN")
assert TOKEN is not None, "GITHUB_TOKEN has not been set."

headers = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/vnd.github+json"}
BASE_URL = "https://api.github.com/search/repositories"

PER_PAGE = 100
MAX_PAGES = 10
#Stars
MIN_STARS = 50

EXCLUDE_PATTERN = re.compile(
    r"\b(tutorial|example|examples|demo|sample|samples|course|courses|"
    r"class|assignment|homework|lab|lecture|workshop|bootcamp|"
    r"book|ebook|cookbook|notes|slides|lesson|learn|learning)\b",
    re.IGNORECASE,
)
#Forks
MIN_FORKS_PROXY = 1
MIN_OPEN_ISSUES_PROXY = 1

TARGET_TOTAL = 600
YEARS = list(range(2020, 2026))
TARGET_PER_YEAR = TARGET_TOTAL // len(YEARS)  # 100

def github_get(url, params=None, max_retries=5):
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params)
        if r.status_code == 200:
            return r
        if r.status_code == 403 and "rate limit" in r.text.lower():
            reset = r.headers.get("X-RateLimit-Reset")
            if reset:
                sleep_for = max(0, int(reset) - int(time.time())) + 2
                print(f"Rate limit hit. Sleeping {sleep_for}s...")
                time.sleep(sleep_for)
                continue
        backoff = 2 ** attempt
        print(f"HTTP {r.status_code}: {r.text[:200]} | retry in {backoff}s")
        time.sleep(backoff)
    return None

#book, class, Tutorial
def looks_like_tutorial_book_class(name, description):
    text = f"{name or ''} {description or ''}"
    return bool(EXCLUDE_PATTERN.search(text))

rows = []

for year in YEARS:
    start = f"{year}-01-01"
    end   = f"{year}-12-31"

    query = (
        f"created:{start}..{end} "
        f"stars:>={MIN_STARS} "
        f"fork:false "
        f"archived:false "
        f"is:public"
    )

    kept_this_year = 0
    print(f"\nYear {year}")

    for page in range(1, MAX_PAGES + 1):
        if kept_this_year >= TARGET_PER_YEAR:
            break

        params = {
            "q": query,
            "sort": "stars",
            "order": "desc",
            "per_page": PER_PAGE,
            "page": page
        }

        r = github_get(BASE_URL, params=params)
        if r is None:
            break

        items = r.json().get("items", [])
        if not items:
            break

        for repo in items:
            if kept_this_year >= TARGET_PER_YEAR:
                break

            # --- KEEP ALL FILTERS ---
            if looks_like_tutorial_book_class(repo.get("name"), repo.get("description")):
                continue
            if repo.get("has_issues") is not True:
                continue
            if (repo.get("forks_count") or 0) < MIN_FORKS_PROXY:
                continue
            if (repo.get("open_issues_count") or 0) < MIN_OPEN_ISSUES_PROXY:
                continue
            # ------------------------

            rows.append({
                "year": year,
                "full_name": repo.get("full_name"),
                "html_url": repo.get("html_url"),
                "created_at": repo.get("created_at"),
                "stargazers": repo.get("stargazers_count"),
                "forks": repo.get("forks_count"),
                "open_issues": repo.get("open_issues_count"),
                "language": repo.get("language"),
                "description": repo.get("description"),
            })
            kept_this_year += 1

        time.sleep(1)

   # print(f"{year} kept: {kept_this_year}")

df = pd.DataFrame(rows).drop_duplicates("full_name")

df.to_csv("repo_list.csv", index=False)

print("\nSaved repo_list.csv")
print("Rows kept:", len(df))
