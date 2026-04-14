import os
import glob
import pandas as pd

DATA_DIR = "monthly_repo"

# Find monthly data
files = glob.glob(os.path.join(DATA_DIR, "*_monthly.csv"))

print("Total files found:", len(files))

all_dfs = []

for f in files:
    try:
        df = pd.read_csv(f)

        # Take repo name from file
        filename = os.path.basename(f)
        repo_name = filename.replace("_monthly.csv", "")

        df["repo"] = repo_name

        all_dfs.append(df)

    except Exception as e:
        print("Error loading:", f, e)

# Concat into 1 panel dataset
panel_df = pd.concat(all_dfs, ignore_index=True)

print("Total rows:", len(panel_df))
print("Total unique repos:", panel_df["repo"].nunique())

# Quick preview
print(panel_df.head())

# Save
panel_df.to_csv("panel_dataset.csv", index=False)

print("Saved panel_dataset.csv")