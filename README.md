# Assessing and Predicting the Health of GitHub Repositories

This project investigates how GitHub repository health can be defined in a systematic and interpretable way, and whether future repository activity status can be predicted from historical repository behavior.

The study uses longitudinal GitHub repository data collected through the GitHub REST API and builds a multi-metric framework based on development activity, issue handling, pull request handling, and contributor participation.

## Project Overview

Open-source repositories often show very different levels of activity, maintenance, and sustainability over time. Relying on a single metric such as stars or commits can be misleading, since repository condition is multi-dimensional.

This project addresses that problem by:

- defining repository health using multiple activity- and community-related indicators
- learning an interpretable weighting scheme for health-score construction
- predicting future repository activity status using historical repository-month data
- evaluating the relative importance of different repository metrics

## Research Questions

This project is guided by the following research questions:

1. How can repository health be systematically defined using multiple activity- and community-related metrics?
2. Can future repository activity status be reliably predicted using historical repository data?
3. What is the relative importance of different repository metrics in determining repository health?

## Dataset

The dataset was collected from public GitHub repositories using the GitHub REST API.

### Repository selection

Repositories were selected using filtering rules such as:

- public repositories only
- non-fork repositories
- non-archived repositories
- minimum visibility/activity requirements
- exclusion of tutorial-like, homework, and non-engineered repositories

### Time coverage

- Repository creation cohorts: 2020–2025
- Monthly extraction period: 2019–2025

### Final analytical dataset

- 584 repositories
- 21,303 repository-month observations

## Features

The project uses monthly repository-level features, including:

- `number_of_commits`
- `number_of_open_issues`
- `number_of_closed_issues`
- `number_of_open_PRs`
- `number_of_closed_PRs`
- `number_of_merged_PRs`
- `number_of_contributors`
- `number_of_new_contributors`
- `days_since_last_commit`
- `stars`
- `forks`
- `repo_age_months`

## Health Framework

Repository health is modeled as a weighted combination of four interpretable component scores:

- **Commit Activity**
- **Issue Closure**
- **Pull Request Closure**
- **Contributor Participation**

Rather than assigning weights manually, the project uses a data-driven approach to estimate the relative contribution of each component.

## Methodology

The project includes two main modeling stages:

### 1. Health-score formulation
A regression-based framework is used to evaluate how strongly the four component scores relate to future repository activity. This is used to derive an interpretable health-score formulation.

### 2. Future activity-status prediction
A supervised classification framework is used to predict whether a repository-month will remain active in the near future.

### Evaluation design

To preserve chronological validity, all experiments use time-aware train-test splits rather than random sampling.

Evaluation includes:

- RMSE
- MAE
- R²
- Balanced Accuracy
- Precision
- Recall
- F1-score
- ROC-AUC

## Repository Structure

```bash
.
├── data/
│   ├── raw/
│   ├── monthly_repo/
│   └── processed/
├── notebooks/
├── scripts/
├── outputs/
│   ├── figures/
│   ├── tables/
│   └── models/
├── paper/
├── README.md
└── requirements.txt
