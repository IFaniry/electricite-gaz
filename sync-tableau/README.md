# Sync Tableau: Export Energy Data Warehouse to Google Sheets (Polars)

A high-performance pipeline written in **Polars** that extracts, enriches, and flattens the energy constellation data warehouse from `brainstorm-data/` into a Google Spreadsheet. Tableau connects directly to this Google Sheet to provide interactive dashboards and automated scheduled refreshes.

---

## Architecture Overview

```
                                      +------------------------------------+
                                      |     Google Spreadsheet (Tabs)      |
                                      |  1. Daily_Energy_Analytics (Flat)  |
                                      |  2. Annual_Conso_Secteur           |
+---------------------+               |  3. Segmented_Linky_Inf36          |
|  brainstorm-data/   |  Polars Lazy  +-----------------+------------------+
|  (CSV & ICS files)  | ----------->                    |
+---------------------+   Pipeline                      | Native Connector
                                                        v
                                              +-------------------+
                                              |  Tableau Cloud /  |
                                              |  Tableau Desktop  |
                                              +-------------------+
```

### Exported Sheets & Cell Budget (From 2020)

| Sheet Name | Grain | Rows | Cols | Total Cells | Purpose in Tableau |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`Daily_Energy_Analytics`** | `(date_jour, code_geo)` | ~29,950 | 27 | ~808,700 | **Master Flat Table**: Daily Elec (MWh, MW avg, peak), Daily Gas (MWh), Calendar flags, Holidays, Regional vacation matching, Climate DJU, and Housing stock influencers. |
| **`Annual_Conso_Secteur`** | `(annee, code_geo, filiere, secteur)` | ~1,680 | 19 | ~32,000 | Annual sectoral consumption (Residential, Tertiary, Industry, Agriculture) aggregated across operators. |
| **`Segmented_Linky_Inf36`** | `(periode, code_geo, profil, kVA)` | ~34,080 | 12 | ~409,000 | Linky residential / pro meters and consumption segmented by subscribed power (kVA). |
| **Total Combined** | — | **~65,700** | — | **~1,250,000** | **~12.5% of Google Sheets quota** (10M cells limit). |

---

## 1. Running Manually from Your Local Machine

### Prerequisites

1. Python 3.10+ (Polars $\ge$ 2.0).
2. Google Cloud Service Account JSON key (or OAuth) with permissions on Google Sheets.
3. An existing Google Spreadsheet shared with your Service Account email as **Editor**.

### Step 1: Install Dependencies

In your project virtual environment:

```bash
# From the repository root:
pip install -r sync-tableau/requirements.txt
```

### Step 2: Configure Environment Variables

Copy the template:

```bash
cp sync-tableau/.env.example sync-tableau/.env
```

Edit `sync-tableau/.env`:

```ini
# Found in the URL: https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/edit
SPREADSHEET_ID=1A2B3C4D5E6F7G8H9I0JkLmNoPqRsTuVwXyZ

# Path to your Google Cloud Service Account JSON file
GOOGLE_APPLICATION_CREDENTIALS=sync-tableau/credentials.json

# Optional defaults
START_YEAR=2020
DATA_DIR=brainstorm-data
```

### Step 3: Test with Dry-Run (No Google API writes)

Before modifying your Google Sheet, run a dry-run to verify data processing:

```bash
python sync-tableau/main.py --dry-run
```

*Output displays execution speed (typically < 0.3s), shapes, memory footprints, and sample rows.*

### Step 4: Run the Sync

To upload all three sheets to your Google Spreadsheet:

```bash
python sync-tableau/main.py
```

#### Selective CLI Options:

```bash
# Export only the Master Flat Table (fastest for single-table Tableau dashboards):
python sync-tableau/main.py --mode daily

# Export to both Google Sheets AND local CSV files for offline testing:
python sync-tableau/main.py --export-csv ./tableau-exports

# Explicit spreadsheet ID and custom chunk size:
python sync-tableau/main.py --spreadsheet-id <YOUR_SHEET_ID> --chunk-size 10000
```

---

## 2. Google Cloud Setup (Service Account & Permissions)

To allow the Python script (locally or in Cloud Run) to write to your Google Sheet:

1. **Enable APIs** in the Google Cloud Console:
   - Go to **APIs & Services > Library**.
   - Enable **Google Sheets API** and **Google Drive API**.
2. **Create a Service Account**:
   - Go to **IAM & Admin > Service Accounts > Create Service Account**.
   - Name: `sync-tableau-sa`.
   - Grant no specific project roles (Sheets permissions are granted at the spreadsheet level).
3. **Generate a JSON Key (for local execution)**:
   - Click the newly created Service Account > **Keys > Add Key > Create new key > JSON**.
   - Download the file as `sync-tableau/credentials.json` (do not commit to Git).
4. **Share the Google Sheet**:
   - Open your Google Spreadsheet in your browser.
   - Click **Share** (top right).
   - Paste the Service Account email (e.g. `sync-tableau-sa@<project-id>.iam.gserviceaccount.com`).
   - Assign the **Editor** role and uncheck "Notify people".

---

## 3. Hosted Version: Google Cloud Run + Cloud Scheduler

For scheduled execution, **Cloud Run Jobs** is the recommended serverless architecture:
- Unlike Cloud Run Services (which listen on HTTP ports), **Cloud Run Jobs** run a task to completion and terminate immediately.
- Zero idle cost (you only pay for the ~30 seconds of compute during the sync).

### Step 1: Set GCP Variables

```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="europe-west1"
export REPO_NAME="energy-dw"
export IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/sync-tableau:latest"
export SPREADSHEET_ID="your_google_spreadsheet_id"
```

### Step 2: Create Artifact Registry Repository & Build Image

```bash
# Create Docker repository (if not already existing)
gcloud artifacts repositories create ${REPO_NAME} \
    --repository-format=docker \
    --location=${REGION} \
    --project=${PROJECT_ID}

# Build and push the image via Cloud Build (from repository root)
gcloud builds submit . \
    --config=sync-tableau/cloudbuild.yaml \
    --project=${PROJECT_ID}
```

*(Or build directly using Docker):*
```bash
docker build -f sync-tableau/Dockerfile -t ${IMAGE_NAME} .
docker push ${IMAGE_NAME}
```

### Step 3: Create the Cloud Run Job

```bash
gcloud run jobs create sync-tableau-job \
    --image=${IMAGE_NAME} \
    --region=${REGION} \
    --project=${PROJECT_ID} \
    --set-env-vars=SPREADSHEET_ID=${SPREADSHEET_ID},START_YEAR=2020 \
    --service-account=sync-tableau-sa@${PROJECT_ID}.iam.gserviceaccount.com \
    --memory=1Gi \
    --cpu=1 \
    --max-retries=2 \
    --task-timeout=10m
```

> **Note on Service Account in Cloud Run**: When running on Cloud Run, Google Application Default Credentials (ADC) automatically authenticate using the attached service account. No JSON key file needs to be mounted!

### Step 4: Test Cloud Run Job Execution

Trigger an immediate execution manually:

```bash
gcloud run jobs execute sync-tableau-job --region=${REGION} --project=${PROJECT_ID}
```

Inspect live logs:
```bash
gcloud beta run jobs executions logs read --region=${REGION}
```

### Step 5: Automate with Cloud Scheduler (Cron)

Set up a recurring trigger to refresh your Google Sheet daily at 06:00 UTC:

```bash
# Create service account for Scheduler invocation
gcloud iam service-accounts create scheduler-run-invoker \
    --display-name="Cloud Scheduler Cloud Run Invoker"

# Grant permission to invoke Cloud Run Jobs
gcloud run jobs add-iam-policy-binding sync-tableau-job \
    --region=${REGION} \
    --member="serviceAccount:scheduler-run-invoker@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/run.invoker"

# Create Cloud Scheduler Job (Daily at 06:00 UTC)
gcloud scheduler jobs create http sync-tableau-daily \
    --location=${REGION} \
    --schedule="0 6 * * *" \
    --time-zone="Europe/Paris" \
    --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/sync-tableau-job:run" \
    --http-method=POST \
    --oauth-service-account-email="scheduler-run-invoker@${PROJECT_ID}.iam.gserviceaccount.com"
```

---

## 4. Connecting Tableau to Google Sheets

### Connecting in Tableau Desktop / Cloud

1. In Tableau, go to **Connect > To a Server > More... > Google Sheets**.
2. Sign in with your Google account.
3. Select your synchronized spreadsheet.
4. **Data Modeling Options**:
   - **Option A (Recommended for 95% of use-cases)**: Drag only `Daily_Energy_Analytics` onto the canvas. It is pre-joined with climate, holidays, and regional context.
   - **Option B (Multi-Table Model)**: Drag `Daily_Energy_Analytics`, `Annual_Conso_Secteur`, and `Segmented_Linky_Inf36` onto Tableau's logical canvas and relate them on `code_geo` or `annee`.

### Setting Up Scheduled Refreshes in Tableau Cloud / Server

1. Publish your data source or workbook to **Tableau Cloud / Server**.
2. When prompted for credentials, choose **Embedded password** for Google Sheets.
3. Under **Refresh Schedules**, schedule an automated extract refresh (e.g. daily at 07:00 UTC, one hour after Cloud Scheduler runs).
4. Tableau will automatically query Google Sheets and update all dashboards without any manual intervention!
