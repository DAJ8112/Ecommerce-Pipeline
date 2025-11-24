# E-Commerce Data Pipeline with PySpark on GCP

End-to-end Big Data pipeline for processing e-commerce data using PySpark, Dataproc Serverless, and BigQuery on Google Cloud Platform.

## Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Step-by-Step Setup](#step-by-step-setup)
- [Running the Pipeline](#running-the-pipeline)
- [Automation Setup](#automation-setup)
- [Visualization](#visualization)
- [Troubleshooting](#troubleshooting)
- [Cost Estimates](#cost-estimates)
- [Cleanup](#cleanup)

## Project Overview

This pipeline processes the theLook eCommerce dataset from BigQuery through four stages:
1. **Extract** - BigQuery → Cloud Storage (Parquet)
2. **Clean & Validate** - Data quality checks with quarantine pattern
3. **Transform** - Business metrics calculation (fulfillment, geographic sales, revenue)
4. **Load** - Cloud Storage → BigQuery curated tables

**Output:** 9 analytical tables + Looker Studio dashboard

## Architecture

![Architecture](ecommerce_pipeline_architecture.svg)

**Data Flow:**
```
BigQuery (Source) → Extract → GCS (Raw) → Clean → GCS (Cleaned) → Transform → GCS (Curated) → Load → BigQuery (Curated) → Looker Studio
```

## Prerequisites

- Google Cloud Platform account
- Billing enabled with ~$20-30 budget
- gcloud CLI installed (or use Cloud Shell)
- Basic knowledge of Python and SQL

## Step-by-Step Setup

### 1. Create GCP Project

```bash
# Go to console.cloud.google.com
# Click "New Project"
# Name: ecommerce-pipeline (or your choice)
# Note your PROJECT_ID (e.g., fa25-1535-joshidh-ecompipeline)
```

Set your project as default:
```bash
gcloud config set project YOUR_PROJECT_ID
```

### 2. Enable Required APIs

```bash
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable dataproc.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
```

### 3. Create Cloud Storage Bucket

Replace `YOUR_BUCKET_NAME` with a globally unique name:

```bash
# Create bucket
gsutil mb -l us-central1 gs://YOUR_BUCKET_NAME

# Create data lake folder structure
gsutil cp /dev/null gs://YOUR_BUCKET_NAME/raw/.keep
gsutil cp /dev/null gs://YOUR_BUCKET_NAME/cleaned/.keep
gsutil cp /dev/null gs://YOUR_BUCKET_NAME/curated/.keep
gsutil cp /dev/null gs://YOUR_BUCKET_NAME/quarantine/.keep
gsutil cp /dev/null gs://YOUR_BUCKET_NAME/metadata/.keep
gsutil cp /dev/null gs://YOUR_BUCKET_NAME/code/.keep
```

Verify folder structure:
```bash
gsutil ls gs://YOUR_BUCKET_NAME/
```

### 4. Create BigQuery Datasets

```bash
bq mk --location=us-central1 --dataset YOUR_PROJECT_ID:ecommerce_curated
bq mk --location=us-central1 --dataset YOUR_PROJECT_ID:ecommerce_metadata
```

Verify:
```bash
bq ls
```

### 5. Configure Networking

Create default VPC network:
```bash
gcloud compute networks create default --subnet-mode=auto
```

Add firewall rule for Dataproc:
```bash
gcloud compute firewall-rules create allow-internal-dataproc \
    --network=default \
    --allow=tcp,udp,icmp \
    --source-ranges=10.128.0.0/9
```

Wait 1-2 minutes for network to be ready.

### 6. Update Job Configuration

Each Python job file (`01_extract.py`, `02_clean.py`, `03_transform.py`, `04_load.py`) has configuration at the top.

**Update these values in ALL job files:**

```python
# Replace these in each job file
PROJECT_ID = "YOUR_PROJECT_ID"  # e.g., fa25-1535-joshidh-ecompipeline
BUCKET_NAME = "YOUR_BUCKET_NAME"  # e.g., ecommerce-pipeline-joshidh
```

Files to update:
- `jobs/01_extract.py` - Lines 10-11
- `jobs/02_clean.py` - Lines 13-14
- `jobs/03_transform.py` - Line 16
- `jobs/04_load.py` - Lines 11-12

### 7. Upload Code to Cloud Storage

```bash
gsutil cp jobs/01_extract.py gs://YOUR_BUCKET_NAME/code/
gsutil cp jobs/02_clean.py gs://YOUR_BUCKET_NAME/code/
gsutil cp jobs/03_transform.py gs://YOUR_BUCKET_NAME/code/
gsutil cp jobs/04_load.py gs://YOUR_BUCKET_NAME/code/
```

Verify upload:
```bash
gsutil ls gs://YOUR_BUCKET_NAME/code/
```

## Running the Pipeline

### Option A: Run Jobs Individually

**Job 1: Extract**
```bash
gcloud dataproc batches submit pyspark \
    gs://YOUR_BUCKET_NAME/code/01_extract.py \
    --batch=extract-job-001 \
    --region=us-central1 \
    --project=YOUR_PROJECT_ID \
    --properties=spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4
```

Wait for completion (~4-5 minutes). You should see:
```
EXTRACTION COMPLETE
orders: 125161 rows
order_items: 181667 rows
users: 100000 rows
products: 29120 rows
distribution_centers: 10 rows
```

Verify data in Cloud Storage:
```bash
gsutil ls gs://YOUR_BUCKET_NAME/raw/
```

---

**Job 2: Clean & Validate**
```bash
gcloud dataproc batches submit pyspark \
    gs://YOUR_BUCKET_NAME/code/02_clean.py \
    --batch=clean-job-001 \
    --region=us-central1 \
    --project=YOUR_PROJECT_ID \
    --properties=spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4
```

Wait for completion (~3-4 minutes). You should see:
```
DATA CLEANING & VALIDATION COMPLETE
Overall pass rate: 100.0%
```

Verify cleaned data:
```bash
gsutil ls gs://YOUR_BUCKET_NAME/cleaned/
```

View quality report:
```bash
gsutil cat gs://YOUR_BUCKET_NAME/metadata/quality_report.json/part-00000
```

---

**Job 3: Transform**
```bash
gcloud dataproc batches submit pyspark \
    gs://YOUR_BUCKET_NAME/code/03_transform.py \
    --batch=transform-job-001 \
    --region=us-central1 \
    --project=YOUR_PROJECT_ID \
    --properties=spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4
```

Wait for completion (~4-6 minutes). You should see:
```
BUSINESS TRANSFORMATIONS COMPLETE
Curated datasets saved to: gs://YOUR_BUCKET_NAME/curated/
```

Verify curated data:
```bash
gsutil ls gs://YOUR_BUCKET_NAME/curated/
```

---

**Job 4: Load to BigQuery**
```bash
gcloud dataproc batches submit pyspark \
    gs://YOUR_BUCKET_NAME/code/04_load.py \
    --batch=load-job-001 \
    --region=us-central1 \
    --project=YOUR_PROJECT_ID \
    --properties=spark.executor.instances=2,spark.driver.cores=4,spark.executor.cores=4
```

Wait for completion (~3-5 minutes). You should see:
```
LOAD TO BIGQUERY COMPLETE
Tables loaded to YOUR_PROJECT_ID.ecommerce_curated:
  - fulfillment_by_dc: XX rows
  - sales_by_country: XX rows
  ...
```

Verify BigQuery tables:
```bash
bq ls YOUR_PROJECT_ID:ecommerce_curated
```

---

### Option B: Orchestrated Pipeline (All Jobs at Once)

**Step 1: Create Workflow Template**
```bash
gcloud dataproc workflow-templates create ecommerce-pipeline \
    --region=us-central1
```

**Step 2: Add Extract Job**
```bash
gcloud dataproc workflow-templates add-job pyspark \
    --workflow-template=ecommerce-pipeline \
    --region=us-central1 \
    --step-id=extract \
    gs://YOUR_BUCKET_NAME/code/01_extract.py
```

**Step 3: Add Clean Job (depends on extract)**
```bash
gcloud dataproc workflow-templates add-job pyspark \
    --workflow-template=ecommerce-pipeline \
    --region=us-central1 \
    --step-id=clean \
    --start-after=extract \
    gs://YOUR_BUCKET_NAME/code/02_clean.py
```

**Step 4: Add Transform Job (depends on clean)**
```bash
gcloud dataproc workflow-templates add-job pyspark \
    --workflow-template=ecommerce-pipeline \
    --region=us-central1 \
    --step-id=transform \
    --start-after=clean \
    gs://YOUR_BUCKET_NAME/code/03_transform.py
```

**Step 5: Add Load Job (depends on transform)**
```bash
gcloud dataproc workflow-templates add-job pyspark \
    --workflow-template=ecommerce-pipeline \
    --region=us-central1 \
    --step-id=load \
    --start-after=transform \
    gs://YOUR_BUCKET_NAME/code/04_load.py
```

**Step 6: Configure Managed Cluster**
```bash
gcloud dataproc workflow-templates set-managed-cluster ecommerce-pipeline \
    --region=us-central1 \
    --cluster-name=ecommerce-cluster \
    --single-node \
    --master-machine-type=n1-standard-4 \
    --image-version=2.1-debian11
```

**Step 7: Verify Template**
```bash
gcloud dataproc workflow-templates describe ecommerce-pipeline --region=us-central1
```

You should see all 4 jobs with their dependencies.

**Step 8: Run Complete Pipeline**
```bash
gcloud dataproc workflow-templates instantiate ecommerce-pipeline \
    --region=us-central1
```

This will take **15-25 minutes** total. Watch for:
```
Creating cluster...
Job ID extract-... RUNNING
Job ID extract-... COMPLETED
Job ID clean-... RUNNING
Job ID clean-... COMPLETED
Job ID transform-... RUNNING
Job ID transform-... COMPLETED
Job ID load-... RUNNING
Job ID load-... COMPLETED
Deleting cluster...
WorkflowTemplate [ecommerce-pipeline] DONE
```

## Automation Setup

Set up weekly automatic execution:

**Step 1: Enable Cloud Scheduler**
```bash
gcloud services enable cloudscheduler.googleapis.com
```

**Step 2: Create Service Account**
```bash
gcloud iam service-accounts create pipeline-scheduler \
    --display-name="Pipeline Scheduler"
```

**Step 3: Grant Permissions**
```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:pipeline-scheduler@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/dataproc.editor"
```

**Step 4: Create Scheduled Job**
```bash
gcloud scheduler jobs create http pipeline-weekly-trigger \
    --location=us-central1 \
    --schedule="0 2 * * 0" \
    --uri="https://dataproc.googleapis.com/v1/projects/YOUR_PROJECT_ID/regions/us-central1/workflowTemplates/ecommerce-pipeline:instantiate" \
    --http-method=POST \
    --oauth-service-account-email=pipeline-scheduler@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

**Schedule:** Every Sunday at 2:00 AM UTC

**Step 5: Test Manual Trigger**
```bash
gcloud scheduler jobs run pipeline-weekly-trigger --location=us-central1
```

**View Scheduled Jobs**
```bash
gcloud scheduler jobs list --location=us-central1
```

**Pause Schedule (to avoid recurring costs)**
```bash
gcloud scheduler jobs pause pipeline-weekly-trigger --location=us-central1
```

## Visualization

Create Looker Studio dashboard to visualize results.

**Step 1: Access Looker Studio**
- Go to https://lookerstudio.google.com
- Sign in with your Google account

**Step 2: Create New Report**
- Click **Create** → **Report**

**Step 3: Connect to BigQuery**
- Select **BigQuery** connector
- Authorize if prompted
- Navigate to: **My Projects** → **YOUR_PROJECT_ID** → **ecommerce_curated**
- Select **sales_by_country**
- Click **Add** → **Add to report**

**Step 4: Add Charts**

*Chart 1: Top Countries by Revenue*
- Click **Add a chart** → **Bar chart**
- Dimension: `country`
- Metric: `total_revenue`
- Sort: Descending by `total_revenue`
- Title: "Top 10 Countries by Revenue"

*Chart 2: Fulfillment Performance*
- Click **Add data** → Select **fulfillment_by_dc**
- Click **Add a chart** → **Table**
- Dimensions: `distribution_center`
- Metrics: `total_orders`, `avg_days_to_ship`, `avg_total_fulfillment_days`
- Sort: Ascending by `avg_total_fulfillment_days`

*Chart 3: Revenue by Category*
- Click **Add data** → Select **revenue_by_category**
- Click **Add a chart** → **Pie chart**
- Dimension: `category`
- Metric: `total_revenue`

*Chart 4: Monthly Revenue Trend*
- Click **Add data** → Select **revenue_monthly_trend**
- Click **Add a chart** → **Table** or **Time series**
- Dimensions: `year`, `month`
- Metric: `total_revenue`
- Sort: Ascending by `year`, then `month`

**Step 5: Add Dashboard Title**
- Click text icon → Add title "E-Commerce Analytics Dashboard"

## Troubleshooting

### Network Errors

**Error:** `The resource 'projects/.../networks/default' was not found`

**Fix:**
```bash
gcloud compute networks create default --subnet-mode=auto
```

Wait 1-2 minutes, then retry.

---

**Error:** `Timed out waiting for workers to register`

**Fix:**
```bash
gcloud compute firewall-rules create allow-internal-dataproc \
    --network=default \
    --allow=tcp,udp,icmp \
    --source-ranges=10.128.0.0/9
```

Wait 1-2 minutes, then retry.

---

### Job Errors

**Error:** `AttributeError: 'DataFrame' object has no attribute 'COLUMN_NAME'`

**Fix:** Check actual schema:
```bash
bq show --schema --format=prettyjson bigquery-public-data:thelook_ecommerce.TABLE_NAME
```

Verify column names match what your code expects.

---

**Error:** `ServiceConfigurationError: BigQueryRelationProvider not a subtype`

**Fix:** Don't use `--jars` flag. Instead, configure BigQuery connector in Spark session:
```python
spark = SparkSession.builder \
    .appName("Job_Name") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2") \
    .getOrCreate()
```

---

### View Job Logs

If a job fails, check detailed logs:
```bash
# List batches
gcloud dataproc batches list --region=us-central1

# Describe specific batch (get the batch ID from list command)
gcloud dataproc batches describe BATCH_ID --region=us-central1
```

Or view in GCP Console:
- Go to **Dataproc** → **Batches**
- Click on failed job
- View logs and error messages

## Cost Estimates

| Service | Usage | Estimated Cost |
|---------|-------|----------------|
| Dataproc Serverless | 15-20 job runs during development | $10-15 |
| Cloud Storage | ~2GB stored for 1 week | $0.50 |
| BigQuery | Queries + storage | $2-5 |
| Data Transfer | Minimal egress | $1-2 |
| Cloud Scheduler | 1 scheduled job | $0.10/month |
| **Total (one-time)** | | **$13-23** |
| **Total (monthly if scheduled)** | | **$12-20** |

**Cost Optimization Tips:**
- Delete resources after project completion
- Use single-node clusters for development
- Set budget alerts at $15 and $25
- Pause Cloud Scheduler when not needed
- Monitor usage in Billing dashboard

**Set Budget Alert:**
```bash
# Go to console.cloud.google.com
# Navigate to Billing → Budgets & alerts
# Create budget: $30
# Set alerts at 50% ($15) and 80% ($24)
```

## Cleanup

**Important:** Delete all resources to avoid ongoing charges.

### Delete Scheduler (if created)
```bash
gcloud scheduler jobs delete pipeline-weekly-trigger --location=us-central1
```

### Delete Workflow Template
```bash
gcloud dataproc workflow-templates delete ecommerce-pipeline --region=us-central1
```

### Delete BigQuery Datasets
```bash
bq rm -r -f -d YOUR_PROJECT_ID:ecommerce_curated
bq rm -r -f -d YOUR_PROJECT_ID:ecommerce_metadata
```

### Delete Cloud Storage Bucket
```bash
gsutil rm -r gs://YOUR_BUCKET_NAME
```

### Delete Service Account
```bash
gcloud iam service-accounts delete pipeline-scheduler@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### Delete Firewall Rule
```bash
gcloud compute firewall-rules delete allow-internal-dataproc
```

### Delete Entire Project (Optional)
```bash
gcloud projects delete YOUR_PROJECT_ID
```

## Querying Results

Example queries to explore your data:

**Top 10 Countries by Revenue:**
```sql
SELECT country, total_orders, total_revenue, avg_order_value
FROM `YOUR_PROJECT_ID.ecommerce_curated.sales_by_country`
ORDER BY total_revenue DESC
LIMIT 10
```

**Fulfillment Performance:**
```sql
SELECT distribution_center, total_orders, avg_days_to_ship, avg_total_fulfillment_days
FROM `YOUR_PROJECT_ID.ecommerce_curated.fulfillment_by_dc`
ORDER BY avg_total_fulfillment_days ASC
```

**Revenue by Category:**
```sql
SELECT category, items_sold, total_revenue, total_profit, avg_profit_margin_pct
FROM `YOUR_PROJECT_ID.ecommerce_curated.revenue_by_category`
ORDER BY total_revenue DESC
```

**Monthly Revenue Trend:**
```sql
SELECT year, month, total_revenue, total_profit, items_sold
FROM `YOUR_PROJECT_ID.ecommerce_curated.revenue_monthly_trend`
ORDER BY year, month
```

## Project Structure

```
ecommerce-pipeline/
├── jobs/
│   ├── 01_extract.py          # Extract from BigQuery to GCS
│   ├── 02_clean.py            # Data validation and cleaning
│   ├── 03_transform.py        # Business metric calculations
│   └── 04_load.py             # Load to BigQuery
├── ecommerce_pipeline_architecture.svg
└── README.md
```

## Data Schema

### Source Tables (BigQuery Public Data)
- `bigquery-public-data.thelook_ecommerce.orders` - 125,161 records
- `bigquery-public-data.thelook_ecommerce.order_items` - 181,667 records
- `bigquery-public-data.thelook_ecommerce.users` - 100,000 records
- `bigquery-public-data.thelook_ecommerce.products` - 29,120 records
- `bigquery-public-data.thelook_ecommerce.distribution_centers` - 10 records

### Output Tables (YOUR_PROJECT_ID.ecommerce_curated)
- `fulfillment_by_dc` - Avg fulfillment time by distribution center
- `fulfillment_by_status` - Order counts by status
- `sales_by_country` - Revenue metrics by country
- `sales_by_state` - Revenue metrics by state
- `sales_by_traffic_source` - Performance by traffic source
- `revenue_by_category` - Revenue/profit by product category
- `revenue_by_brand` - Top 50 brands by revenue
- `revenue_monthly_trend` - Monthly revenue and profit trends
- `revenue_by_department` - Performance by department

## Key Technologies

- **PySpark** - Distributed data processing
- **Dataproc Serverless** - Managed Spark without cluster management
- **BigQuery** - Cloud data warehouse
- **Cloud Storage** - Data lake (Parquet files)
- **Dataproc Workflow Templates** - Job orchestration
- **Cloud Scheduler** - Automated scheduling
- **Looker Studio** - Data visualization

## Support

For issues or questions:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review GCP documentation links
3. Check Dataproc job logs in GCP Console

## License

Educational project for Big Data Management coursework.
