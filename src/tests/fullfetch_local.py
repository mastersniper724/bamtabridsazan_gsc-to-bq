from google.cloud import bigquery

client = bigquery.Client.from_service_account_json("gcp-key.json")

dataset_id = "bamtabridsazan.seo_reports"
try:
    dataset = client.get_dataset(dataset_id)
    print(f"✅ Dataset found: {dataset.dataset_id}")
except Exception as e:
    print(f"❌ Access failed: {e}")
