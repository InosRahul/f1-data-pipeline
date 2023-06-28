from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_dbt.cli import BigQueryTargetConfigs, DbtCliProfile

your_GCS_bucket_name = "f1-data-bucket"
gcs_credentials_block_name = "f1-analysis"

credentials_block = GcpCredentials(
    service_account_file=r"/Users/rahulsoni/.google/credentials/google_credentials.json"
)

credentials_block.save(f"{gcs_credentials_block_name}", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load(f"{gcs_credentials_block_name}"),
    bucket=f"f1-data-bucket",
)

bucket_block.save(f"{gcs_credentials_block_name}-bucket", overwrite=True)


credentials = GcpCredentials.load(gcs_credentials_block_name)
target_configs = BigQueryTargetConfigs(
    schema="f1_dataset",
    credentials=credentials,
)
target_configs.save("f1-dbt-target-config", overwrite=True)

dbt_cli_profile = DbtCliProfile(
    name="f1-dbt-cli-profile",
    target="dev",
    target_configs=target_configs,
)
dbt_cli_profile.save("f1-dbt-cli-profile", overwrite=True)
