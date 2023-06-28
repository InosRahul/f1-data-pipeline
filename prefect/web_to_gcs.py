from google.cloud.storage import Client
from google.cloud.bigquery import LoadJobConfig, SourceFormat, Client as QueryClient
import requests
import zipfile
import os
import time
import fileinput
import csv
from prefect import flow, task
from prefect_dbt import DbtCliProfile, DbtCoreOperation


os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = r"/Users/rahulsoni/.google/credentials/google_credentials.json"


@task(log_prints=True)
def createDownloadsFolder():
    if not os.path.exists("downloads"):
        os.makedirs("downloads")


@task(log_prints=True)
def unzipFileToCSV(zipPath: str):
    zip_file = zipfile.ZipFile(zipPath, "r")
    for names in zip_file.namelist():
        zip_file.extract(names, os.path.join("downloads"))
    zip_file.close()


@task(log_prints=True)
def deleteZipFile(zipPath: str):
    os.remove(zipPath)


@task(log_prints=True)
def deleteCSVfiles(csvPath: str):
    os.remove(csvPath)


@flow(log_prints=True, name="Fetch F1 Data", retries=3)
def downloadFile(uri: str, file_path: str):
    # get filename from url
    uri_res_name = uri.split("/")[-1]

    # create path where file will be saved
    filepath = os.path.join(file_path, uri_res_name)
    timeNow = time.time()

    r = requests.get(uri, stream=True)
    if r.ok:
        print("saving to:", os.path.abspath(filepath))
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
    else:
        print("Download failed, status code {}\{}".format(r.status_code, r.text))
        return "Error downloading file"

    unzipFileToCSV(filepath)
    deleteZipFile(filepath)
    return f"Total Download Execution Time: {time.time() - timeNow} seconds"


@task(log_prints=True)
def removeNulls(filepath: str):
    timeNow = time.time()
    for filename in os.listdir(filepath):
        if filename.endswith(".csv"):
            file_path = os.path.join(filepath, filename)

            with fileinput.FileInput(file_path, inplace=True) as input_file:
                temp_file_path = f"{file_path}.tmp"
                with open(temp_file_path, "w", newline="") as temp_file:
                    writer = csv.writer(temp_file)

                    for row in csv.reader(input_file):
                        # Replace null characters with an empty string in each cell of the row
                        modified_row = [cell.replace(r"\N", "") for cell in row]
                        writer.writerow(modified_row)

            os.replace(temp_file_path, file_path)

    return f"Total Time Removing nulls: {time.time() - timeNow} seconds"


@task(log_prints=True)
def deleteFiles(filepath: str):
    timeNow = time.time()
    for filename in os.listdir(filepath):
        file_path = os.path.join(filepath, filename)
        os.remove(file_path)

    os.rmdir(filepath)
    return f"Total Time Removing csv files: {time.time() - timeNow} seconds"


@task(log_prints=True, name="Upload to cloud storage")
def uploadToStorage(bucket_name: str):
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    timeNow = time.time()

    for file_name in os.listdir("downloads"):
        file_path = os.path.join("downloads", file_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_path)

    return f"Total Upload Execution Time: {time.time() - timeNow} seconds"


@task(log_prints=True, name="Make Table and load data in Big Query")
def makeBigQueryTables(dataset_id: str, bucket_name: str):
    storage_client = Client()
    bigquery_client = QueryClient()

    timeNow = time.time()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    for blob in blobs:
        file_name = blob.name
        table_id = file_name.replace(".csv", "").replace("/", "_")
        table_ref = bigquery_client.dataset(dataset_id).table(table_id)

        job_config = LoadJobConfig()
        job_config.source_format = SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = True

        load_job = bigquery_client.load_table_from_uri(
            f"gs://{bucket_name}/{file_name}", table_ref, job_config=job_config
        )

        load_job.result()

    return f"Total Table Created Time: {time.time() - timeNow} seconds"


@task(log_prints=True, name="Runs dbt to transform the data and derive columns")
def trigger_dbt_flow() -> object:
    """Triggers the dbt dependency and build commands"""

    dbt_cli_profile = DbtCliProfile.load("f1-dbt-cli-profile")

    with DbtCoreOperation(
        commands=[
            "dbt debug",
            "dbt deps",
            "dbt seed",
            "dbt build",
        ],
        project_dir="~/Documents/f1-data-pipeline/dbt/",
        profiles_dir="~/Documents/f1-data-pipeline/dbt/",
    ) as dbt_operation:
        dbt_process = dbt_operation.trigger()
        dbt_process.wait_for_completion()
        result = dbt_process.fetch_result()
    return result


@flow(log_prints=True, name="F1 data to Big Query")
def api_to_bq(
    storage_bucket: str, dataset_id: str, file_path: str, update_prod_table: bool
):
    createDownloadsFolder()

    download_url = "https://ergast.com/downloads/f1db_csv.zip"

    print(downloadFile(download_url, file_path))

    print(removeNulls(file_path))

    print(uploadToStorage(storage_bucket))

    print(makeBigQueryTables(dataset_id, storage_bucket))

    if update_prod_table == True:
        trigger_dbt_flow()
        print("Successfully updated the production table.")

    print(deleteFiles(file_path))


def main():
    storage_bucket = "f1-data-bucket"
    file_path = "downloads"
    dataset_id = "f1_dataset"
    update_prod_table = True
    api_to_bq(storage_bucket, dataset_id, file_path, update_prod_table)


if __name__ == "__main__":
    main()
