import boto3
import csv
import os
import json
from io import StringIO
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def main(mytimer):
    # ==============================
    # Configuración S3
    # ==============================
    BUCKET_NAME = "emcs-ssm"
    PREFIX = "patch-compliance-reports/"
    REGION = "eu-west-1"
    MAX_THREADS = 10

    s3 = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )

    all_results = []

    # ==============================
    # Obtener clientes
    # ==============================
    def list_clients():
        paginator = s3.get_paginator("list_objects_v2")
        clients = []

        # patch-compliance-reports/ → cliente
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX, Delimiter="/"):
            for p in page.get("CommonPrefixes", []):
                client_name = p["Prefix"].replace(PREFIX, "").strip("/")
                clients.append(client_name)

        return clients

    # ==============================
    # Obtener JSONs de un cliente (recorre eu-west-1/año/mes/dia)
    # ==============================
    def list_json_keys(client_name):
        paginator = s3.get_paginator("list_objects_v2")
        client_path = f"{PREFIX}{client_name}/{REGION}/"
        json_keys = []

        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=client_path):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".json"):
                    json_keys.append(key)

        return json_keys

    # ==============================
    # Validar existencia de objeto
    # ==============================
    def object_exists(key):
        try:
            s3.head_object(Bucket=BUCKET_NAME, Key=key)
            return True
        except Exception:
            return False

    # ==============================
    # Leer archivo S3
    # ==============================
    def read_s3(key):
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            return obj["Body"].read().decode("utf-8", errors="ignore")
        except Exception:
            return ""

    # ==============================
    # Extraer metadata del JSON
    # ==============================
    def extract_metadata(text):
        try:
            data = json.loads(text)
        except Exception:
            return {}
        return data

    # ==============================
    # Procesar cliente
    # ==============================
    def process_client(client_name):
        results = []

        json_keys = list_json_keys(client_name)

        for key in json_keys:
            content = read_s3(key) if object_exists(key) else ""

            data = extract_metadata(content)
            if not data:
                continue

            results.append([
                data.get("InstanceId", ""),
                data.get("InstanceName", ""),
                data.get("AccountName", ""),
                data.get("OSName", ""),
                data.get("InstanceType", ""),
                data.get("InstanceState", ""),
                data.get("ComplianceStatus", ""),
                data.get("TotalPatches", ""),
                data.get("InstalledPatches", ""),
                data.get("MissingPatches", ""),
                data.get("FailedPatches", ""),
                data.get("MissingCritical", ""),
                data.get("MissingHigh", ""),
                data.get("MissingMedium", ""),
                data.get("InstalledRejectedPatches", ""),
                data.get("LastPatchOperation", ""),
                data.get("LastPatchOperationType", "")
            ])
        return results

    # ==============================
    # Ejecución principal
    # ==============================
    clients = list_clients()

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_client, client_name) for client_name in clients]
        for future in as_completed(futures):
            all_results.extend(future.result())

    # ==============================
    # Crear CSV en memoria
    # ==============================
    output = StringIO()
    writer = csv.writer(output, delimiter=';')

    writer.writerow([
        "InstanceId",
        "InstanceName",
        "AccountName",
        "OSName",
        "InstanceType",
        "InstanceState",
        "ComplianceStatus",
        "TotalPatches",
        "InstalledPatches",
        "MissingPatches",
        "FailedPatches",
        "MissingCritical",
        "MissingHigh",
        "MissingMedium",
        "InstalledRejectedPatches",
        "LastPatchOperation",
        "LastPatchOperationType"
    ])
    writer.writerows(all_results)

    # ==============================
    # Subir a Azure Blob Storage
    # ==============================
    connection_string = os.environ["AzureWebJobsStorage"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_name = "copydataexport"

    container_client = blob_service_client.get_container_client(container_name)

    for blob in container_client.list_blobs():
        if blob.name.endswith(".csv"):
            container_client.delete_blob(blob.name)

    now_str = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    blob_name = f"patch_compliance_{now_str}.csv"

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(output.getvalue(), overwrite=True)

    print(f"CSV subido correctamente: {container_name}/{blob_name}")
