import boto3
import csv
import os
import json
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from azure.storage.blob import BlobServiceClient
from datetime import datetime


def main(mytimer):

    # ==============================
    # Cliente de S3
    # ==============================
    s3 = boto3.client(
        "s3",
        region_name="eu-west-1",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
    )

    bucket_name = "emcs-ssm"
    prefix = "patch-compliance-reports/"
    max_threads = 10

    # ==============================
    # Obtener clientes
    # ==============================
    clients = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter="/"
    ):
        for p in page.get("CommonPrefixes", []):

            client_name = p["Prefix"].replace(
                prefix,
                ""
            ).strip("/")

            if client_name:
                clients.append(client_name)

    print(f"Total de clientes encontrados: {len(clients)}")

    # ==============================
    # Procesar cliente
    # ==============================
    def process_client(client_name):

        results = []

        path = f"{prefix}{client_name}/eu-west-1/"

        paginator = s3.get_paginator("list_objects_v2")

        json_files = []

        for page in paginator.paginate(
            Bucket=bucket_name,
            Prefix=path
        ):
            for obj in page.get("Contents", []):

                key = obj["Key"]

                if key.lower().endswith(".json"):
                    json_files.append(key)

        print(
            f"Cliente {client_name}: "
            f"{len(json_files)} JSON encontrados"
        )

        # ==============================
        # Leer JSON
        # ==============================
        for key in json_files:

            try:

                obj = s3.get_object(
                    Bucket=bucket_name,
                    Key=key
                )

                content = obj["Body"].read().decode(
                    "utf-8",
                    errors="ignore"
                )

                data = json.loads(content)

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

            except Exception as e:

                print(
                    f"Error leyendo JSON {key}: {e}"
                )

        return results

    # ==============================
    # Procesar clientes en paralelo
    # ==============================
    all_results = []

    with ThreadPoolExecutor(
        max_workers=max_threads
    ) as executor:

        futures = [
            executor.submit(
                process_client,
                client_name
            )
            for client_name in clients
        ]

        for future in as_completed(futures):

            try:
                all_results.extend(
                    future.result()
                )

            except Exception as e:

                print(
                    f"Error procesando cliente: {e}"
                )

    print(
        f"Total de registros obtenidos: "
        f"{len(all_results)}"
    )

    # ==============================
    # Crear CSV en memoria
    # ==============================
    output = StringIO()

    writer = csv.writer(output)

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
    # Subir a Blob Storage
    # ==============================

    connection_string = os.environ[
        "AzureWebJobsStorage"
    ]

    blob_service_client = (
        BlobServiceClient.from_connection_string(
            connection_string
        )
    )

    container_name = "copydataexport"

    container_client = (
        blob_service_client.get_container_client(
            container_name
        )
    )

    # ==============================
    # Eliminar CSV anterior
    # ==============================
    for blob in container_client.list_blobs():

        if blob.name.lower().endswith(".csv"):

            container_client.delete_blob(
                blob.name
            )

            print(
                f"CSV anterior eliminado: "
                f"{blob.name}"
            )

    # ==============================
    # Nombre del nuevo CSV
    # ==============================
    now_str = datetime.utcnow().strftime(
        "%Y-%m-%d_%H-%M-%S"
    )

    blob_name = (
        f"patch_compliance_{now_str}.csv"
    )

    # ==============================
    # Subir CSV
    # ==============================
    blob_client = blob_service_client.get_blob_client(
        container=container_name,
        blob=blob_name
    )

    blob_client.upload_blob(
        output.getvalue(),
        overwrite=True
    )

    print(
        f"CSV subido a "
        f"{container_name}/{blob_name}"
    )