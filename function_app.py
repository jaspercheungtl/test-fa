import azure.functions as func
import logging
import os
import json
import requests
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.mgmt.datafactory import DataFactoryManagementClient

app = func.FunctionApp()
connection_string = os.getenv("dlsmhsalabp2_connectionstring")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
landing_container_name = "01_landing_zone"
container_name = "03-enriched-zone"
# Azure Key Vault URL
KEY_VAULT_URL = "https://akv-mhsa-lab-p2.vault.azure.net/"

# Data Factory and Azure details
SUBSCRIPTION_ID = "16b68e2e-0459-47f3-a050-e757d1e587ce"
RESOURCE_GROUP_NAME = "rg-mhsa"
DATA_FACTORY_NAME = "adf-mhsa-lab-dev"
PIPELINE_NAME = "PL_Wait_5sec"

# Azure REST API endpoint
ADF_PIPELINE_RUN_URL = (
    f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP_NAME}/"
    f"providers/Microsoft.DataFactory/factories/{DATA_FACTORY_NAME}/pipelines/{PIPELINE_NAME}/createRun?api-version=2018-06-01"
)

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="event-hub",
                               connection="event_hub_listen") 
def eventhub_trigger(azeventhub: func.EventHubEvent):
    try: 
        logging.info('Python EventHub trigger2 processed an event: %s',
                    azeventhub.get_body().decode('utf-8'))
        json_data2 = azeventhub.get_body().decode('utf-8')
        blob_name2 = "ou.json"
        blob_client2 = blob_service_client.get_blob_client(container=container_name, blob=blob_name2)
        blob_client2.upload_blob(json_data2, overwrite=True)
        logging.info(f"JSON saved to Data Lake as {blob_name2}")
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
        # Retrieve credentials from Key Vault
        username = secret_client.get_secret("mhsa-api-username").value
        password = secret_client.get_secret("mhsa-api-pwd").value
        auth_string = f"{username}:{password}"
        logging.info(f"{auth_string}")
        token = credential.get_token("https://management.azure.com/.default").token
        # Pipeline parameters (if any)
        payload = {
            "param1": "value1",
            "param2": "value2"
        }

        # Headers with authorization token
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Trigger the pipeline
        response = requests.post(ADF_PIPELINE_RUN_URL, headers=headers, json=payload)

        if response.status_code == 200:
            return func.HttpResponse(
                f"Pipeline triggered successfully. Response: {response.json()}",
                status_code=200
            )
        else:
            return func.HttpResponse(
                f"Failed to trigger pipeline. Status: {response.status_code}, Error: {response.text}",
                status_code=response.status_code
            )
    except Exception as e:
        logging.error(f"Error triggering Data Factory pipeline: {e}")
        return func.HttpResponse("Failed to trigger pipeline.", status_code=500)

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="event-hub2",
                               connection="event_hub2_listen") 
def eventhub2_trigger(azeventhub: func.EventHubEvent):
    logging.info('Python EventHub trigger processed an event: %s',
                azeventhub.get_body().decode('utf-8'))
    json_data = azeventhub.get_body().decode('utf-8')
    blob_name = "output.json"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(json_data, overwrite=True)
    logging.info(f"JSON saved to Data Lake as {blob_name}")
