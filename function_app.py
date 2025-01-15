import azure.functions as func
import logging
import os
import json
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
SUBSCRIPTION_ID = "<your-subscription-id>"
RESOURCE_GROUP_NAME = "<your-resource-group>"
DATA_FACTORY_NAME = "<your-data-factory-name>"
PIPELINE_NAME = "<your-pipeline-name>"

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="event-hub",
                               connection="event_hub_listen") 
def eventhub_trigger(azeventhub: func.EventHubEvent):
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
