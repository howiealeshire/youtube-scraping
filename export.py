import os
from datetime import datetime
from os import listdir
from os.path import isfile, join
from pprint import pprint

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__



def list_blobs():
    print("\nListing blobs...")
    container_client = ContainerClient()
    # List the blobs in the container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)

def main_azure(local_file_name, local_path):
    """DefaultEndpointsProtocol=https;
    AccountName=tiktokscraper;
    AccountKey=DJTlaX2lUjgsxZTF6Oy37REj7EUTvLpmxooE/cJUnD3dr48xiFz8RSt8YhZJ6Rbi1yxJ1WNnCyjXR8jYRifDKQ==;
    EndpointSuffix=core.windows.net"""
    try:

        # local_file_name = "top_instagram_2020-10-06.csv" #"top_youtube_2020-09-25.csv" #"analysis_youtube_2020-09-22.csv"
        upload_file_path = os.path.join(local_path, local_file_name)

        print("Azure Blob storage v" + __version__ + " - Python quickstart sample")
        connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        # "analysis_<platform>_<scraped date in YYYY-MM-DD format>.csv" (e.g. analysis_youtube_2020-09-16.csv)
        container_name = "tiktokscraperdata"
        print("Container name: ")
        print(container_name)
        container_client = ContainerClient.from_connection_string(conn_str=connect_str, container_name=container_name)
        container_client.delete_blob(blob=local_file_name)

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)
        print(connect_str)
        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data)
        # Quick start code goes here
    except Exception as ex:
        print('Exception:')
        print(ex)




def run_exports():
    def get_file_names(chosen_date=datetime.today().strftime('%Y-%m-%d'), platforms=('instagram', 'youtube')):
        filename_list = []
        for platform in platforms:
            top_users = "analysis_" + platform + "_" + chosen_date + ".csv"
            top_posts = "top_" + platform + "_" + chosen_date + ".csv"
            filename_list.append(top_users)
            filename_list.append(top_posts)
        return filename_list

    filenames = get_file_names()
    for filename in filenames:
        main_azure(filename)


def run_exports_temp(mypath='add_already_existing_follower_counts4'):
    def get_file_names(mypath):
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        top_post_files = []
        top_users_files = []
        # for elem in onlyfiles:
        # top_users_files.append(elem)
        # if "instagram" in elem:
        #    if "top" in elem:
        #        top_post_files.append(elem)
        #    else:
        #        top_users_files.append(elem)
        return onlyfiles

    filenames = get_file_names(mypath)
    pprint(filenames)
    for filename in filenames:
        main_azure(filename,mypath)

