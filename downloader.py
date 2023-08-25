# Downloader Python script for downloading video files using NASA OPEN API.
import requests
import wget
import os
import shutil
from tqdm import tqdm

def download_file(url, destinationDirectory, fileNameIndex):
    try:
        # Download the file from the given URL and save.
        fileName = wget.download(str(url))
        os.rename(fileName, fileNameIndex + '.mp4')
        shutil.move(str(fileIndexName) + '.mp4', destinationDirectory + '/' + str(fileIndexName) + '.mp4')
    except Exception as e:
        print(f"Error occurred: {str(e)}")

# Replace NASA API key
api_key = 'XxDnnqZ7MDAnLE2CCTXAjF9lVEZF2TPZAf0Qq2W4'

# Search query to find specific content
search_query = 'earth'

def get_nasa_assets(api_key, search_query):
    # NASA IVL API endpoint URL
    nasa_ivl_url = 'https://images-api.nasa.gov/search'

    # Set up the request parameters, including the API key
    params = {
        'q': search_query,
        'media_type': 'video'
    }

    try:
        # Make the API request
        response = requests.get(nasa_ivl_url, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Process the response data (convert from JSON)
            data = response.json()
            return data['collection']['items']
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None
    
def fetchVideoUrlFromList(itemsList):
    for item in itemsList:
        if(item['href'].endswith('large.mp4')):
            return item['href']
    
def get_nasa_video_link(nasa_id):
    nasa_asset_url = 'https://images-api.nasa.gov/asset/' + nasa_id

    try:
        # Make the API request
        response = requests.get(nasa_asset_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Process the response data (convert from JSON)
            data = response.json()
            return fetchVideoUrlFromList(data['collection']['items'])
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None    

videoLinks = []
# Get a list of NASA content related to the search query
nasa_assets = get_nasa_assets(api_key, search_query)

# Check if the data was retrieved successfully
if nasa_assets:
    # Display the details of each content received
    for asset_info in tqdm(nasa_assets):
        nasa_id = asset_info['data'][0]['nasa_id']
        videoLinks.append(get_nasa_video_link(nasa_id))
else:
    print("Failed to retrieve NASA IDs.")

destinationDirectory = './Videos'
fileIndexName = 1
for url in tqdm(videoLinks):
    if url is not None:
        download_file(url, destinationDirectory, str(fileIndexName))
        fileIndexName += 1
