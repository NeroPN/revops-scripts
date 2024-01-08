import requests
import json
import os
from PIL import Image, UnidentifiedImageError
from io import BytesIO
import time


# Install all necessary python libraries, run:
# pip install requests
# pip install Pillow

# Configurable variables
BEARER_TOKEN = 'YOUR_TOKEN_HERE'
MIN_FILE_SIZE_FOR_COMPRESSION = 800000  # Minimum size the image should have to be compressed in bytes (800kb)
SLEEP_TIMER = 1  # Time in seconds to wait between image request & compressions

# File and directory paths (created automatically in the folder where this is running)
IMAGES_JSON_PATH = 'images.json'
COMPRESSED_IMAGES_DIR = 'compressed_images'
COMPRESSED_LOG_JSON_PATH = 'compressed_images_log.json'

def fetch_images():
    url = 'https://api.hubapi.com/filemanager/api/v2/files'
    headers = {'Authorization': f'Bearer {BEARER_TOKEN}'}
    all_images = []
    offset = 0
    limit = 500
    jpg_quality = 75

    while True:
        params = {'type': 'IMG', 'limit': limit, 'offset': offset}
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            all_images.extend(data['objects'])
            if len(data['objects']) < 499:
                break
            offset += limit
        else:
            print(f"Request failed. Status code: {response.status_code}")
            print(f"Response message: {response.text}")
            break

    return all_images

def compress_images(image_list):
    compressed_images_log = []
    os.makedirs(COMPRESSED_IMAGES_DIR, exist_ok=True)

    for img_info in image_list:
        if img_info['size'] > MIN_FILE_SIZE_FOR_COMPRESSION:
            try:
                response = requests.get(img_info['url'])
                if response.status_code == 200:
                    with Image.open(BytesIO(response.content)) as img:
                        original_extension = img.format.lower()  # Get the original image's format
                        
                        # Convert to RGB if necessary (for PNGs with transparency)
                        if img.mode == 'RGBA':
                            img = img.convert('RGB')

                        compressed_path = os.path.join(COMPRESSED_IMAGES_DIR, f"{img_info['id']}.{original_extension}")
                        img.save(compressed_path, original_extension.upper(), optimize=True)

                        compressed_images_log.append({'id': img_info['id'], 'path': compressed_path})
                        print(f"Compressed and saved image {img_info['id']}")
                time.sleep(SLEEP_TIMER)
            except UnidentifiedImageError as e:
                print(f"Error processing image ID {img_info['id']}: {e}")

    with open(COMPRESSED_LOG_JSON_PATH, 'w') as log_file:
        json.dump(compressed_images_log, log_file, indent=4)

def replace_images():
    headers = {'Authorization': f'Bearer {BEARER_TOKEN}'}

    with open(COMPRESSED_LOG_JSON_PATH, 'r') as log_file:
        compressed_images = json.load(log_file)

    for img in compressed_images:
        endpoint = f'https://api.hubapi.com/filemanager/api/v3/files/{img['id']}/replace'
        files_data = {
            'file': (os.path.basename(img['path']), open(img['path'], 'rb'), 'application/octet-stream'),
            'options': (None, json.dumps({'access': 'PUBLIC_INDEXABLE'}), 'application/json')
        }

        response = requests.post(endpoint, headers=headers, files=files_data)
        files_data['file'][1].close()

        if response.status_code == 200:
            print(f"Successfully replaced file ID {img['id']}")
        else:
            print(f"Failed to replace file ID {img['id']}. Status code: {response.status_code}")
            print(f"Response: {response.text}")

# Main execution
print("Fetching images...")
images = fetch_images()
with open(IMAGES_JSON_PATH, 'w') as json_file:
    json.dump(images, json_file, indent=4)

print("Compressing images...")
compress_images(images)

print("Replacing images...")
replace_images()
