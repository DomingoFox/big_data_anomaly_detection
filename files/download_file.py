import requests
from tqdm import tqdm

url = "https://web.ais.dk/aisdata/aisdk-2024-08-28.zip"
local_filename = "aisdk-2024-08-28.zip"

response = requests.get(url, stream=True)
total_size = int(response.headers.get('Content-Length', 0))

with open(local_filename, "wb") as file:
    # Progress
    for data in tqdm(response.iter_content(chunk_size=1024), total=total_size//1024, unit='KB'):
        file.write(data)

local_filename  # Return the filename to indicate completion.
