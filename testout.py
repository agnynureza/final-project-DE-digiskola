import requests

api_url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'

try:
    response = requests.get(api_url, stream=True)
    if response.status_code == 200:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                with open('output.txt', 'ab') as file:
                    file.write(chunk)
    else:
        print(f"HTTP request failed with status code {response.status_code}")
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")