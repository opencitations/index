import argparse
import os
import hashlib
import json
import requests
import yaml
from requests.exceptions import HTTPError
from tqdm import tqdm

# Endpoint base di Figshare
BASE_URL = "https://api.figshare.com/v2/account/articles"
CHUNK_SIZE = 1048576


def get_file_check_data(file_name):
    with open(file_name, "rb") as fin:
        md5 = hashlib.md5()
        size = 0
        data = fin.read(CHUNK_SIZE)  # circa 10MB
        while data:
            size += len(data)
            md5.update(data)
            data = fin.read(CHUNK_SIZE)
        return md5.hexdigest(), size


def issue_request(method, url, token, data=None, binary=False):
    headers = {"Authorization": "token " + token}
    if data is not None and not binary:
        data = json.dumps(data)
    response = requests.request(method, url, headers=headers, data=data)
    try:
        response.raise_for_status()
        try:
            data = json.loads(response.content)
        except ValueError:
            data = response.content
    except HTTPError as error:
        print(f"Caught an HTTPError: {str(error)}")
        print("Body:\n", response.text)
        raise
    return data


def upload_parts(file_info, file_path, token):
    url = file_info["upload_url"]
    result = issue_request(method="GET", url=url, token=token)
    print(f"\nUploading {os.path.basename(file_path)}:")

    # Calculate total size for progress bar
    total_size = sum(
        part["endOffset"] - part["startOffset"] + 1 for part in result["parts"]
    )

    with open(file_path, "rb") as fin:
        with tqdm(
            total=total_size, unit="B", unit_scale=True, unit_divisor=1024
        ) as pbar:
            for part in result["parts"]:
                chunk_size = part["endOffset"] - part["startOffset"] + 1
                upload_part(file_info, fin, part, token)
                pbar.update(chunk_size)


def upload_part(file_info, stream, part, token):
    udata = file_info.copy()
    udata.update(part)
    url = "{upload_url}/{partNo}".format(**udata)
    stream.seek(part["startOffset"])
    data = stream.read(part["endOffset"] - part["startOffset"] + 1)
    issue_request(method="PUT", url=url, data=data, binary=True, token=token)
    print("  Uploaded part {partNo} from {startOffset} to {endOffset}".format(**part))


def create_file(article_id, file_name, file_path, token):
    url = f"{BASE_URL}/{article_id}/files"
    headers = {"Authorization": f"token {token}"}
    md5, size = get_file_check_data(file_path)
    data = {"name": os.path.basename(file_name), "md5": md5, "size": size}
    post_response = requests.post(url, headers=headers, json=data)
    get_response = requests.get(post_response.json()["location"], headers=headers)
    return get_response.json()


def complete_upload(article_id, file_id, token):
    url = f"{BASE_URL}/{article_id}/files/{file_id}"
    issue_request(method="POST", url=url, token=token)
    print(f"  Upload completion confirmed for file {file_id}")


def main(config_path, article_id = None, files_to_upload = []):
    with open(config_path) as f:
        config = yaml.safe_load(f)

    if article_id != None and len(files_to_upload) == 0:
        print("Nothing to be done!")
        return False

    token = config["figshare_token"]
    article_id = article_id
    files_to_upload = files_to_upload

    print(f"Starting upload of {len(files_to_upload)} files to Figshare...")

    for file_path in tqdm(files_to_upload, desc="Total progress", unit="file"):
        file_name = os.path.basename(file_path)
        print(f"\nPreparing {file_name}...")
        file_info = create_file(article_id, file_name, file_path, token)
        upload_parts(file_info, file_path, token)
        complete_upload(article_id, file_info["id"], token)
        print(f"✓ {file_name} completed")

    print("\nAll files uploaded successfully to Figshare! 🎉")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload files to Figshare.")
    parser.add_argument("config", help="Path to the YAML configuration file.")
    parser.add_argument("article", help="Article id")
    parser.add_argument("files", help="file")
    args = parser.parse_args()
    main(args.config, args.article, args.files)
