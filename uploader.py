import json

import httpx

ACCESS_TOKEN = "<WRITE_YOUR_ACCESS_TOKEN_HERE>"
params = {'access_token': ACCESS_TOKEN}
base_url = "https://sandbox.zenodo.org"

"""
File which contains records and file paths

file content should like this:

{"metadata": {"communities": [], "upload_type": "dataset", "license": "cc-by", "title": "Example Dataset Part #482456", "creators": [{"name": "Doe, John", "affiliation":"Yet Another University"}, {"name":"Doe, Jane","affiliation":"Yet Another University"}], "publication_date": "2022-01-01", "description": "<p>This is example record</p>"}, "filedata": "dataset_files/482456.zip"}
{"metadata": {"communities": [], "upload_type": "dataset", "license": "cc-by", "title": "Example Dataset Part #482457", "creators": [{"name": "Doe, John", "affiliation":"Yet Another University"}, {"name":"Doe, Jane","affiliation":"Yet Another University"}], "publication_date": "2022-01-01", "description": "<p>This is example record</p>"}, "filedata": "dataset_files/482457.zip"}
{"metadata": {"communities": [], "upload_type": "dataset", "license": "cc-by", "title": "Example Dataset Part #482458", "creators": [{"name": "Doe, John", "affiliation":"Yet Another University"}, {"name":"Doe, Jane","affiliation":"Yet Another University"}], "publication_date": "2022-01-01", "description": "<p>This is example record</p>"}, "filedata": "dataset_files/482458.zip"}

For more info on Zenodo metadata scheme, see. https://developers.zenodo.org/#depositions
"""
records_file = open("records.json", "r")


def add_metadata(metadata):
    """ This function create record with given metadata entities. """
    request = httpx.post(f'{base_url}/api/deposit/depositions', json={"metadata": metadata},
                         params=params, timeout=300)
    if request.status_code != 201:
        raise ValueError("Record creation exception: " + request.text)
    else:
        return request.json()


def upload_file(file_path, bucket_url):
    """ This function uploads file to created record. """
    file_obj = open(file_path, "rb")
    file_req = httpx.put(
        f"{bucket_url}/{file_path.split('/')[-1]}",
        content=file_obj,
        params=params, timeout=300.0)
    if file_req.status_code != 200:
        raise ValueError("File upload exception: " + file_req.text)

    return file_req


def publish(_id):
    """ This function publishes the record. """
    pub_req = httpx.post(f"{base_url}/api/deposit/depositions/{_id}/actions/publish", params=params,
                         timeout=300.0)
    if pub_req.status_code != 202:
        raise ValueError("Publish exception: " + pub_req.text)


def send_records():
    with open("upload_log.json", "w+") as log_file:
        for line in records_file.readlines():
            if line:
                line = json.loads(line)
                # Read file line by line and read metadata.
                metadata = line["metadata"]
                result = add_metadata(metadata)
                if result:
                    # If record created, upload file to record.
                    upload_file(line["filedata"], result["links"]["bucket"])
                    # Publish the record if upload is successful
                    publish(result["id"])
                    # Log the created record id and record title for later use
                    log = json.dumps({"record_id": result["id"], "title": result["metadata"]["title"]})
                    log_file.write(f"{log}\n")
                    # Print the which record has been published
                    print(log)


if __name__ == '__main__':
    send_records()
