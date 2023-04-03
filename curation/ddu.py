"""
Dataverse Direct File Upload

This code directly uploads a single file ("filename") from a folder ("path") into an S3 bucket.
This only works if direct upload is enabled on the dataset specified by dataset_pid.
The method handles the initial negotiation with Dataverse obtaining a pre-authorized upload
url, then performs the actual upload via a PUT request on this url.
It DOES NOT finalize saving the file with the Dataverse, but it returns a dict with the
metadata that the separate finalize method will need to send to Dataverse. This way multiple files
can be uploaded and these metadata entries collected, and then finalized with the Dataverse all at once.
This way there's only one update, one reindexing etc. 

Source: https://github.com/IQSS/dataverse.harvard.edu/blob/191-python-direct-upload/util/python/direct-upload/directupload.py

"""

import sys
import os
import re
import requests
import json
import hashlib

def direct_upload(dataverse_url, dataset_pid, key, filename, path, mime_type, retries=10):
    data_id = None
    if path is not None:
        file_path = path + "/" + filename
    else:
        file_path = filename
    
    file_size = os.stat(file_path).st_size 
    # start with a call to Dataverse to obtain a "ticket" for the upload to S3:
    while retries > 0:
        url_string = dataverse_url + "/api/datasets/:persistentId/uploadurls"
        url_string = url_string + "?persistentId=" + dataset_pid + "&key=" + key + "&size=" + str(file_size)

        #print("url string: "+url_string)
        
        response = requests.get(url_string)

        if response.status_code == 200:
            upload_url = None
            storage_identifier = None
            max_part_size = None
            if 'data' in response.json().keys():
                response_data = response.json()['data']
                if 'url' in response_data.keys():
                    upload_url = response_data['url']
                if 'storageIdentifier' in response_data.keys():
                    storage_identifier = response_data['storageIdentifier']
                if 'partSize' in response_data.keys():
                    max_part_size = response_data['partSize']
                    

                if upload_url is not None and storage_identifier is not None:
                    # will attempt to make a Put request to upload the file to the bucket:
                    print("upload url: "+upload_url)
                    #print("storage identifier: "+storage_identifier)
                    #files = {'upload_file': open(file_path,'rb')}
                    upload_response = requests.put(upload_url, data=open(file_path, 'rb'), headers={'x-amz-tagging': 'dv-state=temp'},)

                    if upload_response.status_code == 200:
                        # Calculate MD5:
                        # (this is inefficient - we are going to read the file the second time
                        # but it should work for reasonable-sized files)
                        with open(file_path, "rb") as f:
                            file_hash = hashlib.md5()
                            while chunk := f.read(8192):
                                file_hash.update(chunk)

                        md5_hash = file_hash.hexdigest()
                        
                        json_data = {
                            "storageIdentifier": storage_identifier,
                            "fileName": filename,
                            "mimeType": mime_type,
                            "md5Hash": md5_hash,
                            "fileSize": file_size,
                            }

                        if path is not None:
                            json_data["directoryLabel"] = re.sub('^/', '', path)

                        #json_string = json.dumps(json_data)
                        return json_data
                    else:
                        print("Direct upload to S3 bucket failed. (giving up)")
                        retries = 0

                else:
                    # uploads larger than _partSize_ not supported yet;
                    # but that's 1GB with AWS, so we should be ok
                    retries = 0
            
            else:
                print("Invalid response from Dataverse (no data), retrying")
                retries = retries - 1
            
        else:
            print("Received return code: " + str(response.status_code) + ", retrying")
            retries = retries - 1

    # If we have reached here, that means we have failed.
    return None

def finalize_direct_upload(dataverse_url, dataset_pid, json_data, key):
    url_string = dataverse_url + "/api/datasets/:persistentId/addFiles"
    url_string = url_string + "?persistentId=" + dataset_pid + "&key=" + key

    #print("url string: "+url_string)

    json_string = json.dumps(json_data)

    # One moderately counter-intuitive part about Python requests library
    # that we are running into here:
    # Do not expect
    #    response = requests.post(url_string, data={'jsonData': json_string})
    # to work here. Why - doesn't it work just fine when we use this line for
    # uploading files??
    # - apparently, without any files being uploaded, requests are not going to use
    # multi-part form encoding that Dataverse expect. In order to make it happen,
    # we must create the multipart dict explicitly AND pass it to .post via
    # the "files" parameter, even though we are NOT uploading any files:
    
    multipart_form_data = {
        'jsonData': (None, json_string)
    }
    response = requests.post(url_string, files=multipart_form_data)

    # neat (and weird), huh? 

    if response.status_code == 200:
        return True
    else:
        print("/addFiles call failed. Return code: "+str(response.status_code))
        return False
