"""
Harvard Library Historical Datasets Utility Functions Module

"""
from collections import OrderedDict
import configparser
import json
import math
import osfclient
import pandas as pd
import pprint
import re
import requests
import xmltodict

def mets_to_dataframe(filename):
    """
    Read and extract information about files from an XML METS file.

    Parameter
    ---------
    filename : str
        Full path to METS file.

    Return
    ------
    DataFrame

    """
    # validate filename
    if (not filename):
        return None
    # read mets file
    with open(filename) as fp:
        doc = xmltodict.parse(fp.read())
    # validate mets file
    if (not doc.get('mets')):
        return None
    # get the groups of files in the METS file
    file_grp = doc.get('mets').get('fileSec').get('fileGrp')
    # list of all files named in METS file
    files_list = []
    # iterate over the file groups
    for grp in file_grp:
        # get the file type
        file_type = grp.get('@USE')
        # get the files in the group
        files = grp.get('file')
        # iterate over the files in the group
        for file in files:
            # collect metadata from dictionaries, ignore other element types (e.g., str)
            if isinstance(file, dict):
                # create a row dictionary
                row = {}
                # set the file type
                row['file_type'] = file_type
                # get items from the ordered dict
                for item in file.items():
                    # get/set id
                    if (item[0] == '@ID'):
                        row['@id'] = item[1]
                    # get/set mimetype
                    elif (item[0] == '@MIMETYPE'):
                        row['@mimetype'] = item[1]
                    # get/set mets url and filename
                    elif (item[0] == 'FLocat'):
                        items = dict(item[1].items())
                        row['mets_url'] = items.get('@xlink:href')
                        split = row['mets_url'].split('/')
                        row['filename'] = split[1]
                # append the row to the list of file metadata
                files_list.append(row)

    # create a dataframe from the list of file metadata
    df = pd.DataFrame.from_records(files_list, index=None,
                                   columns=['@id','file_type','@mimetype','mets_url','filename'])
    return df


def osf_get_project_files(project_id, username, password, token):
    """
    Get metadata about all files for this OSF project.

    Note: This function makes many OSF API calls and may take a long time to run.

    Parameters
    ----------
    project_id : str
        Valid OSF id for the project url to the project storage root (e.g., sjtg9)
    username : str
        Valid OSF username
    password : str
        Valid password for username account
    token : str
        Valid OSF API key. 
        Login to OSF and create your API key here: https://osf.io/settings/tokenshttps://osf.io/settings/tokens

    Return
    ------
    dict :
        Keyed on filename
    """
    # initialize the osf client
    client = osfclient.OSF(username=username, password=password, token=token)
    # get the project
    project = client.project(project_id)
    # get the storage associated with the project
    storage = project.storage(provider='osfstorage')
    # get the files and their HTML links
    # caution: the osfclient does NOT publish the HTML link as a public attribute!
    files = {}
    for file in storage.files:
        # populate the file metadata
        fm = {}
        fm['name'] = file.name
        fm['osf_path'] = file.osf_path
        fm['path'] = file.path
        fm['html_url'] = file._html_url
        fm['size'] = file.size
        fm['date_created'] = file.date_created        
        fm['date_modified'] = file.date_modified
        fm['file_type'] = file.name.split('.')[1]
        files[file.name] = fm
    return files

def osf_files_to_dataframe(files):
    """
    Save metadata about project files to a `DataFrame`.

    Parameter
    ---------
    osf_files : dict
        Dictionary of file metadata (e.g., returned from call to osf_get_project_files)

    Return
    ------
    DataFrame
    """
    # valid dict of files must be supplied
    if ((not files) or
        (not isinstance(files,dict))):
        return None
    
    records = []
    for file in files.keys():
        records.append(files[file])
    
    # create dataframe
    df = pd.DataFrame.from_dict(data=records,orient='columns')
    return df

def map_csv_to_image(image_list, csv_list):
    """
     Given a list of image names and a list of csv file names, 
     this function deduces which csv file is related to a image file.


    Parameter
    ---------
    image_list : list
        list of image file names
    csv_list :
        list of csv files

    Return
    ------
    list
        List of dict, with one dict per csv file and matching image
    """
    # parameters must be supplied
    if ((not image_list) or
        (len(image_list) == 0) or
        (not csv_list) or
        (len(csv_list)== 0)):
        return None
    # results
    results = []
    # for each csv file, find a related image file
    for csv_file in csv_list:
        # matched?
        matched = False
        # for each image file, find related csv files
        for image in image_list:
            # get first element of the image name
            # note: some files in this folder have *.txt extensions; ignore them
            name = image.split('.jpg')[0]
            # test for match
            regex = '^{}'.format(name)
            matches = re.search(regex, csv_file)
            if (matches):
                results.append({csv_file:image})
                matched = True
        # if no matches were found
        if (matched == False):
            results.append({csv_file:''})
            pprint.pprint('Warning: unmatched file: {}'.format(csv_file))
    # otherwise, return results
    return results


def iiif_to_dataframe(filename):
    """
    Given a IIIF JSON manifest, save some of its values to a DataFrame

    Parameter
    ---------
    filename : str
        Full path to IIIF JSON manifest file.

    Return
    ------
    list
        List of dict of metadata about contents of IIIF manifest
    """
    # validate filename
    if (not filename):
        return None
    # read iiif json file
    with open(filename) as fp:
        doc = json.loads(fp.read())
    # get the sequences
    sequences = doc.get('sequences')[0]
    # get canvases
    canvases = sequences.get('canvases')
    resources = []
    # gather iiif metadata for each canvas element
    for canvas in canvases:
        images = canvas.get('images')[0]
        iiif_id = images.get('resource').get('service').get('@id')
        tokens = iiif_id.split('/')
        resource = {
            '@id': images.get('resource').get('@id'),
            'format':images.get('resource').get('format'),
            'drs_id': tokens[len(tokens)-1]
        }
        # add resource to resources list
        resources.append(resource)
        
    # save resources to dataframe
    df = pd.DataFrame.from_dict(data=resources,orient='columns')
    return df

# end file