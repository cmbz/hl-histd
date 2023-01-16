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

    Caution: This function makes many OSF API calls and may take a long time to run.
    It is also does not handle multiple large folders of material, either. 
    Use extreme caution when executing this function.

    TO DO: refactor function to handle multiple top-level folders. 

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
     Given a list of image names (with file extension) and a list of csv file names, 
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

def create_digital_object_inventory(iiif_df):
    """
    Given a DataFrame of IIIF manifest information (retrieved from 'iiif_to_dataframe'),
    process the metadata and return a DataFrame of information about the files
    that can be easily compared to other file inventories.

    Parameter
    ---------
    iiif_df : DataFrame
        Output of call to iiif_to_dataframe
    
    Return
    ------
    DataFrame
    """
    # check for empty dataframe
    if (iiif_df.empty == True):
        return pd.DataFrame()

    # create a copy
    df = iiif_df.copy(deep=True)
    # rename columns appropriately
    df.rename(columns = {'@id':'url','format':'mimetype'},errors='raise',inplace=True)
    # create a file_type column (derived from format/mimetype)
    df['file_type'] = ''
    for row in df.iterrows():
        index = row[0]
        mimetype = row[1]['mimetype']
        file_type = mimetype.split('/')[0]
        df.at[index, 'file_type'] = file_type
    return df

def create_vendor_inventory(mets_df, path=None):
    """
    Given a DataFrame of METS information (retrieved from 'mets_to_dataframe'),
    process the metadata and return a DataFrame of information about the files
    that can be easily compared to other file inventories.

    Parameters
    ----------
    mets_df : DataFrame
        Output of call to mets_to_dataframe
    path : str (optional)
        Full path to directory of data files
    
    Return
    ------
    DataFrame
    """
    # check for empty dataframe
    if (mets_df.empty == True):
        return pd.DataFrame()
    # check for path
    filepaths = False
    if (not (path == None)):
        filepaths = True
    # create a copy
    df = mets_df.copy(deep=True)
    # if file paths, apply them
    if (filepaths):
        df['filepath'] = df.apply(lambda row: path + '/' + row.mets_url, axis=1)
    # create drs ids by removing file extension
    # assumes that the filenames are based upon the drs id
    splitnames = df['filename'].str.split('.')
    drs_ids = splitnames.apply(lambda x : x[0])
    import re
    drs_ids = drs_ids.apply(lambda x : re.sub(r'_.*$', '', x))
    df['drs_id'] = drs_ids
    df = df.drop(columns = ['@id','mets_url'])
    # rename columns appropriately
    df.rename(columns = {'@mimetype':'mimetype'},errors='raise',inplace=True)
    return df

def find_missing_drs_ids(do_drs_ids, vendor_drs_ids):
    """
    Given two pandas Series, one of digital object DRS ids and another of
    vendor DRS ids, identify digital object DRS ids that do not appear 
    in the vendor_drs_ids list

    Parameters
    ----------
    do_drs_ids : Series
        Series of digital object DRS ids
    vendor_drs_ids : Series
        Series of vendor DRS ids

    Raise
    -----
    ValueError
        Too many distinct vendor DRS ids detected
        Vendor DRS id/s not found in digital object
    
    Return
    ------
    DataFrame
        List of missing DRS ids, empty if none

    """
    # check for empty series
    if (do_drs_ids.empty == True):
        return pd.DataFrame()
    if (vendor_drs_ids.empty == True):
        return pd.DataFrame()

    # digital object drs ids cannot contain duplicates
    duplicates = do_drs_ids.duplicated(keep='first')
    if (True in duplicates.values):
        raise ValueError('Digital object has one or more duplicate DRS ids')

    # sort the values
    do_drs_ids = do_drs_ids.sort_values(ascending=True)
    vendor_drs_ids = vendor_drs_ids.sort_values(ascending=True)

    # drop duplicates
    v2_drs_ids = vendor_drs_ids.drop_duplicates(keep='first')

    # create sets of drs ids to perform set operations
    set1 = set(do_drs_ids.tolist())
    set2 = set(v2_drs_ids.tolist())

    # are the sets are equal?
    if (set1 == set2):
        return pd.DataFrame()

    # values in vendor must appear in digital object
    if (not (set2.issubset(set1))):
        raise ValueError('Digital object does not contain one or more vendor DRS ids')
           
    # difference between digital object and vendor
    diff = set1.difference(set2)
    return pd.DataFrame(diff)

def extract_transcription_inventory(vendor_inventory_df, ttype='csv', path=False):
    """
    Given a vendor output inventory, extract the named type of transcription files.
    Valid types: 'csv' or 'txt'.

    Parameters
    ----------
    vendor_inventory_df : DataFrame
        Vendor inventory, as output from call to: `create_vendor_inventory`
    ttype : str (default = csv)
        Transcription type, either csv or txt
    path : bool
        Preserve the existence of inventory file paths, if desired

    Raise
    -----
    ValueError 
        Unknown transcription type
    
    Return
    ------
    DataFrame  
    """
    # check for empty inventory
    if (vendor_inventory_df.empty == True):
        return pd.DataFrame()
    # check for invalid transcription type
    if (ttype not in ['csv','txt']):
            msg = 'Unknown transcription type: {}'.format(ttype)
            raise ValueError(msg)
    # check for existence of path, if needed
    if ((path == True) and 
        not('filepath' in vendor_inventory_df.columns)):
        raise ValueError('Inventory is missing column: filepath')
    # get the desired transcription files
    df = vendor_inventory_df.loc[vendor_inventory_df['file_type'] == ttype]
    # drop filepath if desired
    if ((path == False) and 
        ('filepath' in df.columns) == True):
        df = df.drop('filepath', axis=1)
    return df

def generate_transcription_report(transcription_df):
    """
    Generated a report based upon an inventory of vendor transcription files

    Parameter
    ---------
    transcription_df : DataFrame

    Raises
    ------
    KeyError
        Missing required field in transcription DataFrame
   
    Return
    ------
    DataFrame

    """
    # check for empty inventory
    if (transcription_df.empty == True):
        return pd.DataFrame()

    # check for required fields
    if ((not 'drs_id' in transcription_df.columns) or
        (not 'filename' in transcription_df.columns)):
        raise KeyError('Missing required field in transcription DataFrame')

    # process data
    # TO DO: investigate DataFrame.groupby() options for better performance
    data = {}
    for row in transcription_df.iterrows():
        index = row[0]
        drs_id = row[1].get('drs_id')
        filename = row[1].get('filename')
        if (not data.get(drs_id)):
            data[drs_id] = {}
            data[drs_id] = {'drs_id':drs_id, 'filename':[filename], 'count':1}
        else:
            data[drs_id]['filename'].append(filename)
            data[drs_id]['count'] = data[drs_id]['count'] + 1
    # serialize filenames
    for key in data.keys():
        names = ';'.join(data[key].get('filename'))
        data[key]['filename']=names
    # generate report dataframe
    df = pd.DataFrame.from_records(list(data.values()))
    return df

def find_missing_transcription_drs_ids(do_inventory_df, transcription_report_df):
    """
    Generated a report based upon an inventory of vendor transcription files

    Parameter
    ---------
    do_inventory_df : DataFrame
    transcription_report_df : DataFrame

    Raises
    ------
    KeyError
        Missing required field: drs_id in DataFrame
   
    Return
    ------
    DataFrame
    """

    # check for empty inventories
    if (do_inventory_df.empty == True):
        return pd.DataFrame()
    if (transcription_report_df.empty == True):
        return pd.DataFrame()

    # check for drs_id field
    if (not('drs_id' in do_inventory_df.columns) or
        not('drs_id' in transcription_report_df.columns)):
        raise KeyError('Missing required field: drs_id in DataFrame')

    # merge the results on drs_id column
    df = do_inventory_df.merge(transcription_report_df, on='drs_id',how='left')

    # handle NaN values
    df['count'] = df['count'].fillna(0)
    df['filename'] = df['filename'].fillna('')

    return df


# end file