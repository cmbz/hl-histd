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
        # if there are no more files
        if (not files):
            break
        # otherwise, iterate over the files in the group
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

def create_digital_object_inventory(inventory_df, itype='iiif'):
    """
    Given a DataFrame of digital object information 
    (either retrieved from 'iiif_to_dataframe' or 'drs_inventory_to_dataframe),
    process the metadata and return a DataFrame of information about the files
    that can be easily compared to other file inventories.

    Parameter
    ---------
    inventory_df : DataFrame
        Output of call to 'iiif_to_dataframe' or 'drs_inventory_to_dataframe'
    itype : str
        Type of inventory, either 'iiif' or 'drs'

    Raise
    -----
    ValueError
        Unknown inventory type
    
    Return
    ------
    DataFrame
    """
    # check for empty dataframe
    if (inventory_df.empty == True):
        return pd.DataFrame()

    # check for valid ttype
    if (itype not in ['iiif','drs']):
        msg = 'Unknown inventory type: {}'.format(itype)
        raise ValueError(msg)

    # create a copy
    df = inventory_df.copy(deep=True)

    # inventory is from iiif file
    if (itype == 'iiif'):
        # rename columns appropriately
        df.rename(columns = {'@id':'url','format':'mimetype'},errors='raise',inplace=True)
        # create a file_type column (derived from format/mimetype)
        df['file_type'] = ''
        for row in df.iterrows():
            index = row[0]
            mimetype = row[1]['mimetype']
            file_type = mimetype.split('/')[0]
            df.at[index, 'file_type'] = file_type

    # if inventory is from drs file, there is nothing to do
    # ...

    return df

def create_vendor_inventory(mets_df, drsids=True, path=None):
    """
    Given a DataFrame of METS information (retrieved from 'mets_to_dataframe'),
    process the metadata and return a DataFrame of information about the files
    that can be easily compared to other file inventories.

    Parameters
    ----------
    mets_df : DataFrame
        Output of call to mets_to_dataframe
    drsids : bool
        Parse and output DRS ids (default, True)
        Assumes that mets_df contains DRS ids
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
    
    # if drsids are present, process them
    if (drsids == True):
        # create drs ids by removing file extension
        # assumes that the filenames are based upon the drs id
        splitnames = df['filename'].str.split('.')
        drs_ids = splitnames.apply(lambda x : x[0])
        import re
        drs_ids = drs_ids.apply(lambda x : re.sub(r'_.*$', '', x))
        df['drs_id'] = drs_ids
    
    # if drsids are not present, process filename stems
    if (drsids == False):
        import re
        stems = df['filename'].apply(lambda x : re.match(r'^[\d]+_[\S]+_[\d]+[_|.]', x))
        df['filename_stem'] = stems.apply(lambda x : re.sub(r'[_|.]$', '', x[0]))
    
    # drop unneeded columns
    df = df.drop(columns = ['@id','mets_url'])
    
    # rename columns appropriately
    df.rename(columns = {'@mimetype':'mimetype'},errors='raise',inplace=True)
    return df

def find_missing_reference_ids(do_ref_ids, vendor_ref_ids):
    """
    Given two pandas Series, one of digital object reference ids and another of
    vendor reference ids, identify digital object DRS ids that do not appear 
    in the vendor_ref_ids list

    Parameters
    ----------
    do_ref_ids : Series
        Series of digital object reference ids
    vendor_ref_ids : Series
        Series of vendor reference ids

    Raise
    -----
    ValueError
        Too many distinct vendor reference ids detected
        Vendor reference id/s not found in digital object
    
    Return
    ------
    DataFrame
        List of missing reference ids, empty if none

    """
    # check for empty series
    if (do_ref_ids.empty == True):
        return pd.DataFrame()
    if (vendor_ref_ids.empty == True):
        return pd.DataFrame()

    # digital object ref ids cannot contain duplicates
    duplicates = do_ref_ids.duplicated(keep='first')
    if (True in duplicates.values):
        raise ValueError('Digital object has one or more duplicate reference ids')

    # sort the values
    do_ref_ids = do_ref_ids.sort_values(ascending=True)
    vendor_ref_ids = vendor_ref_ids.sort_values(ascending=True)

    # drop duplicates
    v2_ref_ids = vendor_ref_ids.drop_duplicates(keep='first')

    # create sets of ref ids to perform set operations
    set1 = set(do_ref_ids.tolist())
    set2 = set(v2_ref_ids.tolist())

    # are the sets are equal?
    if (set1 == set2):
        return pd.DataFrame()

    # values in vendor must appear in digital object
    if (not (set2.issubset(set1))):
        raise ValueError('Digital object does not contain one or more vendor reference ids')
           
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

def generate_transcription_report(transcription_df, drsids=True):
    """
    Generated a report based upon an inventory of vendor transcription files

    Parameter
    ---------
    transcription_df : DataFrame
    drsids : bool (default = True)
        The inventory does/not contain DRS ids

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
    if (drsids == True): 
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
    else:
        # drsids = False
        data = {}
        for row in transcription_df.iterrows():
            index = row[0]
            stem = row[1].get('filename_stem')
            filename = row[1].get('filename')
            if (not data.get(stem)):
                data[stem] = {}
                data[stem] = {'filename_stem':stem, 'filename':[filename], 'count':1}
            else:
                data[stem]['filename'].append(filename)
                data[stem]['count'] = data[stem]['count'] + 1

    # serialize filenames
    for key in data.keys():
        names = ';'.join(data[key].get('filename'))
        data[key]['filename']=names
    # generate report dataframe
    df = pd.DataFrame.from_records(list(data.values()))
    return df

def find_missing_transcription_reference_ids(do_inventory_df, transcription_report_df, reftype='drs'):
    """
    Generated a report based upon an inventory of vendor transcription files

    Parameter
    ---------
    do_inventory_df : DataFrame
    transcription_report_df : DataFrame
    reftype : str (default: drs)
        Either 'drs' or 'stem' (filename stem)

    Raises
    ------
    ValueError
        Invalid reference type
    KeyError
        Missing required field: drs_id in DataFrame
        Missing required field: filename_stem in DataFrame
   
    Return
    ------
    DataFrame
    """

    # check for empty inventories
    if (do_inventory_df.empty == True):
        return pd.DataFrame()
    if (transcription_report_df.empty == True):
        return pd.DataFrame()

    # check for valid reftype
    if (not reftype in ['drs','stem']):
        raise KeyError('Invalid reference type')

    # reference id
    refid = None

    # check for drs_id field
    if (reftype == 'drs'):
        if (not('drs_id' in do_inventory_df.columns) or
            not('drs_id' in transcription_report_df.columns)):
            raise KeyError('Missing required field: drs_id in DataFrame')
        else:
            refid = 'drs_id'

    # check for filename_stem field
    if (reftype == 'stem'):
        if (not('filename_stem' in do_inventory_df.columns) or
            not('filename_stem' in transcription_report_df.columns)):
            raise KeyError('Missing required field: filename_stem in DataFrame')
        else:
            refid = 'filename_stem'

    # merge the results on drs_id column
    df = do_inventory_df.merge(transcription_report_df, on=refid, how='left')

    # handle NaN values
    df['count'] = df['count'].fillna(0)
    df['filename'] = df['filename'].fillna('')

    # count should be integer
    df['count'] = df['count'].astype('int64')

    return df

def drs_inventory_to_dataframe(filename):
    """
    Read and extract information about files from DRS inventory file.

    Parameter
    ---------
    drs_inventory : str
        Full path to DRS inventory filename 
   
    Return
    ------
    DataFrame
    """
    if (not filename):
        return None

    # columns to use
    cols = ['file_huldrsadmin_ownerSuppliedName_string',
            'file_mets_mimetype_string',
            'file_huldrsadmin_uri_string_sort']

    # read the inventory file
    df = pd.read_csv(filename,delimiter=',',usecols=cols)

    # rename columns
    new_names = {'file_huldrsadmin_ownerSuppliedName_string':'filename_stem',
                'file_mets_mimetype_string':'mimetype',
                'file_huldrsadmin_uri_string_sort':'url'}
    df.rename(columns=new_names,inplace=True)
    df['url'] = df.apply(lambda row: 'https://nrs.harvard.edu/' + row.url, axis=1)

    return df

def get_file_info(path):
    """
    Get information about all files in a directory

    Parameter
    ---------
    path : str
        Full path to directory

    Return
    ------
    dict 
        Dictionary of directory's file information
    """
    # check parameters
    if (not path):
        return {}

    import os
    # get the list of files in the path
    files = os.listdir(path)
    # get file info for each file
    info = {}
    for file in files:
        filename = path + '/' + file
        status = os.stat(filename)
        info[file] = {
            'name':file,
            'size':status.st_size,
            'mode':status.st_mode,
            'ctime':status.st_ctime
        }
    return info

def map_drs_vendor_inventory(vendor_inventory_df, do_osn_inventory_df):
    """
    Given a vendor inventory that uses DRS ids and an inventory 
    of matching content that uses owner-supplied names, generate an
    inventory of new names based upon owner-supplied names.

    Parameters
    ----------
    vendor_inventory_df : DataFrame
        Vendor inventory that uses DRS ids
    do_osn_inventory_df : DataFrame
        Digital object inventory that uses owner-supplied names

    Return
    ------
    DataFrame
    """
    # check for empty inventories
    if (vendor_inventory_df.empty == True):
        return pd.DataFrame()
    if (do_osn_inventory_df.empty == True):
        return pd.DataFrame()
    
    # import regex
    import re

    # create output dataframe
    df = vendor_inventory_df.copy(deep=True)
    # add a column for new file name
    df['filename_osn'] = ''

    # process each row in the digital object inventory
    for row in do_osn_inventory_df.iterrows():
        # get the drs id
        drsid = str(row[1].get('file_id_num'))
        # get the owner-supplied name
        osn = row[1].get('file_huldrsadmin_ownerSuppliedName_string')
        # get the rows corresponding to the drsid from vendor inventory
        matching = df.loc[df['drs_id'] == drsid]
        # rename the file for match
        for match in matching.iterrows():
            index = match[0]
            filename = match[1].get('filename')
            chunks = re.findall('_.*$', filename)
            # filename like: 44319541.jpg
            if (len(chunks) == 0):
                    tokens = filename.split('.')
                    filename_osn = osn + '.innodata.' + tokens[1]
                    df.at[index, 'filename_osn'] = filename_osn
            else:
                # filename like: 44319578_24-25_a.csv
                tokens = chunks[0].split('.')
                filename_osn = osn + tokens[0] + '.innodata.' + tokens[1]
                df.at[index, 'filename_osn'] = filename_osn

    return df

def rename_vendor_files(vendor_osn_inventory_df):
    """
    """
    # check for empty inventory
    if (vendor_osn_inventory_df.empty == True):
        False
    # inventory must have certain columns
    if (('filepath' not in vendor_osn_inventory_df.columns) or
        ('filename_osn' not in vendor_osn_inventory_df.columns)):
            return False

    import os
    for row in vendor_osn_inventory_df.iterrows():
        filepath = row[1].get('filepath')
        filename_osn = row[1].get('filename_osn')
        tokens = filepath.split('/')
        del tokens[-1]
        new_filename = '/'.join(tokens) + '/' + filename_osn
        try :
            os.rename(filepath, new_filename)
            print("Source path renamed to destination path successfully.")
        except IsADirectoryError:
            print("Source is a file but destination is a directory.")
        except NotADirectoryError:
            print("Source is a directory but destination is a file.")
        except PermissionError:
            print("Operation not permitted.")
        except OSError as error:
            print(error)

    return True


# end file