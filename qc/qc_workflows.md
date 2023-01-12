# Historic Datasets Quality Control (QC) Workflow
## About
This document describes the high level workflow tasks needed to perform quality control on vendor-created content for the Historic Datasets Pilot Project.

## Workflow

### Metadata Analysis
This part of the workflow focuses on analyzing metadata about source image files and files that have been created by the vendor

- **Download and Process IIIF Manifest (`JSON` format)**
  - Note: File contains the original manifest of images and OCR text associated with a digital object whose tabular data contents will be transcribed. Manifest can be downloaded directly from the digital object (e.g., see download option at https://iiif.lib.harvard.edu/manifests/view/drs:44319007$1i)
  - Generate `DataFrame` of metadata about files associated with the digital object
    - Use: `util.iiif_to_dataframe(iiif_manifest.json)`
- **Download and Process METS File (`XML` format)**
  - Note: File is generated by the vendor and contains the full list of all files associated with the digital object and its tabular data and text transcriptions. Formats include .txt, .jpg, and .csv. Note also that one image may contain multiple transcribeda tables or annotations. 
  - Generate `DataFrame` of all files that were originally part of the digital object _and_ were produced by the vendor
    - Use: `util.mets_to_dataframe(mets.xml)`
- **Create Digital Object and Vendor File Inventories**
  - Create inventory of files associated with the original digital object
    - Process the original IIIF manifest `DataFrame` so it can be compared to the vendor inventory more easily
    - Use: `util.create_digital_object_inventory(DataFrame)`
  - Create inventory of files created by the vendor 
    - Process the METS file `DataFrame` so that it can be compared to the IIIF manifest `DataFrame` more easily. Include local file paths and test for existence of local file, if desired 
    - Use: `util.create_vendor_inventory(DataFrame)`
- **Compare Digital Object Files to Vendor Files**
  - Note: For each entry in the digital object, look for and record the corresponding vendor file/s, if any

### Data Analysis
- This part of the workflow focuses on analyzing the contents of the datafiles (e.g., images, csv files, and text files) that have been generated by the vendor