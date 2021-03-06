'''
Functions to generate a Pandas dataframe from Ordnance Survey 'Open Codepoint' postcodes data. 

The entry point for external use is:

    generateDataFrameFromSourceData

References:

https://www.ordnancesurvey.co.uk/business-government/tools-support/code-point-open-support
https://en.wikipedia.org/wiki/List_of_postcode_districts_in_the_United_Kingdom
https://en.wikipedia.org/wiki/List_of_postcode_areas_in_the_United_Kingdom
https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-november-2019

'''

import os
import sys
import zipfile

import pandas as pd

# NB Also need to have done a 'pip install xlrd' for pd.read_excel calls to work.

#############################################################################################

def generateDataFrameFromSourceData(dataDir, tmpDir, verbose=False) :
    ''' Top-level function to generate a dataframe of Postcode data from raw data files, returning the dataframe.
        Returns an empty dataframe if a problem is detected during processing.

        dataDir is the location which should hold the following datafiles:
        - codepo_gb.zip                      
          - the zipped set of files provided by the OS
          - as well as postcode data files, this also includes a codelist.xlsx spreadsheet which contains 
            mappings to convert location codes to country/district/ward names.
          - available via https://www.ordnancesurvey.co.uk/opendatadownload/products.html
        - postcode_district_area_lists.xls   
          - an Excel file converting Postcode area labels to 'Post Towns'
          - from https://www.postcodeaddressfile.co.uk/downloads/html_pages/download_postcode_areas_districts.htm

        tmpDir is a location which can be used to unpack the zip file                                        
    '''

    print(f'Generating postcode DataFrame from data files in {dataDir}..')

    startTiming()
    dfEmpty = pd.DataFrame()        # Returned if we detected a problem

    OSZipFile = dataDir + '/codepo_gb.zip'
    postcodeAreasFile = dataDir + '/postcode_district_area_lists.xls'
    # The codelist file is produced when the zip file is extracted under the temp dir
    codelistFile = tmpDir + '/Doc/codelist.xlsx'

    print(f'- preparing files ..')
    success = prepareFiles(OSZipFile, postcodeAreasFile, codelistFile, tmpDir, verbose)
    if not success :
        return dfEmpty
    showTiming()

    print(f'- loading code lookup data ..')
    success, dictLookupdf = loadLookups(postcodeAreasFile, codelistFile, verbose)
    if not success :
        return dfEmpty
    showTiming()

    print(f'- loading raw postcode data files ..')
    df = loadFilesIntoDataFrame(tmpDir, verbose)
    if df.empty :
        return dfEmpty
    showTiming()

    print(f'- adding code lookups ..')
    df = addCodeLookupColumns(df, dictLookupdf, verbose) 
    if df.empty :
        return dfEmpty
    showTiming()

    print(f'- deriving postcode components ..')
    df = addPostCodeBreakdown(df, verbose=verbose)
    if df.empty :
        return dfEmpty
    showTiming()

    # Converting dimension-type columns to Pandas categoricals makes the overall dataframe size significantly smaller
    # (and hence significantly quicker to reload from saved file when plotting from a cache). We can save a few seconds
    # of generation time if we do this piecemeal as we go along, but it's messier and Pandas merge operations seem to lose 
    # the categorical status of columns (?).
    print('- converting dimension columns to categoricals ..')
    catCols = [ 'Postcode_area', 'Quality', 'Country_code', 'Admin_county_code', 'Admin_district_code', 'Admin_ward_code', 
                'Post Town', 'Country Name', 'County Name', 'District Name', 'Ward Name', 
                'Pattern', 'Outward', 'District', 'Inward']
    # Check these columns all exist
    existingCols = set(catCols).intersection(set(df.columns))
    if len(existingCols) != len(catCols) :
        print(f'*** Column(s) missing from expected structure - see earlier failures')
        print(f'*** missing : {set(catCols) - set(df.columns)}')
        return dfEmpty

    df[catCols] = df[catCols].astype('category')
    showTiming()

    print()
    print('Postcode DataFrame generation from source data files finished.')
    showTiming(final=True)

    return df

#############################################################################################

# Simple timing utilties, to show how many seconds have elapsed since the start of the process
# for generating the dataframe

_t1 = None
def startTiming() :
    global _t1
    _t1 = pd.Timestamp.now()

def showTiming(final=False) :
    global _t1
    timeSoFar = pd.Timestamp.now() - _t1
    taken = timeSoFar.total_seconds()
    print(f'- time taken{"" if final else " so far"}: {taken:.2f} seconds')

#############################################################################################

def prepareFiles(OSZipFile, postcodeAreasFile, codelistFile, tmpDir, verbose=False) :
    ''' Checks that source data files exist, and unzips the main OS data file into the tmp directory.
        Returns True/False to indicate success/failure.
    '''

    if not unpackOSZipFile(OSZipFile, tmpDir, verbose) :
        return False

    if not os.path.isfile(OSZipFile) :
        print(f'*** No OS zip file found: {OSZipFile}')
        return False

    if not os.path.isfile(postcodeAreasFile) :
        print(f'*** No postcode areas spreadsheet file found: {postcodeAreasFile}')
        return False

    if not os.path.isfile(codelistFile) :
        print(f'*** No codes list spreadsheet file found: {codelistFile}')
        return False

    return True

def unpackOSZipFile(OSZipFile, tmpDir, verbose=False) :
    '''Unzips the OS data file under a temporary directory. Checks basic sub-directories are as expected.
        Returns True/False to indicate success/failure.
    '''

    # We use the zipfile package to process the OS data file.
    z = zipfile.ZipFile(OSZipFile, mode='r')

    # Look at the zipfile directory listing for files. We expect them all to exist in one of two folders,
    # Data and Doc. [NB this protects from untrusted zip files with absolute file locations in it.]
    for zinfo in z.infolist() :
        if zinfo.filename.startswith('Data/') or zinfo.filename.startswith('Doc/') :
            # Expected
            if verbose: 
                print(f'.. zipfile contains: {zinfo.filename}')
        else :
            print(f'*** Unexpected directory used within zip file: {zinfo.filename}')
            return False

    print(f'.. extracting zip file {OSZipFile} under {tmpDir} ...')

    # NB No error code is returned by the zipfile module if there is a problem unzipping, instead
    # an exception is thrown which we allow to propagate. The zip file extract will overwrite
    # an existing files in the same location with the same name.
    z.extractall(path=tmpDir)
    z.close()

    return True

#############################################################################################

def loadLookups(postcodeAreasFile, codelistFile, verbose=True) :
    '''Function controlling high-level loading of various lookup data into dataframes. 
       Returns two values: True/False to indicate Success/Failure and a dictionary of 
       the dataframes that have been loaded.
    '''
    status = True
    dictLookupdf = {}

    # Postcode area to Postcode town mappings come from their own spreadsheet
    dfAreas = loadPostcodeAreasFile(postcodeAreasFile)
    dictLookupdf['Areas'] = dfAreas
    if dfAreas.empty :
        status = False

    # Country codes are defined locally in this module.
    dfCountries = loadCountryCodes()
    dictLookupdf['Countries'] = dfCountries
    if dfCountries.empty :
        status = False

    # County, district and ward codes come from the same spreadsheet
    dfCounties = loadCountyCodes(codelistFile)
    dictLookupdf['Counties'] = dfCounties
    if dfCounties.empty :
        status = False

    dfDistricts = loadDistrictCodes(codelistFile)
    dictLookupdf['Districts'] = dfDistricts
    if dfDistricts.empty :
        status = False

    dfWards = loadWardCodes(codelistFile)
    dictLookupdf['Wards'] = dfWards
    if dfWards.empty :
        status = False

    return status, dictLookupdf

#############################################################################################

# The post code areas spreadsheet file contains a sheet which lists postcode areas and their
# corresponding post town.
#
# Postcode Area	   Post Town
# AB               Aberdeen
# AL               St. Albans
# B                Birmingham
# BA	           Bath
# ...
# Read these into a dataframe of two columns.

def loadPostcodeAreasFile(postcodeAreasFile) :
    '''Reads the post code areas spreadsheet and returns a dataframe mapping area codes to Post town names.
       Returns an empty dataframe if the file format is not as expected.
    '''

    # Read the relevant sheet from the spreadsheet. Use the first row for column headings.
    dfAreas = pd.read_excel(postcodeAreasFile, sheet_name='Postcode Areas', header=0)
    print(f'.. found {dfAreas.shape[0]} postcode areas in the Postcode Areas spreadsheet')

    # Check the column headings are what we expect:
    expectedColumnNames = ['Postcode Area', 'Post Town']
    for n, name in enumerate(expectedColumnNames) :
        if dfAreas.columns[n] != name :
            print(f'*** Unexpected column heading "{dfAreas.columns[n]}" for the Postcode Areas file - expected "{name}"')
            return pd.DataFrame()

    # Check we have a good primary key
    if not checkPrimaryKey('Postcode areas dataframe', dfAreas, 'Postcode Area') :
        return pd.DataFrame()
    print(f'.. checked postcode areas primary key')

    return dfAreas

# For some reason, the code mapping spreadsheet provided by the OS includes ONS County/District/Borough/Ward mappings, but
# nothing for top-level ONS UK country codes. So for simplicity just hard-code them here.
def loadCountryCodes() :
    '''Returns a dataframe mapping ONS UK country codes to country names.'''

    countryCodeDict = {
        'E92000001' : 'England',
        'S92000003' : 'Scotland',
        'W92000004' : 'Wales',
        'N92000002' : 'N. Ireland'
    }

    dfCountries = pd.DataFrame({ 
                                    'Country Code' : list(countryCodeDict.keys()), 
                                    'Country Name' : list(countryCodeDict.values()) 
                                })
    return dfCountries

def loadCountyCodes(codelistFile) :
    '''Reads the OS Code list spreadsheet and returns a dataframe mapping county codes to county names.'''

    # Read the relevant sheet from the spreadsheet, specifying column names as there are none in the sheet.
    dfCountyCodes = pd.read_excel(codelistFile, sheet_name='CTY', header=None, names=['County Name', 'County Code'])
    
    print(f'.. found {dfCountyCodes.shape[0]} county codes in the code list spreadsheet')

    # Remove the word 'County' from the end of the county name, e.g. 'Essex  County' => 'Essex'
    dfCountyCodes['County Name'] = dfCountyCodes['County Name'].str.strip().str.replace(' County', '').str.strip()

    # Check we have a good primary key
    if not checkPrimaryKey('County codes dataframe', dfCountyCodes, 'County Code') :
        return pd.DataFrame()
    print(f'.. checked county codes primary key')

    return dfCountyCodes

def _adjustLBO(s) :
    '''Helper function invoked by Pandas '.apply()' to adjust the names of London Boroughs.'''
    if s.endswith('London Boro') :
        return 'London Borough of ' + s.replace(' London Boro', '').strip()
    elif 'City of London' in s :
        return 'City of London'
    else :
        return s.strip()

def loadDistrictCodes(codelistFile) :
    '''Reads the OS Code List spreadsheet and returns a dataframe mapping district codes to district names.'''

    # Examination of the post code data files has shown that four types of district code combine to populate the 
    # 'admin_district_code' field of the detailed post code data:
    # - District
    # - Metropolitan District
    # - Unitary Authority
    # - London Borough
    # Each district type has its own sheet in the Code List spreadsheet

    dfList = []
    for districtType in ['DIS', 'MTD', 'UTA', 'LBO'] :
        df = pd.read_excel(codelistFile, sheet_name=districtType, header=None, names=['District Name', 'District Code'])
        dfList.append(df)

        print(f'.. found {df.shape[0]} {districtType} district codes in the Code List spreadsheet')

        # Change 'London Boro' to 'London Borough' at the end of the district name, where relevant.
        # Use apply rather than a bulk-column operation as not all rows need the change, and 
        # making the conditional within the bulk processing is trickier. 
        if districtType == 'LBO' :
            df['District Name'] = df['District Name'].str.strip().apply(_adjustLBO)

    # Join the separate dataframes into one large one.
    dfDistrictCodes = pd.concat(dfList, ignore_index=True)
    print(f'.. found {dfDistrictCodes.shape[0]} combined district codes in the Code List spreadsheet')

    # Check we have a good primary key
    if not checkPrimaryKey('District codes dataframe', dfDistrictCodes, 'District Code') :
        return pd.DataFrame()
    print(f'.. checked district codes primary key')

    return dfDistrictCodes

def loadWardCodes(codelistFile) :
    '''Reads the OS Code List spreadsheet and returns a dataframe mapping ward codes to ward names.'''

    # Examination of the post code data files has shown that five types of ward code combine to populate the 
    # 'admin_ward_code' field of the detailed post code data:
    # - Unitary Authority Ward
    # - Unitary Authority Electoral Division
    # - District Ward
    # - London Borough Ward
    # - Metropolitan District Ward
    # Each ward type has its own sheet in the Code List spreadsheet

    dfList = []
    for wardType in ['UTW', 'UTE', 'DIW', 'LBW', 'MTW'] :
        df = pd.read_excel(codelistFile, sheet_name=wardType, header=None, names=['Ward Name', 'Ward Code'])
        dfList.append(df)

        print(f'.. found {df.shape[0]} {wardType} ward codes in the Code List spreadsheet')

    # Join the separate dataframes into one large one.
    dfWardCodes = pd.concat(dfList, ignore_index=True)
    print(f'.. found {dfWardCodes.shape[0]} combined ward codes in the Code List spreadsheet')

    # Note the spreadsheet has a few cases where a code has two names, with one of them ending (DET). E.g.
    # Tintagel ED	E05009271
    # Tintagel ED (DET)	E05009271
    # According to https://www.ordnancesurvey.co.uk/documents/product-support/tech-spec/boundary-line-technical-specification.pdf
    # this indicates a 'detached' part of the area, i.e. an exclave.
    # For our simple purposes, just delete the (DET) entries.

    # Produce a Series of True/False values per ward code, indexed in the same way as the main wards dataframe
    dfDET = dfWardCodes['Ward Name'].str.strip().str.endswith('(DET)')  
    if dfDET.sum() > 0 :
        print(f'   .. deleting records for {dfDET.sum()} ward names ending in "(DET)"')
        dfWardCodes.drop(dfWardCodes[dfDET].index, inplace=True)
        print(f'  .. leaving {dfWardCodes.shape[0]} combined ward codes')

    # Check we have a good primary key
    if not checkPrimaryKey('Ward codes dataframe', dfWardCodes, 'Ward Code') :
        return pd.DataFrame()
    print(f'.. checked ward codes primary key')

    return dfWardCodes

#############################################################################################

# Functions involved in initial loading of the main postcode CSV data files into a dataframe.

# -------------------------------------------------------------------------------------------
# Column-naming -related data used to read/generate the initial dataframe from CSV files.

# The Column header data we expect in the Column headers file. The 'Names2' data is what we
# use to read in the CSV data initially. (Names1 column names  are checked but not used.)
columnHeaderNames1 = ['PC', 'PQ', 'EA', 'NO', 'CY', 'RH', 'LH', 'CC', 'DC', 'WC']
columnHeaderNames2 = [ 'Postcode', 
                       'Positional_quality_indicator', 
                       'Eastings', 'Northings', 'Country_code', 
                       'NHS_regional_HA_code', 'NHS_HA_code', 
                       'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']

# Define which of the above column names from line 2 we want to rename in the final dataframe 
renamedColumns = {'Positional_quality_indicator' :'Quality'}

# Define columns names and order for the final dataframe. NB we've dropped the NHS ones
outputColumnNames = [ 'Postcode', 
                      'Postcode_area',     # Derived from the file name.
                      'Quality',           # Renamed from its original name
                      'Eastings', 'Northings', 'Country_code', 
                      # NB NHS code columns have been discarded.
                      'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']
# -------------------------------------------------------------------------------------------

def loadFilesIntoDataFrame(tmpDir, verbose=False) :
    '''Combines the individual data CSV files for each postcode area into a single dataframe. Returns the dataframe,
       or any empty dataframe if there is an error.'''

    dfEmpty = pd.DataFrame()        # Returned if we detected a problem

    # Location within the OS Zip file structure of the two items of interest here:
    # - the directory which holds the detailed CSV data files 
    # - the Column Headers file which lists the columns present in the CSV files
    CSVDataDir = 'Data/CSV/'
    columnHeadersFile = 'Doc/Code-Point_Open_Column_Headers.csv'

    # At this point in processing, the ZIP file has already been unzipped under the tmp directory, so
    # we can generate the full directory paths for these two items.
    CSVDataDirPath = tmpDir + '/' + CSVDataDir
    columnHeadersFilePath = tmpDir + '/' + columnHeadersFile

    # First of all, check the Column Headers file provided exists and specifies the columns we expect.
    if not os.path.isfile(columnHeadersFilePath) :
        print(f'*** No CSV column headers definition file found at {columnHeadersFilePath}')
        return dfEmpty
        
    # We expect the values below in these lines
    if not checkColumnHeadersFile(columnHeadersFilePath, verbose) :
        # Column names not as expected.
        print(f'*** Problem with column headers definition file {columnHeadersFilePath}.')
        return dfEmpty

    # Check the CSV data directory exists.
    if not os.path.isdir(CSVDataDirPath) :
        print(f'*** No {CSVDataDir} CSV data directory found under: {tmpDir}')
        return dfEmpty

    # Now process the individual CSV data files.
    df = loadCSVDataFiles(CSVDataDirPath, verbose)
    if df.empty :
        return dfEmpty

    if not checkPrimaryKey('Main postcodes dataframe', df, 'Postcode') :
        return dfEmpty
    print(f'.. checked postcodes primary key')

    return df

def checkColumnHeadersFile(columnHeadersFilePath, verbose=False) :
    '''Check that the file defining the Column headers in the CSV data files contains what we expect. Returns True/False.'''

    headersOK = False

    # Read the headers file. It's a CSV file, we read the first two lines and compare the Header names in the cells 
    # to what we expect to find. If there's a difference, subsequent code may not work properly as the format of the 
    # main CSV data files will have changed.
    with open(columnHeadersFilePath, 'r', ) as f:
        line1 = f.readline().strip()
        line1ValuesList = line1.split(sep=',')
        line2 = f.readline().strip()
        line2ValuesList = line2.split(sep=',')
        if not compareListsOfStrings(columnHeaderNames1, line1ValuesList) :
            headersOK = False
            print(f'*** column headers definition file line 1 not as expected: {line1}')
        elif not compareListsOfStrings(columnHeaderNames2, line2ValuesList) :
            headersOK = False
            print(f'*** column headers definition file line 2 not as expected: {line2}')
        else :
            headersOK = True
            print(f'.. column headers definition file has the expected columns ...')

    return headersOK

def compareListsOfStrings(l1, l2) :
    '''Utility to check whether two lists of strings contain the same items in the same order, ignoring leading/trailing whitespace.'''
    for i in range(len(l1)) :
        if l1[i].strip() != l2[i].strip() : return False
    return True

def loadCSVDataFiles(CSVDataDirPath, verbose=False):
    '''Read the postcode CSV data files in the specified directory, and return a combined dataframe of their data.
       Returns an empty dataframe if their is a problem.'''

    dfEmpty = pd.DataFrame()        # Returned if we detected a problem

    # Make a list of all the CSV files in the directory.
    matchingFilenames = [entry.name for entry in os.scandir(CSVDataDirPath) if entry.is_file() and entry.name.endswith('.csv')]

    print(f'.. found {len(matchingFilenames)} postcode data CSV files to process ...')

    # Load the data for each CSV file in turn.
    # We produce a separate dataframe, converted to the set of desired output column names, for each CSV file, and record these
    # dataframes in a list.

    totalPostcodes = 0
    listOfDataframes = []

    for (fileCount, filename) in enumerate(matchingFilenames, start=1) :
        fullFilename = CSVDataDirPath + '/' + filename
        # We expect one CSV file for each postcode area, with the CSV file for area XX called filled called xx.csv (in lower case).
        postcodeArea = filename.replace('.csv', '').upper()

        # Read the CSV file into a dataframe, using the column header names from the specified list..
        # .. then rename certain columns as specified in a dictionary
        # .. the add a column called Postcode
        # .. and output just the columns we're interested in, in the order we want them.
        # Note that this results in the row index being a numeric range 0-numrows-1
        df = pd.read_csv(fullFilename, header=None, names=columnHeaderNames2)   \
                .rename(columns=renamedColumns) \
                .assign(Postcode_area=postcodeArea)[outputColumnNames]
        
        (numrows, numcols) = df.shape
        if numcols != len(outputColumnNames) :

            print(f'*** Unexpected number of columns ({numcols}) in CSV file {filename}')
            print(df.head())
            return dfEmpty

        totalPostcodes += numrows
        listOfDataframes.append(df)
        if fileCount % 10 == 0 : print(f'   ..{fileCount:3d} files : {filename:>6.6s} {postcodeArea:>2.2s}: {numrows:5d} postcodes : {totalPostcodes:7d} total ..')

    # Produce a combined dataframe by concatenating all the individual dataframes. Ignore the existing indexes, and so
    # regenerate the numeric range index from scratch (0-numrows-1)
    dfCombined = pd.concat(listOfDataframes, ignore_index=True)
    print(f'.. found {dfCombined.shape[0]} postcodes in {len(matchingFilenames)} CSV files')

    return dfCombined

#############################################################################################

def checkPrimaryKey(context, df, pkColumn) :
    '''Check whether the primary key column of a dataframe contains any duplicates or nulls. Returns True/False.'''
        
    if not df[pkColumn].is_unique :
        print(f'*** Found duplicate "{pkColumn}" column values in {context}')
        print(df[pkColumn].value_counts())
        return False

    # Check for any null postcodes. isnull() returns an array of booleans, which should all be False.
    nullCount = df[pkColumn].isnull().sum()
    if nullCount != 0 :
        print(f'*** Found {nullCount} null "{pkColumn}" column values in {context} .')
        return False

    return True

#############################################################################################

def addCodeLookupColumns(df, dictLookupdf, verbose=False) :
    '''High level control of the addition of columns containing values looked up using the codes
       already in the table. Returns a dataframe with the additional columns.'''

    dfDenormalised = df.copy()

    # Resolve each type of code in turn, by joining to the relevant 'lookup' dataframe to add a new column to 
    # the main dataframe. Performs some referential integrity checks as part of this.
    #
    # In each case we pass in a tuple of detailed parameters, just to aid readability. Each tuple 
    # contains:
    # - dataframe containing lookup values
    # - name of the code column to resolve in the main dataframe
    # - name of the code column in the lookup dataframe
    # - name of the value column in the lookup dataframe
    # - how many code values to print out in verbose mode

    areasParameters =     (dictLookupdf['Areas'], 'Postcode_area', 'Postcode Area', 'Post Town', 150)
    countriesParameters = (dictLookupdf['Countries'], 'Country_code', 'Country Code', 'Country Name', 4)
    countiesParameters =  (dictLookupdf['Counties'], 'Admin_county_code', 'County Code', 'County Name', 50)
    districtsParameters = (dictLookupdf['Districts'], 'Admin_district_code', 'District Code', 'District Name', 20)
    wardsParameters =     (dictLookupdf['Wards'], 'Admin_ward_code', 'Ward Code', 'Ward Name', 20)

    for parameters in [ areasParameters, countriesParameters, countiesParameters, districtsParameters, wardsParameters ] :
        dfDenormalised = resolveLookupCodes(dfDenormalised, *parameters, verbose=verbose)

    return dfDenormalised

def resolveLookupCodes(df, dfLookup, mainDataFrameCodeJoinColumn, 
                          lookupTableCodeColumn, lookupTableValueColumn, 
                          reportCodeUsage=10, verbose=False) :
    '''Performs the detailed processing of adding a 'value' column for a specified code column, using a lookup dataframe.
       Also does some referential integrity checking and null-value checking. Returns the dataframe with the additional
       'values' column.'''

    print(f'  .. checking "{mainDataFrameCodeJoinColumn}" column lookups ..')
    indent = ' ' * 5        # For formatting progress messages.

    # First, see if there are unreferenced values in the lookup table - not necessarily a problem, just for interest.
    # Right-outer-join the main table and lookup table to find unused domain values in the lookup data. 
    dfRightOuterJoin = pd.merge(df, dfLookup, left_on=mainDataFrameCodeJoinColumn, right_on=lookupTableCodeColumn, how='right')

    reportOnUnusedLookups(dfRightOuterJoin, mainDataFrameCodeJoinColumn, lookupTableCodeColumn, lookupTableValueColumn, 
                          indent, verbose)

    # Now do the main join, the same as above but Left-outer-join instead of Right-. If there are code in the 
    # main data which don't have a value in the lookup table, these will generate rows where the lookup code
    # column is null and the main table code column is not null.
    dfJoin = pd.merge(df, dfLookup, left_on=mainDataFrameCodeJoinColumn, right_on=lookupTableCodeColumn, how='left')

    reportOnReferentialIntegrity(dfJoin, mainDataFrameCodeJoinColumn, lookupTableCodeColumn, indent, verbose)
    reportOnNullCodes(dfJoin, mainDataFrameCodeJoinColumn, indent) 

    # The Pandas merge operation results in the 'code' column from both dataframes appearing in the merged dataframe. We
    # drop the 'code' column from the lookup table.
    dfJoin.drop(columns=[lookupTableCodeColumn], inplace=True)

    return dfJoin

def reportOnUnusedLookups(dfRightOuterJoin, mainDataFrameCodeJoinColumn, lookupTableCodeColumn, lookupTableValueColumn, 
                            indent='', verbose=False) :
    '''Report on codes in the lookup table that are not referenced in the main data table.'''

    # The dataframe passed in has been produced using an outer join so that for each unused code in the lookup table, there will
    # be a row in the joined dataframe which have null values for the columns generated from the main dataframe.
    dfUnusedLookups = dfRightOuterJoin[ dfRightOuterJoin[mainDataFrameCodeJoinColumn].isnull() ][[lookupTableCodeColumn, lookupTableValueColumn]]
    unusedLookupsCount = dfUnusedLookups.shape[0]
    if unusedLookupsCount == 0 :
        print(f'{indent}.. all values in the lookup table are referenced in the {mainDataFrameCodeJoinColumn} column ..')
    else :
        print(f'{indent}.. {unusedLookupsCount} '
                    f'{"code in the lookup table is" if unusedLookupsCount == 1 else "codes in the lookup table are"} '
                    f'not referenced in the {mainDataFrameCodeJoinColumn} column ..')        
        if verbose :
            # List out the unused values. NB need to access tuple fields by number, as column names have spaces in.
            for row in dfUnusedLookups.itertuples(index=False) :
                print(f'{indent}  - {row[0]} : {row[1]}')


def reportOnReferentialIntegrity(dfJoin, mainDataFrameCodeJoinColumn, lookupTableCodeColumn, indent='', verbose=False) :
    '''Report on cases where lookup of a code in a lookup table failed.'''
    
    dfRIFailed = dfJoin[lookupTableCodeColumn].isnull() & dfJoin[mainDataFrameCodeJoinColumn].notnull()
    dfLookupNotFound = dfJoin[ dfRIFailed ] [['Postcode', mainDataFrameCodeJoinColumn]]

    # Report on referential integrity issues
    lookupsNotFoundCount = dfLookupNotFound.shape[0]
    if lookupsNotFoundCount == 0 :
        print(f'{indent}.. all codes in the {mainDataFrameCodeJoinColumn} column exist in the lookup table')
    else :
        print(f'{indent}*** some codes in the {mainDataFrameCodeJoinColumn} column do not exist in the lookup table ...')

        # Provide some info on how many codes don't match, broken down by code.
        serNonMatchSummary = dfLookupNotFound[mainDataFrameCodeJoinColumn].value_counts()
        for code, count in serNonMatchSummary.iteritems() :
            print(f'{indent}  *** code = {code} : {count} cases')

def reportOnNullCodes(dfJoin, mainDataFrameCodeJoinColumn, indent='', verbose=False) :
    '''Report on nulls in a code column used to drive lookups.'''

    # Report on how many codes are null (and so won't have joined to the lookup table). If the data indicates
    # that the postcode location quality is low, then we don't expect location coding to be present. So we have
    # various messages to show depending on the number of nulls and whether they are related to location quality.
    dfNullValues = dfJoin[ dfJoin[mainDataFrameCodeJoinColumn].isnull() ] [['Postcode', 'Postcode_area', 'Quality']]
    nullValuesCount = dfNullValues.shape[0]
    if nullValuesCount == 0 :
        print(f'{indent}.. all codes in the {mainDataFrameCodeJoinColumn} column are non-null')
    else :
        # How many of the nulls relate to postcodes which don't have location data, as indicated by the Quality field ?
        locatedButNullCount = (~dfNullValues.Quality.isin([60,90])).sum()
        notLocatedCount = nullValuesCount - locatedButNullCount
        print(f'{indent}.. {notLocatedCount} codes in the {mainDataFrameCodeJoinColumn} column are null '
                f'but have known location quality issues')
        if locatedButNullCount > 0 :
            print(f'{indent}++ {locatedButNullCount} other codes in the {mainDataFrameCodeJoinColumn} column are null')

#############################################################################################

def addPostCodeBreakdown(df, verbose=False) :
    '''Breakdown the postcode values in the dataframe into constituent parts and adds them as new columns to 
       the dataframe. Returns the modified dataframe, or an empty dataframe if there is an unexpected postcode pattern.'''

    dfBreakdown = df[['Postcode', 'Postcode_area']].copy()

    # First check that the postcode patterns present are the ones we expected.
    # In every case, the 'Inward' part consists of the last three characters (9XX), with the 'Outward' part
    # being the part before this.
    print(f'  .. determining patterns ..')
    dfBreakdown['Pattern']  = dfBreakdown['Postcode'].str.strip().str.replace(r'[0-9]', '9').str.replace(r'[A-Z]', 'X')
    expectedPatterns = ['X9  9XX',
                        'X99 9XX',
                        'X9X 9XX',
                        'XX9 9XX',
                        'XX999XX',
                        'XX9X9XX']
    dfBadPattern = dfBreakdown [ dfBreakdown['Pattern'].isin(expectedPatterns) == False ]
    if dfBadPattern.shape[0] != 0 :
        print('** Unexpected pattern found in postcodes')
        print(dfBadPattern)
        return pd.DataFrame()

    # Split the postcode into Inward, Outward, and splot the Outward into Area and District.
    # NB Some Districts can include letters, e.g. E1W.
    print(f'  .. producing Inward ..')
    dfBreakdown['Inward']   = dfBreakdown['Postcode'].str[-3:].str.strip()
    print(f'  .. producing Outward ..')
    dfBreakdown['Outward']  = dfBreakdown['Postcode'].str[0:-3].str.strip()
    print(f'  .. producing Area and District ..')
    dfBreakdown[['Area', 'District']]  = dfBreakdown['Outward'].str.extract(r'([A-Z][A-Z]?)([0-9].*)')

    print(f'  .. validating results ..')
    dfBadArea = dfBreakdown [ dfBreakdown.Area != dfBreakdown.Postcode_area ]
    if dfBadArea.shape[0] != 0 :
        print('*** Bad area extraction from postcode')
        print(dfBadArea)
        return pd.DataFrame()

    dfBadDistrict = dfBreakdown[ dfBreakdown.Area + dfBreakdown.District != dfBreakdown.Outward ]
    if dfBadDistrict.shape[0] != 0 :
        print('*** Bad district extraction from postcode')
        print(dfBadDistrict)
        return pd.DataFrame()

    df[['Inward', 'Outward', 'District', 'Pattern']] = dfBreakdown[['Inward', 'Outward', 'District', 'Pattern']]

    return df

#############################################################################################

# Main function for dev/test only. Expect to be invoked from main postcodes.py module normally,
# allowing more flexible choice of locations via command line arguments, and caching resulting dataframe.

if __name__ == '__main__' :

    dataDir = './OSData/Postcodes'      # Where the source data files are
    tmpDir = dataDir + '/tmp2'          # Working area, for unzipping and caching
    verbose = False

    df = generateDataFrameFromSourceData(dataDir, tmpDir, verbose)
    if df.empty :
        print()
        print('*** No dataframe generated from source data.')
    else :
        print()
        print(df)
