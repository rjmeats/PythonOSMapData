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

#############################################################################################

def generateDataFrameFromSourceData(dataDir, tmpDir, verbose=False) :
    ''' Top-level function to generate a dataframe of Postcode data from raw data files, returning the dataframe.
        Returns an empty dataframe if a problem is detected during processing.

        dataDir is the location which should hold the following datafiles:
        - codepo_gb.zip                      
          - the zipped set of files provided by the OS
          - available via https://www.ordnancesurvey.co.uk/opendatadownload/products.html
        - postcode_district_area_lists.xls   
          - an Excel file converting Postcode area labels to 'Post Towns'
          - from https://www.postcodeaddressfile.co.uk/downloads/html_pages/download_postcode_areas_districts.htm

        tmpDir is a location which can be used to unpack the zip file                                        
    '''

    t1 = pd.Timestamp.now()
    dfEmpty = pd.DataFrame()        # Returned if we detected a problem

    print(f'Generating postcode DataFrame from data files in {dataDir}..')

    OSZipFile = dataDir + "/" + "codepo_gb.zip"
    postcodeAreasFile = dataDir + "/" + "postcode_district_area_lists.xls"

    print(f'- preparing files ..')
    success = prepareFiles(OSZipFile, postcodeAreasFile, tmpDir, verbose)
    if not success :
        return dfEmpty
    timeSoFar = pd.Timestamp.now()-t1
    print(f'- time taken so far: {timeSoFar.total_seconds():.2f} seconds')

    print(f'- loading code lookup data ..')
    success, dictLookupdf = loadLookups(postcodeAreasFile, tmpDir, verbose)
    if not success :
        return dfEmpty
    timeSoFar = pd.Timestamp.now()-t1
    print(f'- time taken so far: {timeSoFar.total_seconds():.2f} seconds')

    print(f'- loading raw postcode data files ..')
    startTime = pd.Timestamp.now()
    df = loadFilesIntoDataFrame(tmpDir)
    took = pd.Timestamp.now()-startTime
    if not df.empty :
        print(f'- took {took.total_seconds()} seconds to load raw data files')
        timeSoFar = pd.Timestamp.now()-t1
        print(f'- time taken so far: {timeSoFar.total_seconds():.2f} seconds')
    else :
        return dfEmpty

    print(f'- adding code lookups ..')
    dfDenormalised = addCodeLookupColumns(df, dictLookupdf, verbose) 
    timeSoFar = pd.Timestamp.now()-t1
    print(f'- time taken so far: {timeSoFar.total_seconds():.2f} seconds')

    print(f'- deriving postcode components ..')
    dfDenormalised = addPostCodeBreakdown(dfDenormalised, verbose=verbose)
    if dfDenormalised.empty :
        return dfEmpty

    timeSoFar = pd.Timestamp.now()-t1
    print(f'- time taken so far: {timeSoFar.total_seconds():.2f} seconds')

    examineLocationColumns(dfDenormalised, verbose=verbose)

    useCategoricals = True
    if useCategoricals :
        print('Converting columns to categoricals ...')
        catCols = ['PostcodeArea', 'Quality', 'Country_code', 'Admin_county_code', 'Admin_district_code',
                    'Admin_ward_code', 'Post Town', 'Country Name', 'County Name',
                    'District Name', 'Ward Name', 'Pattern', 'Outward', 'District', 'Inward']
        dfDenormalised[catCols] = dfDenormalised[catCols].astype('category')

    took = pd.Timestamp.now()-t1
    print()
    print('Postcode DataFrame generation from source data files finished.')
    print(f'- took {took.total_seconds():.2f} seconds')

    return dfDenormalised

def prepareFiles(OSZipFile, postcodeAreasFile, tmpDir, verbose=False) :

    if not os.path.isfile(OSZipFile) :
        print("*** No OS zip file found:", OSZipFile)
        return False

    if not os.path.isfile(postcodeAreasFile) :
        print("*** No ONS postcode areas spreadsheet file found:", postcodeAreasFile)
        return False

    if not unpackZipFile(OSZipFile, tmpDir, verbose) :
        return False

    return True

def unpackZipFile(OSZipFile, tmpDir, verbose=False) :
    """ Unzips the data file under a temporary directory. Checks basic sub-directories are as expected."""
    z = zipfile.ZipFile(OSZipFile, mode='r')

    zinfolist = z.infolist()
    for zinfo in zinfolist :
        if zinfo.filename.startswith('Data/') or zinfo.filename.startswith('Doc/') :
            if verbose: 
                print(zinfo.filename)
        else :
            print("*** Unexpected extract location found:", zinfo.filename, file=sys.stderr)
            return False

    print(f'.. extracting zip file {OSZipFile} under {tmpDir} ...')
    z.extractall(path=tmpDir)
    z.close()

    return True

def loadLookups(postcodeAreasFile, tmpDir, verbose=True) :

    status = True
    dictLookupdf = {}

    dfAreas = loadPostcodeAreasFile(postcodeAreasFile)
    dictLookupdf['Areas'] = dfAreas
    if dfAreas.empty :
        status = False

    dfCountries = loadCountryCodes()
    dictLookupdf['Countries'] = dfCountries
    if dfCountries.empty :
        status = False

    dfCounties = loadCountyCodes(tmpDir)
    dictLookupdf['Counties'] = dfCounties
    if dfCounties.empty :
        status = False

    dfDistricts = loadDistrictCodes(tmpDir)
    dictLookupdf['Districts'] = dfDistricts
    if dfDistricts.empty :
        status = False

    dfWards = loadWardCodes(tmpDir)
    dictLookupdf['Wards'] = dfWards
    if dfWards.empty :
        status = False

    return status, dictLookupdf

def addCodeLookupColumns(df, dictLookupdf, verbose=False) :

    # Add further columns showing looked-up values of the various code columns, checking referential
    # itegrity and null-ness at the same time.
    dfDenormalised = df
    areasParameters = ('Postcode Area Codes', 'Postcode', 'PostcodeArea', 'Postcode Area','Post Town')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dictLookupdf['Areas'], areasParameters, 150, verbose=verbose)

    countriesParameters = ('Country Codes', 'Postcode', 'Country_code', 'Country Code', 'Country Name')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dictLookupdf['Countries'], countriesParameters, 10, verbose=verbose)

    countiesParameters = ('County Codes', 'Postcode', 'Admin_county_code', 'County Code','County Name')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dictLookupdf['Counties'], countiesParameters, 50, verbose=verbose)

    districtsParameters = ('District Codes', 'Postcode', 'Admin_district_code', 'District Code','District Name')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dictLookupdf['Districts'], districtsParameters, 20, verbose=verbose)

    wardsParameters = ('Ward Codes', 'Postcode', 'Admin_ward_code', 'Ward Code','Ward Name')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dictLookupdf['Wards'], wardsParameters, 20, verbose=verbose)

    # Prune the column list - the above will have added an extra copy of each 'code' column that we can
    # remove again. Just use the original columns in the dataframe and the main lookup columns added.
    
    fullOutputColumns = (list(df.columns)).copy()
    fullOutputColumns.extend(['Post Town', 'Country Name', 'County Name', 'District Name', 'Ward Name'])
    dfDenormalised = dfDenormalised[fullOutputColumns]

    return dfDenormalised


#############################################################################################
# Items relating to the columns in the main CSV file and resulting dataframe

# Column names are provided in a separate file, with two lines - we expect the values below in these lines
csvColumnNamesFile = 'Doc/Code-Point_Open_Column_Headers.csv'
csvColumnNames1 = ['PC', 'PQ', 'EA', 'NO', 'CY', 'RH', 'LH', 'CC', 'DC', 'WC']
csvColumnNames2 = [ 'Postcode', 
                'Positional_quality_indicator', 'Eastings', 'Northings', 'Country_code', 
                'NHS_regional_HA_code', 'NHS_HA_code', 
                'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']

# Define which of the above column names from line 2 we want to rename in the final dataframe 
renamedDFColumns = {'Positional_quality_indicator' :'Quality'}

# Define columns names and order for the final dataframe. NB we've dropped the NHS ones
outputDFColumnNames = [ 'Postcode', 'PostcodeArea', 
                'Quality', 'Eastings', 'Northings', 'Country_code', 
                'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']

# Where the main data files reside
mainCSVDataDir = 'Data/CSV/'

def listsOfStringsMatch(l1, l2) :
    """Do two lists of strings contain the same items in the same order, ignoring leading/trailing whitespace ?"""
    if(len(l1) != len(l2)) : return False
    for i in range(len(l1)) :
        if l1[i].strip() != l2[i].strip() : return False
    return True

def checkColumns(tmpDir, detail=False) :
    """ Check that the file defining column headers contains what we expect. """
    columnsOK = False
    columnsFile = tmpDir + '/' + csvColumnNamesFile
    if not os.path.isfile(columnsFile) :
        print(f"*** No columns definition file found at {columnsFile}", file=sys.stderr)
        columnsOK = False
    else :
        with open(columnsFile, 'r', ) as f:
            line1 = f.readline().strip()
            line1list = line1.split(sep=',')
            line2 = f.readline().strip()
            line2list = line2.split(sep=',')
            if not listsOfStringsMatch(csvColumnNames1, line1list) :
                columnsOK = False
                print(f"*** Columns definition file {csvColumnNamesFile} line 1 not as expected: {line1}", file=sys.stderr)
            elif not listsOfStringsMatch(csvColumnNames2, line2list) :
                columnsOK = False
                print(f"*** Columns definition file {csvColumnNamesFile} line 2 not as expected: {line2}", file=sys.stderr)
            else :
                columnsOK = True
                print(f".. columns definition file {csvColumnNamesFile} has the expected columns ...")

    return columnsOK

#############################################################################################

def loadFilesIntoDataFrame(tmpDir, detail=False) :
    """ Combines the individual data csv files for each postcode area into a single dataframe. Returns the dataframe,
    or any empty dataframe if there is an error."""

    if not checkColumns(tmpDir, detail) :
        # Column names not as expected.
        return pd.DataFrame()

    # Generate a list of csv files to process
    mainDataDir = tmpDir + '/' + mainCSVDataDir
    if not os.path.isdir(mainDataDir) :
        print(f'*** No {mainCSVDataDir} directory found under: {mainDataDir}', file=sys.stderr)
        return pd.DataFrame()
    matchingFilenames = [entry.name for entry in os.scandir(mainDataDir) if entry.is_file() and entry.name.endswith('.csv')]
    print(f'.. found {len(matchingFilenames)} CSV files to process ...')

    # Build up a list of dataframes, one per CSV file, with the set of desired output column names.
    # There is one CSV file for each postcode area, with the filled called xx.csv where xx is the postcode area (in lower case).
    totalPostcodes = 0
    dfList = []
    for (fileCount, filename) in enumerate(matchingFilenames, start=1) :
        if fileCount > 1000 : break
        fullFilename = mainDataDir + filename
        postcodeArea = filename.replace('.csv', '').upper()

        df = pd.read_csv(fullFilename, header=None, names=csvColumnNames2)   \
                .rename(columns=renamedDFColumns) \
                .assign(PostcodeArea=postcodeArea)[outputDFColumnNames]
        (numrows, numcols) = df.shape
        if numcols != len(outputDFColumnNames) :
            print()
            print(f'*** Unexpected number of columns ({numcols}) in CSV file {filename}', file=sys.stderr)
            print(df.info())
            print(df.head())
            return
        totalPostcodes += numrows
        dfList.append(df)
        if fileCount % 10 == 0 : print(f'.. {fileCount:3d} files : {filename:>6.6s} {postcodeArea:>2.2s}: {numrows:5d} postcodes : {totalPostcodes:7d} total ..')

    # Much faster to concatenate full set of collected dataframes than appending one-by-one.
    # Need to avoid copy index values in, to avoid repeats.
    combined_df = pd.concat(dfList, ignore_index=True)
    print(f'.. found {combined_df.shape[0]} postcodes in {len(matchingFilenames)} CSV files')

    # Make Postcode the index instead of the auto-assigned range. As part of this, check there are no duplicate postcodes. And then
    # go back to an auto-assigned range, as later group-bys are easier if the Postcode is present as a normal column.
    try :
        combined_df = combined_df.set_index('Postcode', verify_integrity=True).reset_index()
    except ValueError as e :
        print()
        print(f'*** Found duplicate postcodes: {e}')
        return pd.DataFrame()

    return combined_df

#############################################################################################

def loadCountryCodes() :

    countryCodeDict = {
        'E92000001' : 'England',
        'S92000003' : 'Scotland',
        'W92000004' : 'Wales',
        'N92000002' : 'N. Ireland'
    }

    dfCountries = pd.DataFrame({ 'Country Code' : list(countryCodeDict.keys()), 'Country Name' : list(countryCodeDict.values()) })
    return dfCountries

# The postcode_district_area_lists.xls file has lists of postcode areas, which should match the 'xx' part
# of the 'xx.csv' postcode data file names. The file comes from here rather than the OS:
# https://www.postcodeaddressfile.co.uk/downloads/free_products/postcode_district_area_lists.xls

def loadPostcodeAreasFile(postcodeAreasFile) :

    # NB Needed to 'pip install xlrd' for this to work.
    dfAreas = pd.read_excel(postcodeAreasFile, sheet_name='Postcode Areas', header=0)
    print(f'.. found {dfAreas.shape[0]} postcode areas in the areas spreadsheet')

    # Check the columns are what we expect:
    if dfAreas.columns[0] != 'Postcode Area' :
        print(f'*** Unexpected column heading {dfAreas.columns[0]} for postcode areas file ')
        return pd.DataFrame()

    if dfAreas.columns[1] != 'Post Town' :
        print(f'*** Unexpected column heading {dfAreas.columns[1]} for postcode areas file ')
        return pd.DataFrame()

    return dfAreas

def stripWordCounty(s) :
    return s.replace(" County", "").strip()

def loadCountyCodes(tmpDir) :

    codeslistFile = tmpDir + r"/Doc/codelist.xlsx"

    if not os.path.isfile(codeslistFile) :
        print("*** No OS code list spreadsheet file found:", codeslistFile, file=sys.stderr)
        return pd.DataFrame()

    # NB Needed to 'pip install xlrd' for this to work.
    dfCountyCodes = pd.read_excel(codeslistFile, sheet_name='CTY', header=None, names=['County Name', 'County Code'])
    
    print(f'.. found {dfCountyCodes.shape[0]} county codes in the code list spreadsheet')

    # Remove the word 'County' from the end of the name, e.g. 'Essex County' => 'Essex'
    dfCountyCodes['County Name'] = dfCountyCodes['County Name'].apply(stripWordCounty)

    return dfCountyCodes

def expandBoro(s) :
    if s.endswith('London Boro') :
        return s.replace(" London Boro", " London Borough")
    else :
        return s

def loadDistrictCodes(tmpDir) :

    codeslistFile = tmpDir + r"/Doc/codelist.xlsx"

    if not os.path.isfile(codeslistFile) :
        print("*** No OS code list spreadsheet file found:", codeslistFile, file=sys.stderr)
        return pd.DataFrame()

    # NB Needed to 'pip install xlrd' for this to work.
    dfDistrictCodes1 = pd.read_excel(codeslistFile, sheet_name='DIS', header=None, names=['District Name', 'District Code'])
    dfDistrictCodes2 = pd.read_excel(codeslistFile, sheet_name='MTD', header=None, names=['District Name', 'District Code'])
    dfDistrictCodes3 = pd.read_excel(codeslistFile, sheet_name='UTA', header=None, names=['District Name', 'District Code'])
    dfDistrictCodes4 = pd.read_excel(codeslistFile, sheet_name='LBO', header=None, names=['District Name', 'District Code'])
    
    print(f'.. found {dfDistrictCodes1.shape[0]} DIS district codes in the code list spreadsheet')
    print(f'.. found {dfDistrictCodes2.shape[0]} MTD district codes in the code list spreadsheet')
    print(f'.. found {dfDistrictCodes3.shape[0]} UTA district codes in the code list spreadsheet')
    print(f'.. found {dfDistrictCodes4.shape[0]} LBO district codes in the code list spreadsheet')

    dfDistrictCodes = pd.concat([dfDistrictCodes1, dfDistrictCodes2, dfDistrictCodes3, dfDistrictCodes4], ignore_index=True)

    print(f'.. found {dfDistrictCodes.shape[0]} combined district codes in the code list spreadsheet')

    # Change 'London Boro' to 'London Borough' at the end of the name, were relevant
    dfDistrictCodes['District Name'] = dfDistrictCodes['District Name'].apply(expandBoro)

    return dfDistrictCodes

def loadWardCodes(tmpDir) :

    codeslistFile = tmpDir + r"/Doc/codelist.xlsx"

    if not os.path.isfile(codeslistFile) :
        print("*** No OS code list spreadsheet file found:", codeslistFile, file=sys.stderr)
        return pd.DataFrame()

    # NB Needed to 'pip install xlrd' for this to work.
    dfWardCodes1 = pd.read_excel(codeslistFile, sheet_name='UTW', header=None, names=['Ward Name', 'Ward Code'])
    dfWardCodes2 = pd.read_excel(codeslistFile, sheet_name='UTE', header=None, names=['Ward Name', 'Ward Code'])
    dfWardCodes3 = pd.read_excel(codeslistFile, sheet_name='DIW', header=None, names=['Ward Name', 'Ward Code'])
    dfWardCodes4 = pd.read_excel(codeslistFile, sheet_name='LBW', header=None, names=['Ward Name', 'Ward Code'])
    dfWardCodes5 = pd.read_excel(codeslistFile, sheet_name='MTW', header=None, names=['Ward Name', 'Ward Code'])
    
    print(f'.. found {dfWardCodes1.shape[0]} UTW ward codes in the code list spreadsheet')
    print(f'.. found {dfWardCodes2.shape[0]} UTE ward codes in the code list spreadsheet')
    print(f'.. found {dfWardCodes3.shape[0]} DIW ward codes in the code list spreadsheet')
    print(f'.. found {dfWardCodes4.shape[0]} LBW ward codes in the code list spreadsheet')
    print(f'.. found {dfWardCodes5.shape[0]} MTW ward codes in the code list spreadsheet')

    dfWardCodes = pd.concat([dfWardCodes1, dfWardCodes2, dfWardCodes3, dfWardCodes4, dfWardCodes5], ignore_index=True)

    print(f'.. found {dfWardCodes.shape[0]} combined ward codes in the code list spreadsheet')

    # Note the spreadsheet has a few cases where a code has two names, with one of them ending (DET). E.g.
    # Tintagel ED	E05009271
    # Tintagel ED (DET)	E05009271
    # According to https://www.ordnancesurvey.co.uk/documents/product-support/tech-spec/boundary-line-technical-specification.pdf
    # this indicates a 'detached' part of the area, i.e. an enclave.
    # For our simple purposes, just delete the (DET) entries.

    dfDET = dfWardCodes['Ward Name'].str.endswith('(DET)')  # Column of True/False per ward code
    if dfDET.sum() > 0 :
        print(f'  .. deleting records for {dfDET.sum()} ward names ending "(DET)""')
        dfWardCodes.drop(dfWardCodes[dfDET].index, inplace=True)
        print(f'  .. leaving {dfWardCodes.shape[0]} combined ward codes')

    return dfWardCodes

def checkAllUniqueValues(context, df, columnName) :

    allUnique = False
    try :
        df.set_index(columnName, verify_integrity=True)
        allUnique = True
    except ValueError as e :
        allUnique = False
        print()
        print(f'*** Found duplicate values in {context} in column {columnName}: {e}', file=sys.stderr)

    return allUnique

def checkCodesReferentialIntegrity(df, dfLookup, parameters, reportCodeUsage=10, verbose=False) :

    (context, mainDataFramePKColumn, mainDataFrameCodeJoinColumn, lookupTableCodeColumn, lookupTableValueColumn) = parameters

    print()
    print(f'Checking {context} ...')

    # Check lookup for unique keys
    uniquenessOK = checkAllUniqueValues(context, dfLookup, lookupTableCodeColumn)
    if not uniquenessOK :
        return df

    print(f'... no duplicate codes in the {lookupTableCodeColumn} lookup table ...')

    # Outer join the main table and lookup table to find unused domain values in the lookup, and pull out records
    # with no value for the main table. We will only have one record per unused value. 
    dfJoin = pd.merge(df, dfLookup, left_on=mainDataFrameCodeJoinColumn, right_on=lookupTableCodeColumn, how='right')
    dfUnusedLookup = dfJoin[ dfJoin[mainDataFrameCodeJoinColumn].isnull() ][[lookupTableCodeColumn, lookupTableValueColumn]]

    unusedLookupsCount = dfUnusedLookup.shape[0]
    if unusedLookupsCount == 0 :
        print(f'... all values in the lookup table are referenced in the {mainDataFrameCodeJoinColumn} column ...')
    else :
        print(f'... {unusedLookupsCount} '
                    f'{"value in the lookup table is" if unusedLookupsCount == 1 else "values in the lookup table are"} '
                    f'not referenced in the {mainDataFrameCodeJoinColumn} column ...')
        if verbose :
            for index, row in dfUnusedLookup.iterrows() :
                print(f'  - {row.values[0]} : {row.values[1]}')


    # Outer join the main table and lookup table in the other direction to find referential integrity issues for 
    # column values in the main table with no matching value in the lookup table. There will probably be multiple
    # records having the same missing lookup value, so we need to do some grouping before reporting at an aggregate
    # level.
    dfJoin = pd.merge(df, dfLookup, left_on=mainDataFrameCodeJoinColumn, right_on=lookupTableCodeColumn, how='left')
    dfLookupNotFound = dfJoin[ dfJoin[lookupTableCodeColumn].isnull() & dfJoin[mainDataFrameCodeJoinColumn].notnull() ] [[mainDataFramePKColumn, mainDataFrameCodeJoinColumn]]

    dfNullValues = df[ df[mainDataFrameCodeJoinColumn].isnull() ] [[mainDataFramePKColumn, 'PostcodeArea']]

    lookupsNotFoundCount = dfLookupNotFound.shape[0]
    nullValuesCount = dfNullValues.shape[0]
    if lookupsNotFoundCount == 0 :
        print(f'... all {"" if nullValuesCount == 0 else "non-null "}rows in the main table {mainDataFrameCodeJoinColumn} column have a '
                    f'lookup value in the {lookupTableCodeColumn} column of the domain table')
    else :
        print(f'*** ... {lookupsNotFoundCount} {"" if nullValuesCount == 0 else "non-null "}row{"" if lookupsNotFoundCount == 1 else "s"} '
                    f'in the main table have no lookup value in the {lookupTableCodeColumn} column of the domain table ...')

        dfLookupNotFoundGrouped = dfLookupNotFound.groupby(mainDataFrameCodeJoinColumn, as_index=False).count()

        print(f'*** ... {dfLookupNotFoundGrouped.shape[0]} distinct code value{"" if lookupsNotFoundCount == 1 else "s"} unmatched:')
        if verbose :
            for index, row in dfLookupNotFoundGrouped.iterrows() :
                print(f'  *** {row.values[0]} : {row.values[1]}')

    if nullValuesCount == 0 :
        print(f'... all rows in the main table {mainDataFrameCodeJoinColumn} column have a non-null value')
    else :
        print(f'*** ... {nullValuesCount} row{"" if nullValuesCount == 1 else "s"} in the main table have'
                    f' a null value in the {mainDataFrameCodeJoinColumn} column ...')

        dfNullValuesGrouped = dfNullValues.groupby('PostcodeArea', as_index=False).count()

        print(f'*** ... {dfNullValuesGrouped.shape[0]} Postcode Area{"" if nullValuesCount == 1 else "s"} have null codes:')
        if verbose :
            for index, row in dfNullValuesGrouped[0:10].iterrows() :
                print(f'  *** {row.values[0]:2.2} : {row.values[1]}')
            if dfNullValuesGrouped.shape[0] > 10 :
                print(f'  *** ... and {dfNullValuesGrouped.shape[0] - 10} more ...')

    if verbose :
        if reportCodeUsage > 0 :
            reportingColumns = [mainDataFramePKColumn, mainDataFrameCodeJoinColumn, lookupTableValueColumn]
            dfReportGroup = dfJoin[reportingColumns] \
                            .groupby([mainDataFrameCodeJoinColumn, lookupTableValueColumn], as_index=False).count()
            print()
            print(f'{dfReportGroup.shape[0]} different {mainDataFrameCodeJoinColumn} values in use:')
            if dfReportGroup.shape[0] > reportCodeUsage :
                print(f'.. listing the first {reportCodeUsage} cases.')
            print()
            for index, row in dfReportGroup[0:reportCodeUsage].iterrows() :
                print(f'  {row.values[0]:10.10} {row.values[1]:30.30} {row.values[2]:7} rows')
            if dfReportGroup.shape[0] > reportCodeUsage :
                print(f'.. and {dfReportGroup.shape[0] - reportCodeUsage} more cases ..')

    return dfJoin

rowsProcessed = 0
xexpectedPatterns = [
    'X9##9XX',
    'X99#9XX',
    'X9X#9XX',
    'XX9#9XX',
    'XX999XX',
    'XX9X9XX'
]

expectedPatterns = [
    'X9  9XX',
    'X99 9XX',
    'X9X 9XX',
    'XX9 9XX',
    'XX999XX',
    'XX9X9XX'
]

import re
pcDigitPattern = re.compile('[0-9]')
pcLetterPattern = re.compile('[A-Z]')
pcSpacePattern = re.compile(r'\s')

# Can this be done using the replace(re)/extract(re) string operation on a column ? https://pandas.pydata.org/pandas-docs/stable/user_guide/text.html
# Might be much faster.
def getPattern(row) :

    global rowsProcessed

    # print(row)
    pc = row['Postcode']
    area = row['PostcodeArea']

    pattern = pcDigitPattern.sub('9', pc)
    pattern = pcLetterPattern.sub('X', pattern)
    pattern = pcSpacePattern.sub('#', pattern)          # dfpc['Postcode'].str.strip().str.replace(r'[0-9]', '9').str.replace(r'[A-Z]', 'X').str.replace(r'\s', '#')

    # Overall pattern should be one of 6 known ones
    
    if pattern not in expectedPatterns :
        print(f'*** unexpected pattern for {pc} : {pattern}')

    # inward = last three digits
    # outward = the rest
    # outward part from the first digit onwards = district
    # remainder of outward part is the area code, and should match the value already provded
    inward = pc[-3:].strip()            # dfpc['Postcode'].str[-3:].str.strip()
    outward = pc[0:-3].strip()          # dfpc['Postcode'].str[0:-3].str.strip()
    district = ''                       # dfpc['Postcode'].str[0:-3].str.strip().str.replace(r'[A-Z]', '')
    a = ''                              # dfpc['Postcode'].str[0:-3].str.strip().str.replace(r'[0-9]', '')
    for i, c in enumerate(outward) :
        if c.isdigit() :
            district = outward[i:]
            break
        else :
            a += c

    if a != area :
        print(f'*** area != area for {pc} : {a} : {area}')

    rowsProcessed += 1
    if rowsProcessed % 100000 == 0 :
        print(f'.. processed {rowsProcessed} postcode patterns ..')
    return { 'Pattern' : pattern, 'Outward' : outward, 'District' : district, 'Inward' : inward }

"""
         Postcode
New
X9##9XX     44363
X99#9XX    156642
X9X#9XX      9511
XX9#9XX    683419
XX999XX    797826
XX9X9XX     10728
"""

def addPostCodeBreakdown(df, verbose=False) :

    global rowsProcessed

    # Look at each postcode and convert to a pattern, extract subcomponents of the postcode into separate columns
    # NB This takes a few minutes for the full set of rows.

    #dfBreakdown = df.copy()
    #rowsProcessed = 0
    # extradf = dfBreakdown[['Postcode', 'Post Town', 'PostcodeArea']].apply(getPattern, axis=1, result_type='expand')      # NB This adds to df too

    dfBreakdown = df[['Postcode', 'PostcodeArea']].copy()

    # Check patterns are as expected first.
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

    dfBreakdown['Inward']   = dfBreakdown['Postcode'].str[-3:].str.strip()
    dfBreakdown['Outward']  = dfBreakdown['Postcode'].str[0:-3].str.strip()
    dfBreakdown[['Area', 'District']]  = dfBreakdown['Outward'].str.extract(r'([A-Z][A-Z]?)([0-9].*)')

    dfBadArea = dfBreakdown [ dfBreakdown.Area != dfBreakdown.PostcodeArea ]
    if dfBadArea.shape[0] != 0 :
        print('** Bad area extraction from postcode')
        print(dfBadArea)
        return pd.DataFrame()

    dfBadDistrict = dfBreakdown[ dfBreakdown.Area + dfBreakdown.District != dfBreakdown.Outward ]
    if dfBadDistrict.shape[0] != 0 :
        print('** Bad district extraction from postcode')
        print(dfBadDistrict)
        return pd.DataFrame()

    df[['Inward', 'Outward', 'Area', 'District', 'Pattern']] = dfBreakdown[['Inward', 'Outward', 'Area', 'District', 'Pattern']]

    #print(f'Concatenating extra postcode columns ..')
    #df = pd.concat([dfBreakdown, extradf], axis='columns')
    #print(f'.. extra postcode columns concatenated ..')

    # This is more like data examination than useful processing info. Do some other way.
    if verbose :
        print()
        print(f'.. Postcodes grouped by pattern ..')
        dfG = df[['Postcode', 'Pattern']].groupby(['Pattern'], as_index=True).count()
        print()
        print(dfG)
        with pd.option_context('display.max_rows', 20000):
            print()
            print(f'.. Postcodes grouped by Area and Outward ..')
            dfG = df[['Postcode', 'PostcodeArea', 'Post Town', 'Outward']].groupby(['PostcodeArea', 'Post Town', 'Outward'], as_index=True).count()
            print()
            print(dfG)
            print()
            print(f'.. Unique Districts per Area ..')
            dfG = df[['Postcode', 'PostcodeArea', 'Post Town', 'Outward']].groupby(['PostcodeArea', 'Post Town'], as_index=True)['Outward'].nunique()
            print()
            print(dfG)

    return df

#############################################################################################

def displayBasicDataFrameInfo(df, verbose=False) :
    """ See what some basic pandas info calls show about the dataframe. """

    print()
    print('###################################################')
    print('################## type and shape #################')
    print()
    print(f'type(df) = {type(df)} : df.shape : {df.shape}') 
    print()
    print('################## print(df) #####################')
    print()
    print(df)
    print()
    print('################## df.dtypes ##################')
    print()
    print(df.dtypes)
    print()
    print('################## df.info() ##################')
    print()
    print(df.info())
    print()
    print('################## df.head() ##################')
    print()
    print(df.head())
    print()
    print('################## df.tail() ##################')
    print()
    print(df.tail())
    print()
    print('################## df.index ##################')
    print()
    print(df.index)
    print()
    print('################## df.columns ##################')
    print()
    print(df.columns)
    print()
    print('################## df.describe() ##################')
    print()
    print(df.describe())
    print()
    print('################## df.count() ##################')
    print()
    print(df.count())
    print()
    print('###################################################')

    return 0

# Code point Open User Guide explains the Quality values as follows:
# https://www.ordnancesurvey.co.uk/documents/product-support/user-guide/code-point-open-user-guide.pdf
# 
# 10 Within the building of the matched address closest to the postcode mean determined automatically by Ordnance Survey.
# 20 As above, but determined by visual inspection by NRS.
# 30 Approximate to within 50 m of true position (postcodes relating to developing sites may be within 100 m of true position).
# 40 The mean of the positions of addresses previously matched in PALF but that have subsequently been deleted or recoded (very rarely used).
# 50 Estimated position based on surrounding postcode coordinates, usually to 100 m resolution, but 10 m in Scotland.
# 60 Postcode sector mean (direct copy from PALF). See glossary for additional information.
# 90 No coordinates available.
#
# Observation of data shows that:
# - for values of 60 and 90, District and Ward information is always missing
# - for values of 90, Eastings and Northings values are set to 0
# - [County information is much more variable - absent for all 60 and 90 examples, but also in many 10,20,30 and 50.]
# for values 10,20,30,50 all information is present
# No cases of 40 were present in the data.
# Nearly all data is assigned as 10, with a small amount of 50 and 90, and traces of 20,30,60


def examineLocationColumns(df, verbose=False) :

    if not verbose :
        return

    dfG = df[ ['Postcode', 'PostcodeArea', 'Quality', 'Eastings', 'Northings', 'County Name', 'District Name', 'Ward Name'] ].groupby('Quality').count()
    print()
    print('Counts of non-null values by location quality:')
    print()
    print(dfG)

    dfG = df[ ['Postcode', 'PostcodeArea', 'Quality', 'Eastings', 'Northings'] ].groupby('Quality').agg(
                Cases = ('Quality', 'count'),
                Min_E = ('Eastings', 'min'),
                Max_E = ('Eastings', 'max'),
                Min_N = ('Northings', 'min'),
                Max_N = ('Northings', 'max')
                    )
    print()
    print('Eastings and Northing ranges by location quality:')
    print()
    print(dfG)

    print()
    print('Quality=60 examples')
    print()
    print(df[ df['Quality'] == 60][0:10])
    print()
    print('Quality=90 examples:')
    print()
    print(df[ df['Quality'] == 90][0:10])

