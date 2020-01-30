import os
import sys

import zipfile
import pandas as pd
import pickle

"""
https://en.wikipedia.org/wiki/List_of_postcode_districts_in_the_United_Kingdom
https://en.wikipedia.org/wiki/List_of_postcode_areas_in_the_United_Kingdom
https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-november-2019
"""

def unpackZipFile(OSZipFile, tmpDir, detail=False) :
    """ Unzips the data file under a temporary directory. Checks basic sub-directories are as expected."""
    z = zipfile.ZipFile(OSZipFile, mode='r')

    zinfolist = z.infolist()
    for zinfo in zinfolist :
        if zinfo.filename.startswith('Data/') or zinfo.filename.startswith('Doc/') :
            if detail: print(zinfo.filename)
        else :
            print("*** Unexpected extract location found:", zinfo.filename, file=sys.stderr)

    print()
    print(f'Extracting zip file {OSZipFile} under {tmpDir} ...')
    z.extractall(path=tmpDir)
    z.close()

#############################################################################################
# Items relating to the columns of in the main CSV file and resulting dataframe

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

#############################################################################################

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
                print(f"Columns definition file {csvColumnNamesFile} has the expected columns ...")

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
    print(f'Found {len(matchingFilenames)} CSV files to process ...')

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

def displayBasicInfo(df) :
    """ See what the basic pandas info calls show about the dataframe. """

    print()
    print('###################################################')
    print()
    print(f'type(df) : {type(df)}') 
    print(f'df.shape : {df.shape}')
    print()
    print('################## df ##################')
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


#############################################################################################

def aggregate(df) :

    print(f'############### Grouping by PostcodeArea, all columns ###############')
    print()
    dfAreaCounts = df.groupby('PostcodeArea').count()
    print(f'Shape is {dfAreaCounts.shape}')
    print()
    print(dfAreaCounts)

    groupByColumns = [ 'PostcodeArea', 'Quality', 'Country_code', 'Admin_county_code', 'Admin_district_code', 
                        'Admin_district_code', 'Admin_ward_code' ]

    # Just show counts of each distinct value, column by column
    for groupByColumn in groupByColumns :
        # Need a specific column to count, otherwise just get list of group-by-column values with no counts.
        print()
        print(f'############### Grouping by {groupByColumn}, count only ###############')
        print()
        dfDistinctColumnValueCounts = df.assign(count=1)[[groupByColumn, 'count']].groupby(groupByColumn).count()
        print(f'Shape is {dfDistinctColumnValueCounts.shape}')
        print()
        print(dfDistinctColumnValueCounts)
        print(dfDistinctColumnValueCounts[0])
        print(dfDistinctColumnValueCounts[1])
        

    # Just PostcodeArea = shows that just a list of distinct values is returned when grouping a column with itself.
    dfAreaCounts = df[['PostcodeArea']].groupby('PostcodeArea').count()
    print()
    print(f'############### Grouping by PostcodeArea with itself ###############')
    print()
    print(f'Shape is {dfAreaCounts.shape}')
    print()
    print(dfAreaCounts)

#############################################################################################


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
    print()
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
    
    print()
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
    
    print()
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
    
    print()
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

def checkCodesReferentialIntegrity(df, dfLookup, parameters, reportCodeUsage=10) :

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
        for index, row in dfLookupNotFoundGrouped.iterrows() :
            print(f'  *** {row.values[0]} : {row.values[1]}')

    if nullValuesCount == 0 :
        print(f'... all rows in the main table {mainDataFrameCodeJoinColumn} column have a non-null value')
    else :
        print(f'*** ... {nullValuesCount} row{"" if nullValuesCount == 1 else "s"} in the main table have'
                    f' a null value in the {mainDataFrameCodeJoinColumn} column ...')

        dfNullValuesGrouped = dfNullValues.groupby('PostcodeArea', as_index=False).count()

        print(f'*** ... {dfNullValuesGrouped.shape[0]} Postcode Area{"" if nullValuesCount == 1 else "s"} have null codes:')
        for index, row in dfNullValuesGrouped[0:10].iterrows() :
            print(f'  *** {row.values[0]:2.2} : {row.values[1]}')
        if dfNullValuesGrouped.shape[0] > 10 :
            print(f'  *** ... and {dfNullValuesGrouped.shape[0] - 10} more ...')

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

def displayExample(df, example=None) :
    # Print out details of an example postcode (Trent Bridge).
    examplePostCode = 'NG2 6AG' if example == None else example

    print()
    print('###############################################################')
    print()
    print(f'Values for example postcode {examplePostCode}')
    print()
    format="Vertical"

    if format == "Vertical" :
        # Print vertically, so all columns are listed
        print(df [df['Postcode'] == examplePostCode] .transpose(copy=True))
    elif format == "Horizontal" :
        # Print horizontally, no wrapping, just using the available space, omitting columns in the middle
        # if needed. (The default setting.)
        print(df [df['Postcode'] == examplePostCode])
    elif format == "HorizontalWrap" :
        # Print horizontally, wrapping on to the next line, so all columns are listed
        pd.set_option('display.expand_frame_repr', False)
        print(df [df['Postcode'] == examplePostCode])
        pd.set_option('display.expand_frame_repr', True)        # Reset to default.
    else :
        print('f*** Unrecognised output row format: {format}')

    print()
    print('###############################################################')


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


def examineLocationColumns(df) :

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

def getCacheFilePath(tmpDir) :
    return tmpDir + '/cached/df.cache'

def readCachedDataFrameFromFile(tmpDir) :

    cacheFile = getCacheFilePath(tmpDir)
    print()
    print(f'Reading dataframe from cache file {cacheFile} ..')
    if os.path.isfile(cacheFile) :
        print(f'.. cache file {cacheFile} found ..')

        with open(cacheFile, 'rb') as f:
            response = pickle.load(f)
            print(f'.. read pre-existing response from cache file')
    else :
        print(f'*** No cache file {cacheFile} found.')
        response = pd.DataFrame()

    return response

def writeCachedDataFrame(tmpDir, df) :
    cacheFile = getCacheFilePath(tmpDir)
    print()
    print(f'Writing dataframe to cache file {cacheFile} ..')
    
    cacheFileDir = os.path.dirname(cacheFile)
    if not os.path.isdir(cacheFileDir) :
        os.makedirs(cacheFileDir)
        print(f'.. created cache location {cacheFileDir}')

    # Use pickle to cache the response data.
    with open(cacheFile, 'wb') as f:
        pickle.dump(df, f)
        print(f'.. written dataframe as binary object to cache file {cacheFile}')

def regenerateDataFrame(OSZipFile, tmpDir, postcodeAreasFile) :

    dfEmpty = pd.DataFrame()

    if not os.path.isfile(OSZipFile) :
        print("*** No OS zip file found:", OSZipFile, file=sys.stderr)
        return dfEmpty

    if not os.path.isfile(postcodeAreasFile) :
        print("*** No ONS postcode areas spreadsheet file found:", postcodeAreasFile, file=sys.stderr)
        return dfEmpty

    print()
    print('Loading code lookup data ..')

    dfAreas = loadPostcodeAreasFile(postcodeAreasFile)
    if dfAreas.empty :
        return dfEmpty

    dfCountries = loadCountryCodes()
    if dfCountries.empty :
        return dfEmpty

    dfCounties = loadCountyCodes(tmpDir)
    if dfCounties.empty :
        return dfEmpty

    dfDistricts = loadDistrictCodes(tmpDir)
    if dfDistricts.empty :
        return dfEmpty

    dfWards = loadWardCodes(tmpDir)
    if dfWards.empty :
        return dfEmpty

    unpackZipFile(OSZipFile, tmpDir)

    startTime = pd.Timestamp.now()
    df = loadFilesIntoDataFrame(tmpDir)
    took = pd.Timestamp.now()-startTime
    if not df.empty :
        print(f'Took {took.total_seconds()} seconds to load data files into a dataframe')
    else :
        return dfEmpty

    displayBasicInfo(df)

    # Add further columns showing looked-up values of the various code columns, checking referential
    # itegrity and null-ness at the same time.
    dfDenormalised = df
    areasParameters = ('Postcode Area Codes', 'Postcode', 'PostcodeArea', 'Postcode Area','Post Town')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dfAreas, areasParameters, 150)

    countriesParameters = ('Country Codes', 'Postcode', 'Country_code', 'Country Code', 'Country Name')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dfCountries, countriesParameters, 10)

    countiesParameters = ('County Codes', 'Postcode', 'Admin_county_code', 'County Code','County Name')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dfCounties, countiesParameters, 50)

    districtsParameters = ('District Codes', 'Postcode', 'Admin_district_code', 'District Code','District Name')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dfDistricts, districtsParameters, 20)

    wardsParameters = ('Ward Codes', 'Postcode', 'Admin_ward_code', 'Ward Code','Ward Name')
    dfDenormalised = checkCodesReferentialIntegrity(dfDenormalised, dfWards, wardsParameters, 20)

    # Prune the column list - the above will have added an extra copy of each 'code' column that we can
    # remove again. Just use the original columns in the dataframe and the main lookup columns added.
    
    fullOutputColumns = (list(df.columns)).copy()
    fullOutputColumns.extend(['Post Town', 'Country Name', 'County Name', 'District Name', 'Ward Name'])
    dfDenormalised = dfDenormalised[fullOutputColumns]

    examineLocationColumns(dfDenormalised)

    return dfDenormalised

#############################################################################

from tkinter import Tk, Canvas, mainloop

pcDefaultColour = "black"
pcDefaultColourRGB = "(0,0,0)"

def assignAreasToColourGroups(df) :
    # Determine extent of each Postcode Area

    print('Determining Eastings and Northing ranges by Postcode area:')
    # Ignore 0s
    df = df [ df['Eastings'] != 0 ]
    dfAreaExtents = df[ ['PostcodeArea', 'Eastings', 'Northings'] ].groupby('PostcodeArea').agg(
                Cases = ('PostcodeArea', 'count'),
                Min_E = ('Eastings', 'min'),
                Max_E = ('Eastings', 'max'),
                Min_N = ('Northings', 'min'),
                Max_N = ('Northings', 'max')
                    )
    #print()
    #print('Eastings and Northing ranges by Postcode area:')
    #print()
    #print(dfAreaExtents)

    # ???? Algorithm to assign areas to colour groups so that close areas don't use the same colour. 
    # For now just use lots of colours and rely on chance ! 
    # https://www.tcl.tk/man/tcl8.4/TkCmd/colors.htm
    availableColours = [ "red", "blue", "green", "yellow", "orange", "purple", "brown", 
                            "pink", "cyan", "magenta", "violet", "grey"]
    availableColoursRGB = [ (255,0,0), (0,0,255), (0,255,0), (255,255,0), (255,165,0), (160,32,240), (165,42,42), 
                            (255,192,203), (0,255,255), (255,0,255), (238,130,238), (190,190,190)]
    numGroups = len(availableColours)

    # Set up a list of lists, one per colour
    colourGroupsList = []
    for i in range(numGroups) :
        colourGroupsList.append([])

    # Spread the Postcode areas across these lists
    for index, (row) in enumerate(dfAreaExtents.iterrows()) :
        colourGroupsList[index % numGroups].append(row[0]) 

    # Produce a dictionary to map each Postcode Area to its colour
    d = {}
    dRGB = {}
    for i in range(numGroups) :
        for a in colourGroupsList[i] :
            d[a] = availableColours[i]
            dRGB[a] = availableColoursRGB[i]

    return d, dRGB

def tkPlot(df, density=100) :

    # Dictionary to allow us to show different Postcode Areas in different colours.
    areaColourDict, areaColourDictRGB = assignAreasToColourGroups(df)

    master = Tk()
    canvas_width = 800
    canvas_height = 1000            # Need to get to 1213165 to cover Shetland, 

    london = True
    if london :
        scaling_factor = 100
        offset_e = -480000
        offset_n = -120000
    else :
        scaling_factor = 1000
        offset_e = 0
        offset_n = 0

    w = Canvas(master, 
            width=canvas_width,
            height=canvas_height)
    w.pack()

    # Vary width of oval dot depending on density ????
    # Do we need to use 'oval's to do the plotting, rather than points ?


    # Different ways to work through the set of postcodes, row by row.
    useZip = True
    if useZip :
        # Faster than using iterrows.
        # Could do bulk e/n scaling first too in bulk ?
        # Keep more Scotland (and perhaps Wales, and perhaps more generally remote areas) to maintain shape of landmass ?
        dfSlice = df.iloc[::density]
        print(dfSlice)
        for index, r in enumerate(zip(dfSlice['Eastings'], dfSlice['Northings'], dfSlice['Postcode'], dfSlice['PostcodeArea'])):
            (e, n, pc, area) = r
            if e == 0 :
                continue
            e_offset  = e + offset_e
            n_offset  = n + offset_n
            e_scaled = e_offset // scaling_factor
            n_scaled = canvas_height - n_offset // scaling_factor
            if index % (100000/density) == 0 :
                print(index, e_scaled, n_scaled, pc)
            if n_scaled >=0 and n_scaled < canvas_height :
                if e_scaled >=0 and e_scaled < canvas_width :
                #print(index, e_scaled, n_scaled, pc)
                    colour = areaColourDict.get(area, pcDefaultColour)
                    w.create_oval(e_scaled,n_scaled,e_scaled,n_scaled, fill=colour, outline=colour, width=0)
                    #w.create_line(e_scaled,n_scaled,e_scaled+1,n_scaled, fill=colour, width=1)
    else :
        for index, row in df[::density].iterrows():
            e = getattr(row, "Eastings")
            n = getattr(row, "Northings")
            pc = getattr(row, "Postcode")
            area = getattr(row, "PostcodeArea")
            if e == 0 :
                continue
            e_scaled = e // scaling_factor
            n_scaled = canvas_height - n // scaling_factor
            if index % 100000 == 0 :
                print(index, e_scaled, n_scaled, pc)
            e_scaled += e_offset
            n_scaled += n_offset
            if n_scaled >=0 and n_scaled < canvas_height :
                #print(index, e_scaled, n_scaled, pc)
                colour = areaColourDict.get(area, pcDefaultColour)
                w.create_oval(e_scaled,n_scaled,e_scaled,n_scaled, fill=colour, outline=colour, width=2)

    # Display the tk plot

    mainloop()


import numpy as np

def newImageArray(y, x) :
    """ Create a new image array of dimensions [y,x,RGB], set to all white """
    return np.full((y,x,3), 255, dtype='uint8')

def convertToBGR(imgArray) :
    """ Converts a 3-D [y,x,RGB] numpy array to [y,x,BGR] format, (for use with CV2) """
    return imgArray[:,:,::-1]

def cv2plot(df, density=100) :
    # For CV2 we need to reverse the colour ordering of the array to BGR
    from cv2 import cv2

    canvas_width = 800
    canvas_height = 1000            # Need to get to 1213165 to cover Shetland, 
    img = newImageArray(canvas_height, canvas_width)

    areaColourDict, areaColourDictRGB = assignAreasToColourGroups(df)

    london = True
    if london :
        scaling_factor = 100
        offset_e = -480000
        offset_n = -120000
    else :
        scaling_factor = 1000
        offset_e = 0
        offset_n = 0

    # Vary width of oval dot depending on density ????
    # Do we need to use 'oval's to do the plotting, rather than points ?

    # Different ways to work through the set of postcodes, row by row.
    useZip = True
    if useZip :
        # Faster than using iterrows.
        # Could do bulk e/n scaling first too in bulk ?
        # Keep more Scotland (and perhaps Wales, and perhaps more generally remote areas) to maintain shape of landmass ?
        dfSlice = df.iloc[::density]
        print(dfSlice)
        for index, r in enumerate(zip(dfSlice['Eastings'], dfSlice['Northings'], dfSlice['Postcode'], dfSlice['PostcodeArea'])):
            (e, n, pc, area) = r
            if e == 0 :
                continue
            e_offset  = e + offset_e
            n_offset  = n + offset_n
            e_scaled = e_offset // scaling_factor
            n_scaled = canvas_height - n_offset // scaling_factor
            if index % (100000/density) == 0 :
                print(index, e_scaled, n_scaled, pc)
            if n_scaled >=0 and n_scaled < canvas_height :
                if e_scaled >=0 and e_scaled < canvas_width :
                #print(index, e_scaled, n_scaled, pc)
                    colour = areaColourDictRGB.get(area, pcDefaultColourRGB)
                    #colour = RGBColourGreen = (0,255,0)
                    #w.create_oval(e_scaled,n_scaled,e_scaled,n_scaled, fill=colour, outline=colour, width=0)
                    #w.create_line(e_scaled,n_scaled,e_scaled+1,n_scaled, fill=colour, width=1)
                    thickness = -1      # negative == fill
                    lineType = 8
                    shift = 0
                    cv2.circle(img, center=(e_scaled,n_scaled), radius=1, color=colour, thickness=thickness)

    title = 'Title'
    cv2.imshow(title, convertToBGR(img.copy()))
    cv2.waitKey(0)
    cv2.destroyAllWindows()

import re
pcreDigit = '[0-9]'
pcDigitPattern = re.compile(pcreDigit)
pcreLetter = '[A-Z]'
pcLetterPattern = re.compile(pcreLetter)
pcreSpace = r'\s'
pcSpacePattern = re.compile(pcreSpace)


def getPattern(row) :
    pc = row['Postcode']
    area = row['PostcodeArea']
    result = pc + "-" + area

    result = pcDigitPattern.sub('9', pc)
    result = pcLetterPattern.sub('X', result)
    result = pcSpacePattern.sub('#', result)

    areaPattern = pcDigitPattern.sub('9', area)
    areaPattern = pcLetterPattern.sub('X', areaPattern)
    areaPattern = pcSpacePattern.sub('#', areaPattern)

    result = result + '=' + areaPattern

    outward = pc[0:-3].strip()
    #print(f'In getPattern: {pc} + {area} = {result} : outward = {outward}')
    return result

    # How to return values for multiple columns ? Tuple doesn't seem to work
"""
         Postcode
New
X9##9XX     44363
X99#9XX    156642
X9X#9XX      9511
XX9#9XX    683419
XX999XX    797826
XX9X9XX     10728

            Postcode
New
X9##9XX=X      44363
X99#9XX=X     156642
X9X#9XX=X       9511
XX9#9XX=XX    683419
XX999XX=XX    797826
XX9X9XX=XX     10728
"""

# All end 9XX. Variety of starts:
# X9
# X99
# X9X
# XX9
# XX99
# XX9X
#
# Really 3 variants X9, X99, X9X  repeated with leading XX where the postal area has two letters.
# To get the outward part of the postcode, remove 9XX from end, trim spaces


def assessPostCodeBreakdown(df) :

    # Look at each postcode and convert to a pattern.
    # 

    # df['pattern'] = 
    dfTrial = df
    #print()
    #print(dfTrial)
    dfTrial['New'] = dfTrial.apply(getPattern, axis=1)      # NB This adds to df too
    print()
    print(dfTrial)
    dfG = dfTrial[['Postcode', 'New']].groupby(['New'], as_index=True).count()
    print()
    print(dfG)
    print()
    print(df)

def main(args) :

    OSZipFile = r"./OSData/PostCodes/codepo_gb.zip"
    tmpDir = os.path.dirname(OSZipFile) + '/tmp'
    postcodeAreasFile = r"./OSData/PostCodes/postcode_district_area_lists.xls"

    # Decide whether to generate a new dataframe or read a cached one from file. Any sort of command line argument
    # means generate from scratch.
    readFromFile = False
    if len(args) > 1 :
        readFromFile = False
    else :
        readFromFile = True

    if readFromFile :
        df = readCachedDataFrameFromFile(tmpDir)
    else :
        df = regenerateDataFrame(OSZipFile, tmpDir, postcodeAreasFile)
        if not df.empty :
            writeCachedDataFrame(tmpDir, df)

    if df.empty :
        print()
        print('*** No dataframe produced')
        return

    print()
    print(df)

    displayExample(df)

    assessPostCodeBreakdown(df)

    #aggregate(df)
    print()

    #tkPlot(df, 1)
    #cv2plot(df, 1)


if __name__ == '__main__' :
    main(sys.argv)

