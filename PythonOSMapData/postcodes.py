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

def saveAsCSV(df, tmpDir) :
    outputDir = tmpDir + '/' + 'output'
    outFile = outputDir + '/' + 'postcodes.out.csv'

    print(f'Saving dataframe to CSV file {outFile} ..')
    if not os.path.isdir(outputDir) :
        os.makedirs(outputDir)
        print(f'.. created output location {outputDir} ..')

    df.to_csv(outFile, index=False)
    print(f'.. saved dataframe to CSV file {outFile}')



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

def displayExample(df, example=None, plotter='CV2', dimension=25) :
    # Print out details of an example postcode (Trent Bridge).
    examplePostCode = 'NG2 6AG' if example == None else example

    formattedPostCode = normalisePostCodeFormat(examplePostCode)

    print()
    print('###############################################################')
    print()
    print(f'Values for example postcode {examplePostCode}')
    print()
    format="Vertical"

    founddf = df [df['Postcode'] == formattedPostCode]
    if founddf.empty :
        print(f'*** Postcode not found : {formattedPostCode}')
        return

    if format == "Vertical" :
        # Print vertically, so all columns are listed
        print(founddf.transpose(copy=True))
    elif format == "Horizontal" :
        # Print horizontally, no wrapping, just using the available space, omitting columns in the middle
        # if needed. (The default setting.)
        print(founddf)
    elif format == "HorizontalWrap" :
        # Print horizontally, wrapping on to the next line, so all columns are listed
        pd.set_option('display.expand_frame_repr', False)
        print(founddf)
        pd.set_option('display.expand_frame_repr', True)        # Reset to default.
    else :
        print('f*** Unrecognised output row format: {format}')
        print(founddf)

    print()
    print('###############################################################')

    showPostcode(df, formattedPostCode, plotter='CV2', savefilelocation=None)

def normalisePostCodeFormat(postCode) :
    pc = postCode.upper().strip().replace(' ', '')

    # Formats which we can use:
    # 'X9##9XX',
    # 'X99#9XX',
    # 'X9X#9XX',
    # 'XX9#9XX',
    # 'XX999XX',
    # 'XX9X9XX'
    # all 7 chars long
    # If more than 7, leave - must be wrong
    # If 6 chars long, put in a space in the middle
    # If 5 chars long, put in two spaces in the middle
    # If less than 5, leave - must be wrong
    # Don't bother checking letter/number aspects - just trying to smooth out spacing/case issues here.

    if len(pc) == 6 :
        pc = pc[0:3] + ' ' + pc[3:]
        print(f'6 : Modified {postCode} to {pc}')
    elif len(pc) == 5 :
        pc = pc[0:2] + '  ' + pc[2:]
        print(f'5 : Modified {postCode} to {pc}')

    return pc

def showPostcode(df, postCode, plotter='CV2', savefilelocation=None) :

    formattedPostCode = normalisePostCodeFormat(postCode)

    founddf = df [df['Postcode'] == formattedPostCode]
    if founddf.empty :
        print(f'*** Postcode not found in dataframe : {postCode}')
        return 1
    else :
        showAround(df, postCode, founddf['Eastings'], founddf['Northings'], 1, plotter)
        return 0

def showAround(df, title, e, n, dimension_km, plotter) :
    dimension_m = dimension_km * 1000       # m
    bl = (int(e-dimension_m/2), int(n-dimension_m/2))
    tr = (int(e+dimension_m/2), int(n+dimension_m/2))
    print(f'bl = {bl} : tr = {tr}')

    if plotter == 'CV2' :
        cv2plotSpecific(df, title=title, canvas_h=800, density=1, bottom_l=bl, top_r=tr)
    elif plotter == 'Tk' :
        tkplotSpecific(df, title=title, canvas_h=800, density=1, bottom_l=bl, top_r=tr)

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

    print('Regenerating postcode DataFrame from source data files ..')

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

    #displayBasicInfo(df)

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

    startTime = pd.Timestamp.now()
    dfDenormalised = addPostCodeBreakdown(dfDenormalised)
    took = pd.Timestamp.now()-startTime
    print(f'Took {took.total_seconds()} seconds to add postcode breakdown columns')

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
    dfAreaExtents.sort_values('Min_E', inplace=True)
    print()
    print('Eastings and Northing ranges by Postcode area:')
    print()
    print(type(dfAreaExtents))
    print()
    print(dfAreaExtents)

    # ???? Algorithm to assign areas to colour groups so that close areas don't use the same colour. 
    # For now just use lots of colours and rely on chance ! 
    # https://www.tcl.tk/man/tcl8.4/TkCmd/colors.htm
    # Doesn't seem to work very well = e.g. YO (York) and TS (Teeside) have same colour, and also WC and SE in London. Also
    # PE (Peterborough) and MK (Milton Keynes). IG/RM/SS form a triple ! Probably more ..
    # https://stackoverflow.com/questions/309149/generate-distinctly-different-rgb-colors-in-graphs has lots of colours about half way down
    availableColours = [ "red", "blue", "green", "yellow", "orange", "purple", "brown", 
                            "pink", "cyan2", "magenta2", "violet", "grey"]
    availableColoursRGB = [ (255,0,0), (0,0,255), (0,255,0), (255,255,0), (255,165,0), (160,32,240), (165,42,42), 
                            (255,192,203), (0,238,238), (238,0,238), (238,130,238), (190,190,190)]

    # Soften the colours
    ac2 = []
    for c in availableColoursRGB :
        n = [0,0,0]
        for i in (0,1,2) :
            ci = c[i]
            if ci < 255 :
                ci = int(ci + (255-ci) * 0.5)
            n[i] = ci
        ac2.append( (n[0],n[1],n[2]))

    print(ac2)
    availableColoursRGB = ac2



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


tkObjDict = {}
dfClickLookup = None

# https://stackoverflow.com/questions/20399243/display-message-when-hovering-over-something-with-mouse-cursor-in-python

def onObjectClick(event):                  
    print('Got tk object click', event.x, event.y)
    #print(event.widget.find_closest(event.x, event.y))
    objID = event.widget.find_closest(event.x, event.y)[0]
    #print(type(objID))
    pc = tkObjDict[objID]
    pcinfo = dfClickLookup [dfClickLookup['Postcode'] == pc]

    print(f'obj={objID} : pc={pc}')
    print(pcinfo)

def tkplotSpecific(df, title=None, canvas_h=1000, bottom_l=(0,0), top_r=(700000,1250000), density=100) :

    canvas_height, canvas_width, dfSlice = getScaledPlot(df, canvas_h, bottom_l, top_r, density)
    areaColourDict, areaColourDictRGB = assignAreasToColourGroups(df)

    global dfClickLookup
    dfClickLookup = df

    # Vary width of oval dot depending on density ????
    # Do we need to use 'oval's to do the plotting, rather than points ?

    # https://effbot.org/tkinterbook/canvas.htm
    master = Tk()
    w = Canvas(master, width=canvas_width, height=canvas_height)
    w.pack()

    for index, r in enumerate(zip(dfSlice['e_scaled'], dfSlice['n_scaled'], dfSlice['Postcode'], dfSlice['PostcodeArea'])):
        (es, ns, pc, area) = r
        if index % (100000/density) == 0 :
            print(index, es, ns, pc)
        colour = areaColourDict.get(area, pcDefaultColour)
        objid = w.create_oval(es,canvas_height-ns,es,canvas_height-ns, fill=colour, outline=colour, width=2)
        w.tag_bind(objid, '<ButtonPress-1>', onObjectClick)    
        #print(f'Adding objid: {objid}')
        tkObjDict[objid] = pc

    mainloop()

import numpy as np

def newImageArray(y, x) :
    """ Create a new image array of dimensions [y,x,RGB], set to all white """
    return np.full((y,x,3), 255, dtype='uint8')

def convertToBGR(imgArray) :
    """ Converts a 3-D [y,x,RGB] numpy array to [y,x,BGR] format, (for use with CV2) """
    return imgArray[:,:,::-1]

def getScaledPlot(df, canvas_h=1000, bottom_l=(0,0), top_r=(700000,1250000), density=100) :
    e0 = bottom_l[0]
    e1 = top_r[0]
    n0 = bottom_l[1]
    n1 = top_r[1]
    e_extent = e1 - e0
    n_extent = n1 - n0

    print(f'canvas_h = {canvas_h} : bottom_l = {bottom_l} : top_r = {top_r}')
    canvas_height = int(canvas_h)                        # pixels
    canvas_width = int(canvas_h * e_extent / n_extent)   # pixels
    scaling_factor = n_extent / canvas_h            # metres per pixel

    print(f'.. scale = {scaling_factor} : canvas_width = {canvas_width}')

    # Could do bulk e/n scaling first too in bulk ?
    # Keep more Scotland (and perhaps Wales, and perhaps more generally remote areas) to maintain shape of landmass ?
    if density != 1 :
        dfSlice = df.copy().iloc[::density]
    else :
        dfSlice = df.copy()
    #print(dfSlice)

    dfSlice['e_scaled'] = (dfSlice['Eastings'] - e0) // scaling_factor
    dfSlice['n_scaled'] = (dfSlice['Northings'] - n0) // scaling_factor
    #print(dfSlice)

    dfSlice = dfSlice [ dfSlice['Eastings'] > 0 ]
    dfSlice = dfSlice [ (dfSlice['n_scaled'] >= 0) & (dfSlice['n_scaled'] <= canvas_height)]
    dfSlice = dfSlice [ (dfSlice['e_scaled'] >= 0) & (dfSlice['e_scaled'] <= canvas_width)]

    # Why can't the above be combined into one big combination ?
    # Also, any way to check n_scale and e_scaled are in a range ?
    #dfSlice = dfSlice [ dfSlice['Eastings'] > 0 &
    #                    ((dfSlice['n_scaled'] >= 0) & (dfSlice['n_scaled'] <= canvas_height)) &
    #                    ((dfSlice['e_scaled'] >= 0) & (dfSlice['e_scaled'] <= canvas_width)) ]

    return canvas_height, canvas_width, dfSlice

# Map 
from cv2 import cv2

img = None
imgLookupIndex = None


def CV2ClickEvent(event, x, y, flags, param):
    if event == cv2.EVENT_LBUTTONDOWN:
        yy = img.shape[0] - y
        print (f'CV2 event: event={event}, x={x}, y={y} == {yy}, flags={flags}, param={param}')
        # y is first dimension of image - need to subtract from height
        # NB RGB in img array, convertToBGR converts just before CV2 sees it
        red = img[y,x,0]
        green = img[y,x,1]
        blue = img[y,x,2]
        print (f'{red} : {blue} : {green}')
        if red == 255 and green == 255 and blue == 255 :
            print('.. white')
        else :
            print('.. point - looking up')
            index = imgLookupIndex[yy,x]
            pcinfo = dfClickLookup.iloc[index]
            print(f'index={index}')
            print(pcinfo)

def cv2plotSpecific(df, title=None, canvas_h=1000, bottom_l=(0,0), top_r=(700000,1250000), density=100) :

    canvas_height, canvas_width, dfSlice = getScaledPlot(df, canvas_h, bottom_l, top_r, density)
    areaColourDict, areaColourDictRGB = assignAreasToColourGroups(df)

    global img
    global dfClickLookup
    global imgLookupIndex
    img = newImageArray(canvas_height, canvas_width)
    imgLookupIndex = np.full((canvas_height+2, canvas_width+2), 0, dtype='int32')   # +2s partly because of size of circle,
    # but also getting some out of bounds errors before [-1,0,1] adjustment was there - because ????
    dfClickLookup = dfSlice

    for index, r in enumerate(zip(dfSlice['e_scaled'], dfSlice['n_scaled'], dfSlice['Postcode'], dfSlice['PostcodeArea'])):
        (es, ns, pc, area) = r
        if index % (100000/density) == 0 :
            print(index, es, ns, pc)
        colour = areaColourDictRGB.get(area, pcDefaultColourRGB)
        #circle radius = 0 == single pixel. 
        #                          x
        # radius=1 gives a cross: xxx
        #                          x
        # cv2.circle(img, center=(es, canvas_height-ns), radius=0, color=colour, thickness=-1)

        x = 2   # Seems to work well
        cv2.rectangle(img, pt1=(es, canvas_height-ns), pt2=(es+x, canvas_height-ns+x), color=colour, thickness=-1)
        # Record item against a small 3x3 square of points, not just the central one. Why is this necessary,
        # how does it relate to the radius value ? What about close postcodes overwriting each other ?
        for i in [-1,0,1] :
            for j in [-1,0,1] :
                imgLookupIndex[ns+i, es+j] = index

    cvtitle = 'Title' if title == None else title
    v_km = (top_r[0] - bottom_l[0]) // 1000
    h_km = (top_r[1] - bottom_l[1]) // 1000
    dimensions = f'{v_km} km x {h_km} km'
    cvtitle = f'{cvtitle} : {dimensions}'
    # For CV2 we need to reverse the colour ordering of the array to BGR
    cv2.imshow(cvtitle, convertToBGR(img))
    cv2.setMouseCallback(cvtitle, CV2ClickEvent)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

    return img

rowsProcessed = 0
expectedPatterns = [
    'X9##9XX',
    'X99#9XX',
    'X9X#9XX',
    'XX9#9XX',
    'XX999XX',
    'XX9X9XX'
]

import re
pcDigitPattern = re.compile('[0-9]')
pcLetterPattern = re.compile('[A-Z]')
pcSpacePattern = re.compile(r'\s')

def getPattern(row) :

    global rowsProcessed

    # print(row)
    pc = row['Postcode']
    area = row['PostcodeArea']

    pattern = pcDigitPattern.sub('9', pc)
    pattern = pcLetterPattern.sub('X', pattern)
    pattern = pcSpacePattern.sub('#', pattern)

    # Overall pattern should be one of 6 known ones
    
    if pattern not in expectedPatterns :
        print(f'*** unexpected pattern for {pc} : {pattern}')

    # inward = last three digits
    # outward = the rest
    # outward part from the first digit onwards = district
    # remainder of outward part is the area code, and should match the value already provded
    inward = pc[-3:].strip()    
    outward = pc[0:-3].strip()
    district = ''
    a = ''
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
        print(f'.. processed {rowsProcessed} postcode patterns')
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

def addPostCodeBreakdown(df) :

    global rowsProcessed

    # Look at each postcode and convert to a pattern, extract subcomponents of the postcode into separate columns
    # NB This takes a few minutes for the full set of rows.

    dfBreakdown = df.copy()
    rowsProcessed = 0
    print()
    print(f'.. generating extra postcode columns ..')
    extradf = dfBreakdown[['Postcode', 'Post Town', 'PostcodeArea']].apply(getPattern, axis=1, result_type='expand')      # NB This adds to df too
    print()
    print(f'.. concatenating extra postcode columns ..')
    df = pd.concat([dfBreakdown, extradf], axis='columns')
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

import nationalgrid as ng
def showGridSquare(df, sqName='TQ', plotter='CV2', savefilelocation=None) :
    sq = ng.dictGridSquares[sqName.upper()]        
    print(f'Sq name = {sq.name} : e = {sq.eastingIndex} : n = {sq.northingIndex} : mlength {sq.mLength}')
    print(f'{sq.getPrintGridString()}')
    
    if not sq.isRealSquare :
        print(f'Square {sq.name} is all sea ..')
    
    bl = (sq.eastingIndex * 100 * 1000, sq.northingIndex * 100 * 1000)
    tr = ((sq.eastingIndex + 1) * 100 * 1000, (sq.northingIndex+1) * 100 * 1000)
    print(f'bl = {bl} : tr = {tr}')

    if plotter == 'CV2' :
        img = cv2plotSpecific(df, title=sqName, canvas_h=800, density=1, bottom_l=bl, top_r=tr)
        if savefilelocation != None :
            filename = savefilelocation + '/' + 'postcodes.' + sqName + '.cv.png'
            writeImageArrayToFileUsingCV2(filename, img)
    else :
        tkplotSpecific(df, title=sqName, canvas_h=800, density=1, bottom_l=bl, top_r=tr)
    
def writeImageArrayToFileUsingCV2(filename, img) :
    from cv2 import cv2
    if cv2.imwrite(filename, convertToBGR(img)) :
        print(f'Image file saved as: {filename}')
    else :
        print(f'*** Failed to save image file as: {filename}')

def showAllGB(df, plotter='CV2', savefilelocation=None) :
    if plotter == 'CV2' :
        img = cv2plotSpecific(df, title='All GB', canvas_h=800, density=1)
        if savefilelocation != None :
            filename = savefilelocation + '/' + 'postcodes.allGB.cv.png'
            writeImageArrayToFileUsingCV2(filename, img)
    else :
        tkplotSpecific(df, title='All GB', canvas_h=800, density=10)

OSZipFile = r"./OSData/PostCodes/codepo_gb.zip"
tmpDir = os.path.dirname(OSZipFile) + '/tmp'
postcodeAreasFile = r"./OSData/PostCodes/postcode_district_area_lists.xls"

import argparse

def addStandardArgumentOptions(subparser, forCacheWrite=False) :
    subparser.add_argument('-v', '--verbose', action='store_true', help='Show some diagnostics')
    subparser.add_argument('-t', '--tempdir', 
                        help='Set the temporary directory location (used for unzipping data and as the default cache location)')

    fileMode = 'w' if forCacheWrite else 'r'
    subparser.add_argument('-c', '--cachefile', type=argparse.FileType(fileMode), help='Specify the location for the dataframe cache file')

def main() :

    # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_args
    parser = argparse.ArgumentParser(description='OS Code-Point Postcode data processing program')
    subparsers = parser.add_subparsers(help='sub-command help')

    subparser = subparsers.add_parser('generate', help='read OS data files to generate a cached dataframe for use with other commands')
    subparser.set_defaults(cmd='generate')
    addStandardArgumentOptions(subparser, forCacheWrite=True)

    subparser = subparsers.add_parser('df_info', help='Show info about the Pandas dataframe structure')
    subparser.set_defaults(cmd='df_info')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('stats', help='Produce stats and aggregates relating to the postcodes dataset')
    subparser.set_defaults(cmd='stats')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('to_csv', help='Produce a csv file containing the postcodes dataset')
    subparser.set_defaults(cmd='to_csv')
    subparser.add_argument('-o', '--outfile', type=argparse.FileType('w'), help='Specify the location for the output CSV file)')
    addStandardArgumentOptions(subparser)
    
    subparser = subparsers.add_parser('info', help='Display info about a specified postcode')
    subparser.add_argument('postcode', help='the postcode of interest, in quotes if it contains any spaces')
    subparser.set_defaults(cmd='info')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('plot', help='Plot a map around the specified postcode')
    addStandardArgumentOptions(subparser)
    subparser.add_argument('-p', '--plotter', choices=['CV2', 'TK'], default='CV2', help='Plot using CV2 (OpenCV) or TK')
    subparser.add_argument('place', help='Identifies the area to be plotted, in quotes if it contains any spaces')
    subparser.set_defaults(cmd='plot')

    parsed_args = parser.parse_args()
    #print(type(parsed_args))
    #print(parsed_args)
    d = vars(parsed_args)
    print(d)

    if not 'cmd' in d:
        parser.print_help()
        return 1
    elif parsed_args.cmd == 'generate' :
        print('generate .. command')
        # Handle verbose, alternative tmp/cache file location options
        df = regenerateDataFrame(OSZipFile, tmpDir, postcodeAreasFile)
        if not df.empty :
            writeCachedDataFrame(tmpDir, df)
        else :
            return 1
    else :
        # Need to retrieve cached data
        df = readCachedDataFrameFromFile(tmpDir)
        if df.empty :
            print()
            print('*** No dataframe read from cache')
            return 1

        plotwith = 'CV2'
        outputFileLocation = './pngs'

        if parsed_args.cmd == 'info' :
            print(f'info .. command for {parsed_args.postcode}')
            displayExample(df, example=parsed_args.postcode, plotter=plotwith, dimension=10)
        elif parsed_args.cmd == 'df_info' :
            print('Run df_info command ...')
            displayBasicInfo(df)
        elif parsed_args.cmd == 'stats' :
            # Could have a geog component.
            print('Run stats command ...')
            aggregate(df)
        elif parsed_args.cmd == 'to_csv' :
            print('Run to_csv command ...')
            saveAsCSV(df, tmpDir)
        elif parsed_args.cmd == 'plot' :
            print(f'Run plot command for "{parsed_args.place}" ..')
            # Work out if the place is a postcode, a grid square, <others?> or 'all'
            # Allow for spaces, and case. Further options:
            # county, post area, post district, district/ward/borough etc, ONS unit code ?
            # Or just a rectangle identified using NG top-left, bottom right/dimensions.
            # Can these overlap ? E.g. Post area = NG square (populated). Certainly town names can clash.
            # Plotter control, dimensions control, control of whether or not to save as png and where
            
            if parsed_args.place == 'all' :
                showAllGB(df, plotter=plotwith, savefilelocation=outputFileLocation)
            elif ng.checkGridSquareName(parsed_args.place) :
                showGridSquare(df, parsed_args.place, plotter=plotwith, savefilelocation=outputFileLocation)
            else :
                status = showPostcode(df, parsed_args.place, plotter=plotwith, savefilelocation=outputFileLocation)
            #else :
            #    # How best to handle this
            #    #print()
            #    #print(f'*** unrecognised place {parsed_args.area} to plot')
            #    return
            # ???? Handle arbitrary areas ?
        else :
            print(f'Unrecognised command: {parsed_args.cmd}')
            return 1

    return 0    # Not always

if __name__ == '__main__' :
    status = main()
    sys.exit(status)
