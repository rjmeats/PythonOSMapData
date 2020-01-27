import os
import sys

import zipfile
import pandas as pd

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

from tkinter import Tk, Canvas, mainloop

def tk(df) :
    master = Tk()

    canvas_width = 800
    canvas_height = 400
    w = Canvas(master, 
            width=canvas_width,
            height=canvas_height)
    w.pack()

    y = int(canvas_height / 2)
    #w.create_line(0, y, canvas_width, y)

    for x in range(0, 100, 4) :
        for y in range(0, 100, 4) :
            w.create_oval(x,y,x,y, fill="red", outline="#fb0", width=1)

    mainloop()

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

    print()
    print(dfDistrictCodes)

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

    print()
    print(dfWardCodes)

    # Note the spreadsheet has a few duplicates in the UTE sheet  ????
    # Tintagel ED	E05009271
    # Tintagel ED (DET)	E05009271
    # In each case there is a (DET) version as well as a 'normal' one. Not sure why, but can perhaps just
    # ignore 'DET' cases 
    # can use 'match' and/or 'contains' somehow ?
    # https://www.geeksforgeeks.org/get-all-rows-in-a-pandas-dataframe-containing-given-substring/

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

def checkCodesReferentialIntegrity(df, dfLookup, parameters) :

    (context, mainDataFramePKColumn, mainDataFrameCodeJoinColumn, lookupTableCodeColumn, lookupTableValueColumn) = parameters

    print()
    print(f'Checking {context} ...')

    # Check lookup for unique keys
    uniquenessOK = checkAllUniqueValues(context, dfLookup, lookupTableCodeColumn)
    if not uniquenessOK :
        return

    print(f'... {lookupTableCodeColumn} values are unique in lookup table ...')

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
        print()
        for index, row in dfUnusedLookup.iterrows() :
            print(f'  - {row.values[0]} : {row.values[1]}')


    # Outer join the main table and lookup table in the other direction to find referential integrity issues for 
    # column values in the main table with no matching value in the lookup table. There will probably be multiple
    # records having the same missing lookup value, so we need to do some grouping before reporting at an aggregate
    # level.
    dfJoin = pd.merge(df, dfLookup, left_on=mainDataFrameCodeJoinColumn, right_on=lookupTableCodeColumn, how='left')
    dfLookupNotFound = dfJoin[ dfJoin[lookupTableCodeColumn].isnull() ] [[mainDataFramePKColumn, mainDataFrameCodeJoinColumn]]

    lookupsNotFoundCount = dfLookupNotFound.shape[0]
    if lookupsNotFoundCount == 0 :
        print()
        print(f'... all rows in the main table {mainDataFrameCodeJoinColumn} column have a '
                    f'lookup value in the {lookupTableCodeColumn} column of the domain table')
    else :
        print()
        print(f'*** ... {lookupsNotFoundCount} row{"" if lookupsNotFoundCount == 1 else "s"} in the main table have '
                    f'no lookup value in the {lookupTableCodeColumn} column of the domain table ...')

        dfLookupNotFoundGrouped = dfLookupNotFound.groupby(mainDataFrameCodeJoinColumn, as_index=False).count()

        print(f'*** ... {dfLookupNotFoundGrouped.shape[0]} distinct code value{"" if lookupsNotFoundCount == 1 else "s"} unmatched:')
        print()
        for index, row in dfLookupNotFoundGrouped.iterrows() :
            print(f'  *** {row.values[0]} : {row.values[1]}')

    if 1==1 :
        return

    # Produce a detail group-by breakdown summary ????
    # And now do a final join ????

def main(args) :
    OSZipFile = r"./OSData/PostCodes/codepo_gb.zip"
    tmpDir = os.path.dirname(OSZipFile) + '/tmp'
    postcodeAreasFile = r"./OSData/PostCodes/postcode_district_area_lists.xls"

    if not os.path.isfile(OSZipFile) :
        print("*** No OS zip file found:", OSZipFile, file=sys.stderr)
        return

    if not os.path.isfile(postcodeAreasFile) :
        print("*** No ONS postcode areas spreadsheet file found:", postcodeAreasFile, file=sys.stderr)
        return

    dfAreas = loadPostcodeAreasFile(postcodeAreasFile)
    if dfAreas.empty :
        return

    dfCountries = loadCountryCodes()
    if dfCountries.empty :
        return

    dfCounties = loadCountyCodes(tmpDir)
    if dfCounties.empty :
        return

    dfDistricts = loadDistrictCodes(tmpDir)
    if dfDistricts.empty :
        return

    dfWards = loadWardCodes(tmpDir)
    if dfWards.empty :
        return

    unpackZipFile(OSZipFile, tmpDir)

    startTime = pd.Timestamp.now()
    df = loadFilesIntoDataFrame(tmpDir)
    took = pd.Timestamp.now()-startTime
    if not df.empty :
        print(f'Took {took.total_seconds()} seconds to load data files into a dataframe')
    else :
        return

    countriesParameters = ('Country Codes', 'Postcode', 'Country_code', 'Country Code', 'Country Name')
    checkCodesReferentialIntegrity(df, dfCountries, countriesParameters)

    areasParameters = ('Postcode Area Codes', 'Postcode', 'PostcodeArea', 'Postcode Area','Post Town')
    checkCodesReferentialIntegrity(df, dfAreas, areasParameters)

    countiesParameters = ('County Codes', 'Postcode', 'Admin_county_code', 'County Code','County Name')
    checkCodesReferentialIntegrity(df, dfCounties, countiesParameters)

    districtsParameters = ('District Codes', 'Postcode', 'Admin_district_code', 'District Code','District Name')
    checkCodesReferentialIntegrity(df, dfDistricts, districtsParameters)

    wardsParameters = ('Ward Codes', 'Postcode', 'Admin_ward_code', 'Ward Code','Ward Name')
    checkCodesReferentialIntegrity(df, dfWards, wardsParameters)

    # 1 million records in main table have no county code lookup = Nan ? And some other codes too ????

    #displayBasicInfo(df)
    #aggregate(df)

    #tk(df)

if __name__ == '__main__' :
    main(sys.argv)

