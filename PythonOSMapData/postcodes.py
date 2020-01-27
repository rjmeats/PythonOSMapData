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

    # Make Postcode the index instead of the auto-assigned range. As part of this, check there are no duplicate postcodes.
    try :
        combined_df = combined_df.set_index('Postcode', verify_integrity=True)
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

def checkCountryCodes(df) :
    countryCodeDict = {
        'E92000001' : 'England',
        'S92000003' : 'Scotland',
        'W92000004' : 'Wales',
        'N92000002' : 'N. Ireland'
    }
    dfCountryCodes = pd.DataFrame({ 'code' : list(countryCodeDict.keys()), 'value' : list(countryCodeDict.values()) })
    print(dfCountryCodes)

    dfFirst10 = df[0:10]
    print(dfFirst10)
    dfJoin = pd.merge(df, dfCountryCodes, left_on='Country_code', right_on='code', how='left')
    print(dfJoin)
    print(dfJoin.groupby(['Country_code','value']).count())

    # Group by 'value' counts by Country code value - E, S, W
    # but counts depend on a non Nan value - only index 'Postcode' has this (can we confirm this ?). So what is the 
    # best way to count this ?
    # Also, the join produces extra columns called 'code' and 'value' on the end - not great names, and 'code' is just
    # a repeat of Country_code.
    # How does the above work if reading in e.g. county-codes from XLS (into a dataframe directly - what does that look like ?d)

def checkAreaCodes(df, dfAreas) :

    print()
    print('Checking area codes ...')
    print()
    dfFirst10 = df[0:10]
    print(dfFirst10)
    dfJoin = pd.merge(df, dfAreas, left_on='PostcodeArea', right_on='Postcode Area', how='left')
    print()
    print('Initial join')
    print()
    print(dfJoin)
    print()
    # Replace NaN values with '??' in the join-related columns, for ease of grouping by missing values.
    dfJoin.fillna({'Postcode Area' : '??', 'Post Town' : '??', 'PostcodeArea' : '??'}, inplace=True)
    print()
    print('Filled join')
    print()
    print(dfJoin)
    print()
    # Post Code/Town is defined, but no actual postcodes. Outer join just produces one row, so no
    # real need to group
    print(dfJoin[dfJoin['PostcodeArea']  == '??'])      
    print()

    # PostcodeArea from xx.csv is not in the spreadsheet, so can't assign a Post Code Town. If there are any cases,
    # will be 100s of them for that xx, so use grouping.
    dfG = dfJoin[dfJoin['Postcode Area']  == '??'].groupby([dfJoin['PostcodeArea'],dfJoin['Postcode Area'],dfJoin['Post Town']]).count()

    print()
    print('Grouped by Postcode area fields - no Post Town lookup:')
    print()
    print(dfG)
    print()

    dfG = dfJoin[dfJoin['Postcode Area']  != '??'].groupby([dfJoin['PostcodeArea'],dfJoin['Postcode Area'],dfJoin['Post Town']]).count()

    print()
    print('Grouped by Postcode area fields - Post Town lookup exists:')
    print()
    print(dfG)
    print()

    # Find info about areas in main data not in lookup
    # Eek numpy/pandas issue https://stackoverflow.com/questions/40659212/futurewarning-elementwise-comparison-failed-returning-scalar-but-in-the-futur
    # May be because the groupby dataframe is a different type of data structure, so its columns are int offsets into the source ?
    # See https://realpython.com/pandas-groupby/
    # If so would explain the comparison issues.
    # https://pandas.pydata.org/pandas-docs/stable/reference/groupby.html
    # 'Having' clause equivalent ?
#    print(dfG.info())
    #dfNoLookup = dfG['PostcodeArea'] == '??'
    
#    print('No lookup:')
#    print(dfNoLookup)
    #print(dfG[dfNoLookup])

    # Probably need to do filtering on joined df, with na's replaced, then do grouping on result ?

    # Other consideration is checking that post town codes (and other lookups) are unique in source data.




def main(args) :
    OSZipFile = r"./OSData/PostCodes/codepo_gb.zip"
    postcodeAreasFile = r"./OSData/PostCodes/postcode_district_area_lists_x.xls"
    tmpDir = os.path.dirname(OSZipFile) + '/tmp'

    if not os.path.isfile(OSZipFile) :
        print("*** No OS zip file found:", OSZipFile, file=sys.stderr)
        return

    if not os.path.isfile(postcodeAreasFile) :
        print("*** No ONS postcode areas spreadsheet file found:", postcodeAreasFile, file=sys.stderr)
        return

    unpackZipFile(OSZipFile, tmpDir)

    startTime = pd.Timestamp.now()
    df = loadFilesIntoDataFrame(tmpDir)
    took = pd.Timestamp.now()-startTime
    if not df.empty :
        print(f'Took {took.total_seconds()} seconds to load data files into a dataframe')
    else :
        return

    dfAreas = loadPostcodeAreasFile(postcodeAreasFile)
    if dfAreas.empty :
        return

    #displayBasicInfo(df)
    #aggregate(df)

    # checkCountryCodes(df)
    checkAreaCodes(df, dfAreas)

    #tk(df)

if __name__ == '__main__' :
    main(sys.argv)

