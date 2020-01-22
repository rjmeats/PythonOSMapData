import os
import sys

import zipfile
import pandas as pd

"""
https://en.wikipedia.org/wiki/List_of_postcode_districts_in_the_United_Kingdom
https://en.wikipedia.org/wiki/List_of_postcode_areas_in_the_United_Kingdom
https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-november-2019
"""

def unpackZipFile(baseFile, tmpDir, detail=False) :
    z = zipfile.ZipFile(baseFile, mode='r')

    zinfolist = z.infolist()
    for zinfo in zinfolist :
        if zinfo.filename.startswith('Data/') or zinfo.filename.startswith('Doc/') :
            if detail: print(zinfo.filename)
        else :
            print("*** Unexpected extract location found:", zinfo.filename, file=sys.stderr)

    print(f'Extracting zip file {baseFile} under {tmpDir} ...')
    z.extractall(path=tmpDir)
    z.close()

CSVcolumnNames1 = ['PC', 'PQ', 'EA', 'NO', 'CY', 'RH', 'LH', 'CC', 'DC', 'WC']
CSVcolumnNames2 = [ 'Postcode', 
                'Positional_quality_indicator', 'Eastings', 'Northings', 'Country_code', 
                'NHS_regional_HA_code', 'NHS_HA_code', 
                'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']

renamedDFColumns = {'Positional_quality_indicator' :'Quality'}
# Re-order columns and drop NHS ones for the output dataframe
outputDFColumnNames = [ 'Postcode', 'PostcodeArea', 
                'Quality', 'Eastings', 'Northings', 'Country_code', 
                'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']

def listsOfStringsMatch(l1, l2) :
    if(len(l1) != len(l2)) : return False
    for i in range(len(l1)) :
        if l1[i].strip() != l2[i].strip() : return False
    return True

def checkColumns(tmpDir, detail=False) :
    """ Check that the file defining column headers contains what we expect. """
    columnsOK = False
    columnsFile = tmpDir + '/Doc/Code-Point_Open_Column_Headers.csv'
    if not os.path.isfile(columnsFile) :
        print(f"*** No columns definition file found at {columnsFile}", file=sys.stderr)
        columnsOK = False
    else :
        with open(columnsFile, 'r', ) as f:
            line1 = f.readline().strip()
            line1list = line1.split(sep=',')
            line2 = f.readline().strip()
            line2list = line2.split(sep=',')
            if not listsOfStringsMatch(CSVcolumnNames1, line1list) :
                columnsOK = False
                print(f"*** Columns definition file line 1 not as expected: {line1}", file=sys.stderr)
            elif not listsOfStringsMatch(CSVcolumnNames2, line2list) :
                columnsOK = False
                print(f"*** Columns definition file line 2 not as expected: {line2}", file=sys.stderr)
            else :
                columnsOK = True
                print(f"Columns definition file has expected columns")

    return columnsOK

def processFiles(tmpDir, detail=False) :
    if not checkColumns(tmpDir, detail) :
        return pd.DataFrame()

    maindatafolder = tmpDir + '/Data/CSV/'
    if not os.path.isdir(maindatafolder) :
        print(f'*** No /Data/CSV/ folder found at: {maindatafolder}', file=sys.stderr)
        return pd.DataFrame()

    matchingFilenames = [entry.name for entry in os.scandir(maindatafolder) if entry.is_file() and entry.name.endswith('.csv')]
    print(f'Found {len(matchingFilenames)} CSV files to process ...')

    # Joining multiple CSVs together by reading one by one then copy/appending to a target DataFrame. Very slow.
    # https://stackoverflow.com/questions/20906474/import-multiple-csv-files-into-pandas-and-concatenate-into-one-dataframe
    # has some alternatives that may be faster.
    # Also https://engineering.hexacta.com/pandas-by-example-columns-547696ff78dd
    totalCodes = 0
    dfList = []
    for (fileCount, filename) in enumerate(matchingFilenames, start=1) :
        #if fileCount > 30 : break
        fullfilename = maindatafolder + filename
        postcodeArea = filename.replace('.csv', '').upper()

        df = pd.read_csv(fullfilename, header=None, names=CSVcolumnNames2)   \
                .rename(columns=renamedDFColumns) \
                .assign(PostcodeArea=postcodeArea)[outputDFColumnNames]
        (numrows, numcols) = df.shape
        if numcols != len(outputDFColumnNames) :
            print(f'*** Unexpected number of columns ({numcols}) in CSV file {filename}', file=sys.stderr)
            print(df.info())
            print(df.head())
            return
        totalCodes += numrows
        # This combination line takes ~34 out of ~40 seconds of the processing time of this function. Much quicker to use concat at end
        # combined_df = combined_df.append(df, ignore_index=True)    # NB Need to avoid copy index values in, to avoid repeats.
        dfList.append(df)
        if fileCount % 10 == 0 : print(f'.. {fileCount:3d} files : {filename:>6.6s} {postcodeArea:>2.2s}: {numrows:5d} post codes : {totalCodes:7d} total rows ..')

    # Much faster to concatenate full set of collected dataframes than appending one-by-one
    combined_df = pd.concat(dfList, ignore_index=True)
    print(f'.. found {combined_df.shape[0]} post codes in {len(matchingFilenames)} CSV files')
    return combined_df

def main(args) :
    baseFile = r"./OSData/PostCodes/codepo_gb.zip"
    tmpDir = os.path.dirname(baseFile) + '/tmp'

    if not os.path.isfile(baseFile) :
        print("*** No base file found:", baseFile, file=sys.stderr)
        return

    unpackZipFile(baseFile, tmpDir)

    startTime = pd.Timestamp.now()
    df = processFiles(tmpDir)
    took = pd.Timestamp.now()-startTime
    if df.empty :
        return

    print(f'Took {took.total_seconds()} seconds to load data files into a data frame')
    print()
    print(df.info())
    print(df.head())
    print(df.tail())

if __name__ == '__main__' :
    main(sys.argv)

