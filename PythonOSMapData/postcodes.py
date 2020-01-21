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

    print(f'Extracting zip file under {tmpDir} ...')
    z.extractall(path=tmpDir)
    z.close()

def processFiles(tmpDir, detail=False) :
    maindatafolder = tmpDir + '/Data/CSV/'
    if os.path.isdir(maindatafolder) :
        matchingFilenames = []
        for entry in os.scandir(maindatafolder) :
            if entry.is_file() and entry.name.endswith('.csv'):
                matchingFilenames.append(entry.name)
        print(f'Found {len(matchingFilenames)} CSV files to process ...')

        # Joining multiple CSVs together by reading one by one then copy/appending to a target DataFrame. Very slow.
        # https://stackoverflow.com/questions/20906474/import-multiple-csv-files-into-pandas-and-concatenate-into-one-dataframe
        # has some alternatives that may be faster.
        # Also https://engineering.hexacta.com/pandas-by-example-columns-547696ff78dd
        totalCodes = 0
        combined_df = pd.DataFrame()
        for (fileCount, filename) in enumerate(matchingFilenames, start=1) :
            # if fileCount > 30 : break
            fullfilename = maindatafolder + filename
            postcodeArea = filename.replace('.csv', '').upper()

            columnNames = ['PC', 'PQ', 'EA', 'NO', 'CY', 'RH', 'LH', 'CC', 'DC', 'WC']
            columnNames = [ 'Postcode', 
                            'Quality', 'Eastings', 'Northings', 'Country_code', 
                            'NHS_regional_HA_code', 'NHS_HA_code', 
                            'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']
            df = pd.read_csv(fullfilename, header=None, names=columnNames).assign(PostcodeArea=postcodeArea)
            # Re-order columns and drop NHS ones
            reorderedColumnNames = [ 'PostcodeArea', 'Postcode', 
                            'Quality', 'Eastings', 'Northings', 'Country_code', 
                            'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']
            df = df[reorderedColumnNames]

            #df = pd.read_csv(fullfilename, header=None).insert(loc=0, column='PostcodeArea', value=postcodeArea) # Doesn't work - None returned
            combined_df = combined_df.append(df, ignore_index=True)    # NB Need to avoid copy index values in, to avoid repeats.
            (numrows, numcols) = df.shape
            if numcols != 10+1-2 :
                print(f'*** Unexpected number of columns ({numcols}) in CSV file {filename}', file=sys.stderr)
                print(df.info())
                print(df.head())
                return
            totalCodes += numrows
            if fileCount % 10 == 0 : print(f'.. {fileCount:3d} files : {filename:>6.6s} {postcodeArea:>2.2s}: {numrows:5d} post codes : {combined_df.shape[0]:7d} total rows ..')

        (numrows, numcols) = combined_df.shape
        print(f'.. found {combined_df.shape[0]} post codes in {len(matchingFilenames)} CSV files')

        print(combined_df.info())
        print(combined_df.head())
        print(combined_df.tail())
    else :
        print(f'*** No /Data/CSV/ folder found at: {maindatafolder}', file=sys.stderr)

if __name__ == '__main__' :
    baseFile = r"./OSData/PostCodes/codepo_gb.zip"
    tmpDir = os.path.dirname(baseFile) + '/tmp'

    if not os.path.isfile(baseFile) :
        print("*** No base file found:", baseFile, file=sys.stderr)
    else :
        unpackZipFile(baseFile, tmpDir)
        processFiles(tmpDir)