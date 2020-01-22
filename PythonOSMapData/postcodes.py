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

CSVcolumnNames = ['PC', 'PQ', 'EA', 'NO', 'CY', 'RH', 'LH', 'CC', 'DC', 'WC']
CSVcolumnNames = [ 'Postcode', 
                'Quality', 'Eastings', 'Northings', 'Country_code', 
                'NHS_regional_HA_code', 'NHS_HA_code', 
                'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']
# Re-order columns and drop NHS ones for the output dataframe
outputDFColumnNames = [ 'Postcode', 'PostcodeArea', 
                'Quality', 'Eastings', 'Northings', 'Country_code', 
                'Admin_county_code', 'Admin_district_code', 'Admin_ward_code']

def processFiles(tmpDir, detail=False) :
    maindatafolder = tmpDir + '/Data/CSV/'
    if os.path.isdir(maindatafolder) :
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

            df = pd.read_csv(fullfilename, header=None, names=CSVcolumnNames).assign(PostcodeArea=postcodeArea)[outputDFColumnNames]
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

        # Much faster than appending one-by-one
        combined_df = pd.concat(dfList, ignore_index=True)

        print(f'.. found {combined_df.shape[0]} post codes in {len(matchingFilenames)} CSV files')
        return combined_df
    else :
        print(f'*** No /Data/CSV/ folder found at: {maindatafolder}', file=sys.stderr)
        return None

if __name__ == '__main__' :
    baseFile = r"./OSData/PostCodes/codepo_gb.zip"
    tmpDir = os.path.dirname(baseFile) + '/tmp'

    if not os.path.isfile(baseFile) :
        print("*** No base file found:", baseFile, file=sys.stderr)
    else :
        unpackZipFile(baseFile, tmpDir)

        startTime = pd.Timestamp.now()
        df = processFiles(tmpDir)
        took = pd.Timestamp.now()-startTime
        print(f'Took {took.total_seconds()} seconds to load data files into a data frame')
        
        print()
        print(df.info())
        print(df.head())
        print(df.tail())
