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

        totalCodes = 0
        fileCount = 0
        combined_df = pd.DataFrame()
        for filename in matchingFilenames :
            fullfilename = maindatafolder + filename
            fileCount += 1
            df = pd.read_csv(fullfilename, header=None)
            combined_df = combined_df.append(df)
            (numrows, numcols) = df.shape
            if numcols != 10 :
                print(f'*** Unexpected number of columns ({numcols}) in CSV file {filename}', file=sys.stderr)
                return
            totalCodes += numrows
            if fileCount % 10 == 0 : print(f'{filename} : {numrows} codes : {combined_df.shape[0]} total rows')

        (numrows, numcols) = combined_df.shape
        print(f'.. found {combined_df.shape[0]} post codes')
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