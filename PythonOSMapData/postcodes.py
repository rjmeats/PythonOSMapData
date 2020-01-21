import os
import sys

import zipfile
import pandas as pd

/**
https://en.wikipedia.org/wiki/List_of_postcode_districts_in_the_United_Kingdom
https://en.wikipedia.org/wiki/List_of_postcode_areas_in_the_United_Kingdom
https://geoportal.statistics.gov.uk/datasets/ons-postcode-directory-november-2019
**/

def unpackZipFile(baseFile) :
    z = zipfile.ZipFile(baseFile, mode='r')

    zinfolist = z.infolist()
    for zinfo in zinfolist :
        if zinfo.filename.startswith('Data/') or zinfo.filename.startswith('Doc/') :
            print(zinfo.filename)
        else :
            print("*** Unexpected extract location found:", zinfo.filename, file=sys.stderr)

    tmpDir = os.path.dirname(baseFile) + '/tmp'
    print('Extracting zip file under ', tmpDir)
    z.extractall(path=tmpDir)

    maindatafolder = tmpDir + "/Data/CSV/"
    if os.path.isdir(maindatafolder) :
        matchingFilenames = []
        for entry in os.scandir(maindatafolder) :
            if entry.is_file() and entry.name.endswith(".csv"):
                matchingFilenames.append(entry.name)
        print("Found ", len(matchingFilenames), " CSV files to process")

        totalCodes = 0
        for filename in matchingFilenames :
            fullfilename = maindatafolder + filename
            df = pd.read_csv(fullfilename, header=None)
            (numrows, numcols) = df.shape
            if numcols != 10 :
                print("*** Unexpected number of columns:", numcols, file=sys.stderr)
            totalCodes += numrows
            print(filename, numrows)

        print('Found', totalCodes, " codes")
    else :
        print("*** No /Data/CSV/ folder found at:", maindatafolder, file=sys.stderr)

    z.close()

if __name__ == '__main__' :
    baseFile = r"./OSData/PostCodes/codepo_gb.zip"
    if not os.path.isfile(baseFile) :
        print("*** No base file found:", baseFile, file=sys.stderr)
    else :
        unpackZipFile(baseFile)