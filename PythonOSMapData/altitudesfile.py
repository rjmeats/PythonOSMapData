# Extract OS OpenData altitude data for a 10kmx10km grid square, identified by its Grid Reference,
# for example NY12
#
# The data is read from downloaded OS OpenData 'Terrain 50' files, which provide an altitude figure (m)
# every 50 metres. The original file is one big zip file, but this module assumes it has been unzipped
# to produce a set of folders, one per high level Grid Square (e.g. NY), with these folders containing
# a zip file per 10x10km square, and these zip files each containing an '.asc' file containing the 
# actual altitude data. 

import sys
import os
import io
import re
import zipfile
import numpy as np
import time

import nationalgrid as ng

# Reads the OS Data folders/zip files to extract the contents of the altitude data file for
# the specified grid square, returning it as a list of strings, one per line in the file.
# An empty list is returned where the data file is missing - this indicates that the entire grid square
# is in the sea.
# Returns None if there are any problems detected (invalid grid square or file access problems)
def readTarget10x10SquareData(target10x10Square) :

    # Is this a valid square 10x10 square identifier ?
    sqInfo = ng.check10x10SquareName(target10x10Square)
    if sqInfo == None :
        print("*** invalid 10x10km grid square name:", target10x10Square, file=sys.stderr)
        return None

    # The unzipped OS data file produces folders and files which use the target squre name as
    # both upper and lower case, and also via the enclosing 100x100 grid square name
    targetSquareLower = sqInfo['name'].lower()              # E.g. ny12
    targetSquareUpper = sqInfo['name'].upper()              # E.g. NY12
    gridSquareLower = sqInfo['gridsquare'].lower()          # E.g. ny

    # The original zip file provided by OS Open Data is called terr50_gagg_gb.zip. We expect
    # this to have already been unzipped under OSData/Altitudes, resulting in a terr50_gagg_gb
    # folder, containing numerous sub-folders named after 100x100km grid squares, e.g. 'ny'.
    baseFolder = r"./OSData/Altitudes/terr50_gagg_gb/data"
    if not os.path.isdir(baseFolder) :
        print("*** No base folder for data files found:", baseFolder, file=sys.stderr)
        return None

    targetFolder = baseFolder + "/" + gridSquareLower
    if not os.path.isdir(targetFolder) :
        # Valid grid square but no specific data folder means this main grid square is sea-based
        return []

    # Look in targetFolder for a zip file with the expected name format xxnn_OST50*.zip, based on the
    # 10x10km grid square name, e.g. ny12_*.zip. NB Actual files names have a date as part of the
    # '*' part, e.g. ny12_OST50GRID_20180619.zip, so we can't predict the full name, just its
    # start and end.

    matchingFilenames = []
    for entry in os.scandir(targetFolder) :
        if entry.is_file() and entry.name.startswith(targetSquareLower) and entry.name.endswith(".zip"):
            matchingFilenames.append(entry.name)

    if len(matchingFilenames) == 0 :
        # No data for this 10x10 square, indicating its in the sea.
        return []
    elif len(matchingFilenames) > 1 :
        # Don't expect this
        print("*** Multiple zipped data files found for grid square:", matchingFilenames, file=sys.stderr)
        return None

    # We've found the expected zip file for our 10x10 square. We expect it to contain our
    # 'asc' data file, named after the 10x10 grid square, e.g. NY12.asc
    zipfilename = targetFolder + "/" + matchingFilenames[0]
    expectedDataFile = targetSquareUpper + ".asc"

    # Use the Python zipfile library to handle the extraction
    try :
        z = zipfile.ZipFile(zipfilename, mode="r")
        # Check the expected file exists - getinfo throws an expection if not
        zi = z.getinfo(expectedDataFile)        
        # Read from the file, using the io library to treat it as a text stream, and
        # put the lines read into a list of strings
        with io.TextIOWrapper(z.open(expectedDataFile, mode="r")) as f:
            lines = f.readlines()
    except :
        print("*** Didn't find/read the expected data file", expectedDataFile, "in zip file", zipfilename, file=sys.stderr)
        return None

    return lines


# Processes the lines in an OS Open Data file (as read by the readTarget10x10SquareData function), and converts them
# into a 2-D numpy array of altitude values for the 10x10km.
#
# The function returns the following:
# - dictionary of data from the file header (and the square name passed in)
# - 2-D array of altitude points, at intervals 
# - minimum and maximum altitudes found in the square
#
# If the file is not in the expected format, the function returns None
#
#                    xa, minAltitude, maxAltitude, header = readASCfile(lines)
#
# The OS asc file format consists of 5 header lines, e.g.
#
# ncols 200                             [expect 200 altitude values per line]
# nrows 200                             [expect 200 lines]
# xllcorner 320000                      [6 digit (i.e. metres) easting of the lower-left (i.e. south-west) corner]
# yllcorner 520000                      [6 digit (i.e. metres) northing of the lower-left (i.e. south-west) corner]
# cellsize 50                           [metres between points. Square is 10kmx10km, ncols*cellsize = 10km]
# 
# then 'nrows' lines of data, each having 'ncols' altitude (in metres) values separated by spaces, e.g.
# 
# 317.2 324.9 332.7 334.7 ... 
# #
# Order of columns and rows is as if looking at grid on a normal map with N at top, so the bottom line bottom left corner value is
# for the (xllcorner, yllcorner) point ie the sout-west corner, with points to the right and above separated by 'cellsize' intervals.

def processFileContents(squareName, lines) :
    
    ##
    ## Header processing 
    ##

    # Expect at least 5 lines, to cover the header
    headerFieldNames = ['ncols', 'nrows', 'xllcorner', 'yllcorner', 'cellsize']
    if len(lines) < len(headerFieldNames) :
        print("*** Unexpected file format: too few lines for square", squareName, file=sys.stderr)
        return None

    # Process the header lines, storing values in a dictionary. Expect each line to have the format:
    # <field name> <intvalue>
    headerDict = { 'name' : squareName.upper() }
    for lineNo in range(len(headerFieldNames)) :
        lineSplit = lines[lineNo].split()
        if len(lineSplit) != 2 :
            print("*** Unexpected header line format for square", squareName, ", line:", lineNo+1, file=sys.stderr)
            return None
        fieldName, fieldValue = lineSplit[0], lineSplit[1]
        try :
            intFieldValue = int(fieldValue)
        except :
            print("*** Failed to convert header value to integer for square", squareName, ", field:", fieldName, ", value:", fieldValue, file=sys.stderr)
            return None
        headerDict[fieldName] = intFieldValue
    
    # Are all the expected fields present in the header ?
    for fn in headerFieldNames :
        if fn not in headerDict :
            print("*** No", fn, "field in header for square", squareName, file=sys.stderr)
            return None
    
    ## 
    ## Data lines processing
    ##

    # Now process the data lines - we expect nrows of them, each with ncols values
    ncols = headerDict['ncols']
    nrows = headerDict['nrows']
    dataLines = lines[len(headerFieldNames):]
    if len(dataLines) != nrows :
        print("*** Unexpected number of data lines for square:", squareName, "-", len(dataLines), "instead of", nrows, file=sys.stderr)
        return None

    # Store the values in a numpy 2-D array. The file presents the rows ordered with north at the top, so the first line in the
    # file is for the most northerly. We want our numpy array to have [row0,col0] as the south-west corner, so we have to index
    # the array by the row number in the file counting up from the bottom.
    a = np.empty([nrows, ncols], dtype=float)

    for dataRowNo in range(nrows) :
        rowIndex = nrows-dataRowNo-1        # So that [0,0] relates to the south-west corner of this square
        lineSplit = dataLines[dataRowNo].split()
        if len(lineSplit) != ncols:
            print("*** Unexpected number of data values for data line:", dataRowNo+1, "in square:", squareName,
                        "-", len(lineSplit), "instead of", ncols, file=sys.stderr)
            return None
        
        # Convert each value to a float and store it in the array
        try :
            for dataColNo in range(ncols) :
                a[rowIndex, dataColNo] = float(lineSplit[dataColNo])
        except :
            print("*** Failed to convert a data value to a float on data line:", dataRowNo+1, "in square:", squareName, "-", lineSplit[dataColNo], file=sys.stderr)
            return None

    return headerDict, a

####################################
# Standalone main
####################################

if __name__ == "__main__" :
    targetSquareArg = sys.argv[1]
    print("Target grid square:", targetSquareArg)

    startTime = time.time()
    lines = readTarget10x10SquareData(targetSquareArg)
    print("Read data in", round(time.time() - startTime, 6), "seconds")

    if lines == None :
        print("*** Problem extracting data", file=sys.stderr)
    elif len(lines) == 0 :
        print("No data for this grid square => in the sea")        
    elif len(lines) < 5 :
        print("*** Unexpected number of data lines:", len(lines))
        print(lines)        
    else :
        print(len(lines), "lines in uncompressed file")
        print("Header:", lines[0:4])

        if lines != None :
            contents = processFileContents(targetSquareArg, lines)
            if contents != None :
                header, aData = contents
                print(header)
                print()
                print(aData[::-1,:])
                import altitudeRGB
                import altitudestats
                colourScheme = altitudeRGB.getColourScheme("standard")
                altitudestats.analyseAltitudes(colourScheme, targetSquareArg, aData)
            else :
                print("Problem processing file contents", file=sys.stderr)

