import sys
import os
import re
import numpy as np
import matplotlib.pyplot as plt

import time

import altitudesfile
import nationalgrid as ng
import altitudeassessment
import altitudeRGB
import altitudestats

# ###########################################################
# Note on rows, columns and array indexing in numpy:
#
# numpy 2-D arrays treat the first dimension as 'rows' and the second as 'columns', with the origin [0,0] being in
# the top-left corner when the array is printed. E.g. the output from this code:
#
#   rows,cols=3,2
#   example = np.empty((rows, cols), dtype=object)
#   for r in range(0, rows) :
#       for c in range(0, cols) :
#           example[r, c] = "r{0:d}, c{1:d}".format(r, c)
#   print(example)
#
# is:
#
#    [['r0, c0' 'r0, c1']
#     ['r1, c0' 'r1, c1']
#     ['r2, c0' 'r2, c1']]
#
# This program populates its 2-D arrays as follows:
# - the first dimension (rows) relates to the northing component of the map
# - the second dimension (columns) relates to the easting
# - and the element [0,0] relates to the south-west corner of the map
# 
# Note that when printing out the numpy array via 'print(a)', the output is produced in row order, starting
# from row 0, so in terms of our map, the output is reflected north-to-south. To print out the value as they
# would relate to the normal map representation, with the south-west at bottom left, use the syntax:
# 'print(a[::-1,:])'  i.e. reverse the first dimension via '::-1', and leave the second dimension as is via ':'.
#
# ###########################################################
    
# Parameters indicate the area to be processed, in terms of:
# - the name of a 100x100 or 10x10 square to form the south-west corner
# - the number of squares east and north of this to also include, as a string "nnxmm"
def main(basisSquareName, dimensions, colourSchemeName, displayPlot=True, savePlot=True) :

    # When dumping out numpy arrays for diagnostics, use a wider screen than the default (only 75 chars)
    np.set_printoptions(linewidth=200)

    # Work out what area we are going to map, in terms of 10x10km squares (the unit size of the OS Open Data
    # altitude files).
    areaToMap = areaToMapFromParameters(basisSquareName, dimensions)
    if areaToMap == None :
        # Bad parameters
        return None
    else :
        # The return is a tuple containing the south-west corner square, and the number of 10x10km squares east and north
        # of this to include to make the overall map rectangle/square
        sw10x10CornerSquare, ew10x10Dimension, ns10x10Dimension = areaToMap

    print("Processing ", basisSquareName, dimensions, " => ", sw10x10CornerSquare, ew10x10Dimension, ns10x10Dimension)
    startTime = time.time()

    # Create a 2-D array, with an element for each 10x10km square in our map, which we will fill with basic info
    # about the identity of each square, allowing the data for each square to be fetched in turn later.
    a10x10SquareNames = np.empty((ns10x10Dimension, ew10x10Dimension), dtype=object)

    for eastOffset in range(ew10x10Dimension) :
        for northOffset in range(ns10x10Dimension) :
            # Find out what the ID of the 10x10km square at these offsets from our south-west corner cell.
            # NB this can return None if we've gone off the edge of the OS national grid
            squareReached = ng.get10x10SquareReached(sw10x10CornerSquare, eastOffset, northOffset)            
            a10x10SquareNames[northOffset, eastOffset] = squareReached['name'] if squareReached != None else None

    print("Covering these 10x10km squares:")
    print(a10x10SquareNames[::-1,:])        # Print with the first dimension (S-N) reversed so [0,0] is at the bottom of the output

    # Read the detailed altitude data points for the 10x10km squares, and populate one large array with this data. Also produce an array with
    # processing status info for each 10x10km squares (shadowing the a10x10SquareNames array) and a dictionary with some overall info about
    # the squares and data points.

    summaryDict, aAllAltitudes, aStatus = readDetailedDataPoints(a10x10SquareNames)

    if summaryDict['status'] == "ok" :
        print("Read detailed data OK, taking", round(time.time() - startTime, 3), " seconds")
        print(aStatus[::-1,:])          # Print with the first dimension (S-N) reversed so [0,0] is at the bottom of the output
        print()
        print(aAllAltitudes[::-1,:])               # Print with the first dimension (S-N) reversed so [0,0] is at the bottom of the output
        startTime = time.time()
        print("Starting altitude processing ...")
        s = altitudeassessment.sameAltitudeGrid(aAllAltitudes, aStatus)    
        print("Altitude processing took:", round(time.time() - startTime, 3), "seconds")

        title = basisSquareName.upper() + " " + dimensions
        title = title.strip()
        if colourSchemeName == "" :
            colourSchemeName = "standard"
        colourScheme = altitudeRGB.getColourScheme(colourSchemeName, (summaryDict['minAltitude'], summaryDict['maxAltitude']))

        useSeparatePlotWindows = True
        if useSeparatePlotWindows :
            plt.figure(1)
        else :
            plt.figure(figsize=(16, 8))
            plt.subplot(211)

        if displayPlot :
            altitudestats.analyseAltitudes(colourScheme, title, aAllAltitudes)
            if useSeparatePlotWindows :
                plt.figure(figsize=(16, 8))
                plt.figure(2)
            else :
                plt.subplot(212)

        fig, ax = generatePlot(colourScheme, title, aAllAltitudes, s, summaryDict['minAltitude'], summaryDict['maxAltitude'])

        if savePlot :
            startTime = time.time()
            if colourSchemeName == "standard" :
                pngName = title.replace(" ", "_") + ".png"
            else :
                pngName = title.replace(" ", "_") + "_" + colourSchemeName + ".png"

            pngName = "pngs/" + pngName
            plt.savefig(pngName, dpi=600, bbox_inches='tight', pad_inches = 0)        # NB Still leaves some white space around edge ????
            print("Generated file", pngName, " in", round(time.time() - startTime, 3), "seconds")

        if displayPlot :
            plt.show()

        return True, fig, ax

    elif summaryDict['status'] == "sea" :
        print("No GB land covered by this area")
        return False, 'dummy', 'dummy'
    else :
        print("Something went wrong!")
        return False, 'dummy', 'dummy'

# From the parameters, work out the identity of the 10x10 square which will be at the south-west corner of our plot 
def areaToMapFromParameters(basisSquareName, dimensions) :
    # Is this a valid 100x100 grid square identifier ?
    squareInfo = ng.checkGridSquareName(basisSquareName)
    if squareInfo != None :
        squareSize = squareInfo['size']
        sw10x10CornerSquare = ng.check10x10SquareName(basisSquareName + "00")
        sizeMultiplier = 10
    else :
        # Is this a valid square 10x10 square identifier ?
        squareInfo = ng.check10x10SquareName(basisSquareName)
        if squareInfo != None :
            squareSize = squareInfo['size']
            sw10x10CornerSquare = squareInfo
            sizeMultiplier = 1
        else :
            print("*** invalid square name:", basisSquareName, file=sys.stderr)
            return None

    # Have we specified valid dimensions for our plot ? Dimensions default to a one-by-one square
    if len(dimensions) == 0 :
        dimensions = "1x1"

    ew10x10Dimension,ns10x10Dimension = checkDimensions(dimensions, squareSize)
    if ew10x10Dimension == 0 :
        print("*** Invalid dimensions", dimensions, file=sys.stderr)
        return None
    else :
        # Convert dimensions to 10km squares, to match the size of the individual OS data files
        ew10x10Dimension *= sizeMultiplier
        ns10x10Dimension *= sizeMultiplier

    return sw10x10CornerSquare, ew10x10Dimension, ns10x10Dimension

# Expect dimensions to be specified as two integers sepated by an 'x', e.g 4x12. If so
# return the two numbers as a tuple; if not, return the tuple (0,0). Also limit the
# dimensions to 2000km x 2000km, when allowing for the squareSize being used in the
# dimensions.
def checkDimensions(dimensions, squareSize) :
    r = re.compile("([0-9]+)[xX]([0-9]+)")
    m = r.fullmatch(dimensions)

    if m :
        x = int(m.group(1))
        y = int(m.group(2))

        maxSize = 2000
        if x*squareSize > maxSize or y*squareSize > maxSize :
            # Too large
            x,y = 0,0
    else :
        # No match
        x,y = 0,0

    return (x,y)

# For each of the 10x10km squares in the 2-D array passed in, read the detailed altitude data points and generate a 
# single combined array with the data points for all the squares, and return this array.
#
# Also return a separate small array paralleling the array passed in, showing the status of the data points read for
# each square, to indicate one of:
# ""         - not yet processed (transient value)
# "offgrid"  - the (unnamed) square is beyond the national grid boundaries - no data available
# "sea"      - no data file found => no land in the square, all points are in the sea
# "ok"       - data read (probably => at least some points are on land)
# "error"    - problem reading the data file for the square => format not as expected ?
#
# Where no data points are available for a square (all except "ok"), the detailed data array contains a 'None' value instead of an altitude.
#
# As elsewhere, arrays use rows (1st dimension) for the north-south dimension, columns (2nd dimension) for the east-west
# dimension, and the rows index needs to be 'reversed' to make [0,0] represent the data for the south-west corner

def readDetailedDataPoints(a10x10SquareNames) :

    # Extract the dimensions of the array of 10x10km square names so we can loop over the array.    
    ns10x10Dimension = a10x10SquareNames.shape[0]
    ew10x10Dimension = a10x10SquareNames.shape[1]

    # Array to hold the status (string) of processing for each 10x10km square, using the same shape and indexing as a10x10SquareNames
    aStatus = np.empty(a10x10SquareNames.shape, dtype=object)

    # Track some overall values
    errorCount = 0
    foundSomeData = False
    overallMinAlt, overallMaxAlt = 10000, -10000

    for eastOffset in range(ew10x10Dimension) :
        for northOffset in range(ns10x10Dimension) :
            squareName = a10x10SquareNames[northOffset, eastOffset]
            status = ""
            if squareName == None :
                status = "offgrid"
            else :
                lines = altitudesfile.readTarget10x10SquareData(squareName)
                if len(lines) == 0 :    
                    # No data file found for this square - assume this means it's all in the sea
                    status = "sea"
                else :
                    # There is a data file, convert its contents into a detailed data points array
                    contents = altitudesfile.processFileContents(squareName, lines)
                    if contents == None :
                        # Something went wrong processing the file contents - stderr has a detailed reason.
                        status = "error"
                        errorCount += 1
                    else :
                        # The file contents make sense, we've got a 2-D array of altitudes for the square
                        (header, aAltitudes) = contents

                        # Check the array of datapoints has the same characteristics for each 10x10 square
                        if not foundSomeData :
                            # Record the characteristics expected for all squares
                            dataCellSize = header['cellsize']
                            dataCols = header['ncols']
                            dataRows = header['nrows']
                            # Create the overall array to hold combined altitude data for all 10x10 squares
                            aCombined = np.full([dataRows * ns10x10Dimension, dataCols * ew10x10Dimension], altitudeassessment.NO_ALTITUDE)
                            foundSomeData = True
                        else :
                            # Check data for subsequent squares has the same characteristics as the first
                            if dataCellSize != header['cellsize'] or dataCols != header['ncols'] or dataRows != header['nrows'] :
                                status = "error"
                                errorCount += 1
                                print("*** Data squares have different characteristic values:", 
                                        (dataCellSize, dataCols, dataRows), " v ", 
                                        (header['cellsize'], header['ncols'], header['nrows']),
                                        file=sys.stderr)

                    if status == "" :
                        status = "ok"
                        minAltitude, maxAltitude = aAltitudes.min(), aAltitudes.max()
                        overallMinAlt = overallMinAlt if minAltitude > overallMinAlt else minAltitude
                        overallMaxAlt = overallMaxAlt if maxAltitude < overallMaxAlt else maxAltitude
                        rowStartOffset = northOffset * dataRows
                        columnStartOffset = eastOffset * dataCols
                        # Copy this block of data into the larger array at its appropriate position
                        aCombined[rowStartOffset:rowStartOffset+dataRows, columnStartOffset:columnStartOffset+dataCols] = aAltitudes
            
            aStatus[northOffset, eastOffset] = status

    summaryDict = {}
    
    if foundSomeData :
        summaryDict['status'] = "ok"
        summaryDict['cellsize'] = dataCellSize
        summaryDict['colsPer10x10Square'] = dataCols
        summaryDict['rowsPer10x10Square'] = dataRows
        summaryDict['minAltitude'] = overallMinAlt
        summaryDict['maxAltitude'] = overallMaxAlt
        return summaryDict, aCombined, aStatus
    elif errorCount == 0 :
        summaryDict['status'] = "sea"
        return summaryDict, None, aStatus
    else :
        summaryDict['status'] = "error"
        return summaryDict, None, aStatus

def generatePlot(colourScheme, title, a, s, minAltitude, maxAltitude) :

    print("Working out RGB for plot: altitude range:", minAltitude, " - ", maxAltitude)
    startTime = time.time()

    av = np.empty([a.shape[0], a.shape[1]], dtype=float)
    avrgb = np.empty([a.shape[0], a.shape[1], 3], dtype=int)

    for row in range(a.shape[0]) :
        for col in range(a.shape[1]) :
            alt = a[row][col]
            #av[row, col] = alt
            if s[row,col] == 0 :
                rgb = colourScheme.getRGBForLandAltitude(alt)
            else :
                rgb = colourScheme.getRGBForWaterAltitude(alt)

            #rgb = (100,200,150)
            avrgb[row,col] = rgb #[0]

    print("Prepared plot after:", time.time() - startTime)
    plt.imshow(avrgb, origin='lower')
    print("Loaded plot after:", time.time() - startTime)
    plt.axis('off')
    plt.title(title)

    fig, ax = plt.gcf(), plt.gca()
    return fig, ax

######################################################
###
### Program entry point
###

if __name__ == "__main__" :

    if len(sys.argv) == 1 :
        print("No target grid square argument provided")
        exit()

    targetArg = sys.argv[1]
    dimensionsArg = sys.argv[2] if len(sys.argv) > 2 else ""
    colourSchemeArg = sys.argv[3] if len(sys.argv) > 3 else ""

    ok, fig, ax = main(targetArg, dimensionsArg, colourSchemeArg, displayPlot=True)

    if ok :
        print(fig)
        print()
        print(ax)
