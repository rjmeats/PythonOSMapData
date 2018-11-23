# Functions to work out some attributes of particular locations from altitude info, e.g.
# blocks of the same altitude, likely to be water (lakes, sea, estuaries and some larger rivers)

import numpy as np
import itertools

# Value used where we don't have a real altitude value to use
NO_ALTITUDE = -1000.0

# Are two float altitudes practically the same for our purposes ?
def sameAlt(f1, f2) :
    tolerance = 0.01
    return abs(f1 - f2) < tolerance

def sameAltitudeGrid(a, status) :
    s = np.empty([a.shape[0], a.shape[1]], dtype=int)

    # Set s values to 1 if a cell has the same altitude as all its neighbours, otherwise 0
    count = 0
    sameAltList = []
    for row in range(a.shape[0]) :
        for col in range(a.shape[1]) :
            alt = a[row][col]
            if alt == NO_ALTITUDE :
                s[row, col] = 1
            elif isSameAltAsAllNeighbours(row, col, alt, a) :
                s[row, col] = 1
            else :
                s[row, col] = 0
            # s[row, col] = 1 if isSameAltAsAllNeighbours(row, col, cell, a) else 0
            if s[row, col] == 1 :
                count += 1
                sameAltList.append((row, col))

    print(count, "cells having the same altitude as all neighbouring cells")
    total = count

    ## Look at each cell neighbouring the initial set to find further extensions around the edge. Keep looking
    ## as long as we find new extensions.

    extensionList = sameAltList
    carryOn = True
    loopCount = 0
    while(carryOn) :
        loopCount += 1
        if not extensionList :
            it = itertools.product(range(a.shape[0]), range(a.shape[1]))
        else :
            it = extensionList
        newExtensionList = []
        for row,col in it :
            alt = a[row][col]
            if s[row, col] != 0 and alt != NO_ALTITUDE :
                ## Get list of neighbouring cells of the same altitude not yet identified
                furtherCases = checkForUnassignedNeighbours(row, col, alt, a, s)                
                newExtensionList.extend(furtherCases)
                #print(row, col, len(furtherCases), furtherCases)
                #print("Found", len(extensionList), "further cases")
        if len(newExtensionList) == 0 :
            carryOn = False
        else :
            extensionList = newExtensionList
            total += len(extensionList)

    print(".. extended to a total of :", total)
    return s

def checkForUnassignedNeighbours(row, col, alt, a, s) :
    furtherCases = []
    for rowNo in range(row-1, row+2) :
        if rowNo < 0 or rowNo >= a.shape[0] :
            continue
        for colNo in range(col-1, col+2) :
            if colNo < 0 or colNo >= a.shape[1] :
                continue
            elif rowNo == row and colNo == col :
                continue
            elif s[rowNo, colNo] != 0 :
                continue
            else :
                if sameAlt(alt, a[rowNo, colNo]) :
                    s[rowNo, colNo] = 2
                    furtherCases.append((rowNo, colNo))
    return furtherCases

def isSameAltAsAllNeighbours(row, col, cell, a) :
    cellRowNo = row
    cellColNo = col
    alt = cell #[4]

    isSame = False
    verbose = False
    sameCount = 0
    notSameCount = 0

    for rowNo in range(cellRowNo-1, cellRowNo+2) :
        if notSameCount > 2 :
            return False

        if rowNo < 0 or rowNo >= a.shape[0] :
            notSameCount += 1
            continue
        for colNo in range(cellColNo-1, cellColNo+2) :
            if colNo < 0 or colNo >= a.shape[1] :
                notSameCount += 1
                continue
            elif rowNo == cellRowNo and colNo == cellColNo :
                continue

            compCell = a[rowNo, colNo]
            if compCell != None :
                compAlt = compCell #[4]
                if verbose :
                    print("Cell is", cell, alt)
                    print("Comp is", compCell, compAlt)

                if sameAlt(alt, compAlt) :
                    if verbose :
                        print("Same alt")
                    sameCount += 1
                else :
                    notSameCount += 1

    if sameCount >= 6 :
        isSame = True

    if verbose :
        print("Returning", isSame)

    return isSame

def isSameAltAsAssignedNeighbour(row, col, cell, a, s) :
    cellRowNo = row
    cellColNo = col
    alt = cell #[4]

    isSame = False
    verbose = False
    isSameCount = 0

    for rowNo in range(cellRowNo-1, cellRowNo+2) :
        if rowNo < 0 or rowNo >= a.shape[0] :
            continue
        for colNo in range(cellColNo-1, cellColNo+2) :
            if colNo < 0 or colNo >= a.shape[1] :
                continue
            elif rowNo == cellRowNo and colNo == cellColNo :
                continue
            # Don't include diagonal links
            #elif abs(rowNo - cellRowNo) + abs(colNo - cellColNo) > 1 :
            #    continue

            compCell = a[rowNo, colNo]
            compAlt = compCell #[4]
            if verbose :
                print("Cell is", cell, alt)
                print("Comp is", compCell, compAlt, s[rowNo, colNo])

            if sameAlt(alt, compAlt) and s[rowNo, colNo] > 0:
                if verbose :
                    print("Same alt")
                isSame = True

    if verbose :
        print("Returning", isSame)

    return isSame

