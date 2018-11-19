# Module for manipulating OS national grid squares and squares within them.
#

## t_.... names ????

import re
import numpy as np

# Land-based top-level squares start with S, T, N, O or H - a top-level map of GB in 500x500km squares 
# is:
#
#    HJ         (J is all sea)
#    NO         (O is virtuall all at sea)
#    ST
#
# Each of these squares is split into 25 100x100km squares (A-Z except I):
#
#    ABCDE
#    FGHJK
#    LMNOP
#    QRSTU
#    VWXYZ
#
# Southwest corner of the SV square provides the origin of National Grid coordinate system. Squares also map to the 1st digit of 
# # pure-numeric grid references (ie instead of grid references which specify a grid square), with the digit representing a 100 km
# offset of the south or west side of the square from the origin. (For north of scotland, this '1st digit' can actually be two digits, 
# 10, 11 ...)

class GridSquare :
    
    def __init__(self, name, eastingIndex, northingIndex) :
        self.name = name
        self.eastingIndex = eastingIndex
        self.northingIndex = northingIndex
        self.kmLength = 100
        self.mLength = self.kmLength * 1000

        # Values set up by separate calls to indicate whether this square is a real square or
        # just one filling out the overall grid, not containing any GB land.
        self.used = False

    def setUsed(self, isUsed=True) :
        self.isUsed = isUsed

    def getName(self) :
        return self.name

    # For diagnostic printing out
    def getPrintGridString(self) :
        if self.isUsed :
            return "{0:s}=({1:02d},{2:02d})".format(self.name, self.northingIndex, self.eastingIndex)
        else :
            return "{0:10.10s}".format("")


# Set up two structures of national grid squares:
#
# - 2D array grid
# - a dictionary indexed by square name (uppercase)

def generateGridSquares():
    superSquaresGrid = [ "HJ",
                         "NO", 
                         "ST" ]

    innerSquaresGrid = [ "ABCDE",
                         "FGHJK",
                         "LMNOP",
                         "QRSTU",
                         "VWXYZ" ]

    # We're going to set up a 2D array with an entry for each grid square. The array indexing will be (row, column), i.e. (northing, easting), with 0,0
    # representing the southwest corner grid square (SV) - and so if printing the array, we need to print in reverse row order to match normal map.
    gridSquaresNorthSouth = len(superSquaresGrid) * len(innerSquaresGrid)
    gridSquaresEastWest = len(superSquaresGrid[0]) * len(innerSquaresGrid[0])
    a = np.empty((gridSquaresNorthSouth, gridSquaresEastWest), dtype=object)
    d = {}

    #northingIndex = 0
    #eastingIndex = 0

    eastingIndexBasis = 0

    for superGridColumn in range(len(superSquaresGrid[0])) :
        northingIndexBasis = 0
        for superGridRow in superSquaresGrid[::-1] :
            letter1 = superGridRow[superGridColumn]
            #print(letter1, "n=", northingIndexBasis, "e=", eastingIndexBasis)
            eastingIndex = eastingIndexBasis*5
            for innerGridColumn in range(len(innerSquaresGrid[0])) :
                northingIndex = northingIndexBasis*5
                for innerGridRow in innerSquaresGrid[::-1] :
                    letter2 = innerGridRow[innerGridColumn]
                    squareName = letter1 + letter2
                    gs = GridSquare(squareName, eastingIndex, northingIndex)
                    a[northingIndex, eastingIndex] = gs
                    d[squareName] = gs
                    #print(letter1, letter2, "n=", northingIndex, "e=", eastingIndex)
                    northingIndex += 1
                eastingIndex += 1
            northingIndexBasis += 1
        eastingIndexBasis += 1
    
    return a, d

# Assign further info to grid squares:
# - are they 'real' squares, or just ones around the edge containing no GB land, produced to make an overall rectangle
# Assignments based on visual inspection of file:///C:/Users/owner/Documents/Development/PythonOSMapData/OSDocs/guide-to-nationalgrid.pdf

def assignGridInfo(a, d) :
    squareStatus = [ 
                        # Squares-to-exclude lists
                        ("S", "-ABFGLQ"), 
                        ("T", "-BCDEHJKNOPSTUWXYZ"),
                        ("N", "-QV"),
                        ("H", "-ABCDEFGHJKLMNQRSV"),
                        # Squares-to-include lists
                        ("O", "+V"),        # Only OV is real
                        ("J", "+")          # No J- squares are real
                    ]

    for (letter1, lettersList) in squareStatus :
        for C in "ABCDEFGHJKLMNOPQRSTUVWXYZ" :
            sq = d[letter1 + C]
            if lettersList[0] == "-" :
                # An artificial square, mark it
                sq.setUsed(C not in lettersList)
            elif lettersList[0] == "+" :
                # A real squares
                sq.setUsed(C in lettersList)


# Just show square name for full grid ????
def printFullGrid(a) :
    np.set_printoptions(linewidth=200, formatter = { 'object' : GridSquare.getPrintGridString})
    print(a[::-1,:])

aGridSquares, dictGridSquares = generateGridSquares()
assignGridInfo(aGridSquares, dictGridSquares)


# End of initialisation

# ===========================================================================

# Is this a valid 100x100 two-letter main grid square identifier ? If so, return a dictionary of its properties; if not, return None.
def checkGridSquareName(squareName) :

    if squareName.upper() in dictGridSquares:
        sq = dictGridSquares[squareName.upper()]
    else :
        return None

    dict = {}
    dict['name'] = sq.name# squareName.upper()
    dict['size'] = sq.kmLength
    dict['gridsquare'] = sq.name

    # ???? Include a full 6-digit easting and northing for the south-west corner of the square ?
    return dict

# Is this a valid 10x10 square identifier ? If so, return a dictionary of its properties; if not, return None.
# Check that the target 10x10km square is specified in the expected format, and uses the expected letters for
# a square in the Great Britain national grid.
#
# And these squares are split into 100 10x10km squares via an Easting 0-9and a Northing 0-9 value, both 0-9, 
# e.g. NY12

def check10x10SquareName(squareName) :
    r = re.compile("([A-Z][A-Z])([0-9])([0-9])", flags=re.IGNORECASE)
    m = r.fullmatch(squareName) 

    if not m :
        return None

    gridSquareName = m.group(1).upper()
    if checkGridSquareName(gridSquareName) :        
        dict = {}
        dict['name'] = squareName.upper()
        dict['size'] = 10
        dict['gridsquare'] = gridSquareName
        dict['east'] = int(m.group(2))
        dict['north'] = int(m.group(3))

        # ???? Include a full 6-digit easting and northing for the south-west corner of the square ?
        return dict
    else :
        return None

# Function to work out the identify of the 10x10km square we reach if
# we start from a specified 10x10 square and increment the eastings
# and northings by a specified number of 10km units, i.e. which square
# to we reach if we move so many squares to the east and north.
#
# Returns the identify of the 10x10km sqaure reached, or None if we've
# gone beyond the end of the OS national grid.

def get10x10SquareReached(base10x10square, e_inc, n_inc) :

    t_sq = base10x10square['gridsquare']
    t_e = base10x10square['east'] + e_inc
    t_n = base10x10square['north'] + n_inc

    t_e = t_e % 10
    eastSquares = (base10x10square['east'] + e_inc - t_e) // 10
    for i in range(eastSquares) :
        t_sq = nextSquareEast(t_sq)

    t_n = t_n % 10
    northSquares = (base10x10square['north'] + n_inc - t_n) // 10
    for i in range(northSquares) :
        t_sq = nextSquareNorth(t_sq)

    if len(t_sq) != 0 :
        name = "{0:s}{1:n}{2:n}".format(t_sq, t_e, t_n)
    else :
        # Gone beyond the grid
        return None

    return check10x10SquareName(name)


def nextSquareNorth(t_sq) :
    nextSqName = ""
    if t_sq.upper() in dictGridSquares :
        sq = dictGridSquares[t_sq.upper()]
        if sq.northingIndex+1 < aGridSquares.shape[0] :
            nextSq = aGridSquares[sq.northingIndex+1, sq.eastingIndex]            
            nextSqName = nextSq.name
    return nextSqName

def nextSquareEast(t_sq) :
    nextSqName = ""
    if t_sq.upper() in dictGridSquares :
        sq = dictGridSquares[t_sq.upper()]
        if sq.eastingIndex+1 < aGridSquares.shape[1] :
            nextSq = aGridSquares[sq.northingIndex, sq.eastingIndex+1]            
            nextSqName = nextSq.name
    return nextSqName

if __name__ == "__main__" :
    print("Checking NG ...")
    printFullGrid(aGridSquares)

    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()

    # Plot using a 100x100 scaled up version of the array so that we can show the axis tick points as being at
    # the southwest corner of the grid square rather than in the middle of it.
    scale = 100         
    # Where in each scaled-up square to put the text label:
    ntextLabelPos = 45
    etextLabelPos = 50

    a = np.empty((aGridSquares.shape[0]*scale, aGridSquares.shape[1]*scale, 3), dtype=int)
    for nscaled in range(a.shape[0]) :
        for escaled in range(a.shape[1]) :
            # Which national grid square are we looking at ?
            n = nscaled // scale
            e = escaled // scale
            squareColour = (0x99,0xff,0x66) if aGridSquares[n,e].isUsed else (0x99, 0xCC, 0xFF)
            a[nscaled,escaled] = squareColour
            if nscaled % scale == ntextLabelPos and escaled % scale == etextLabelPos  :
                textcolour = "black" if aGridSquares[n,e].isUsed else "gray"
                ax.text(escaled, nscaled, aGridSquares[n,e].name, ha="center", va="center", color=textcolour)   # NB Note swapped over axes!

    # Plot axes using our main grid array indexes, aligned to the bottom left (south-west) corner
    xtickPoints = list(range(0, a.shape[1], scale))
    xtickLabels = list(range(len(xtickPoints)))
    ytickPoints = list(range(0, a.shape[0], scale))
    ytickLabels = list(range(len(ytickPoints)))

    ax.set_xticks(xtickPoints)
    ax.set_xticklabels(xtickLabels)
    ax.set_yticks(ytickPoints)
    ax.set_yticklabels(ytickLabels)

    # Put in a light grey grid to show grid square boundaries
    plt.grid(True, color="gray", linewidth=1, linestyle="solid")

    im = ax.imshow(a, origin='lower')
    
    plt.title("National Grid Squares")
    plt.show()
