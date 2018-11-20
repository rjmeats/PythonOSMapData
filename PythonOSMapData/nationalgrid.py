# Module for manipulating OS national grid squares and squares within them.
#

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
        self.isRealSquare = False

    def setRealSquare(self, isRealSquare=True) :
        self.isRealSquare = isRealSquare

    def getName(self) :
        return self.name

    # For diagnostic printing out
    def getPrintGridString(self) :
        if self.isRealSquare :
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

# Determine whether a grid square is a 'real' square, containing GB land of some sort, or just an excess square around the edge, present
# only to produce an overall rectangular grid. The excess squares can be all-sea or contain only non-GB land.
# Assignments based on visual inspection of file:///C:/Users/owner/Documents/Development/PythonOSMapData/OSDocs/guide-to-nationalgrid.pdf

def assignGridInfo(arrayGrid, dictGrid) :
    # Array to control how individual grid squares are classified
    squareStatus = [ 
                        # Squares-to-exclude lists
                        ("S", "-ABCFGLQ"),                  # SC == Isle of Man
                        ("T", "-BCDEHJKNOPSTUWXYZ"),
                        ("N", "-EPQV"),
                        ("H", "-ABCDEFGHJKLMNOQRSV"),       # H covers Orkneys and Shetland
                        # Squares-to-include lists
                        ("O", "+V"),        # Only OV is real, and very marginally so.
                        ("J", "+")          # No J- squares are real land.
                    ]

    for (letter1, lettersList) in squareStatus :
        for C in "ABCDEFGHJKLMNOPQRSTUVWXYZ" :
            # Find the grid square in the dictionary based on the two-letter name, and determined if it should be
            # marked as 'real' or not
            sq = dictGrid[letter1 + C]
            if lettersList[0] == "-" :
                # An artificial square, mark it
                sq.setRealSquare(C not in lettersList)
            elif lettersList[0] == "+" :
                # A real squares
                sq.setRealSquare(C in lettersList)


# End of initialisation

# ===========================================================================

# Module initialisation code to populate two global data items:
# aGridSquares - an array of GridSquare objects, indexed by northing, easting, 0,0 being the south-west corder of the grid (SV)
# dictGridSquares - a dictionary of GridSquare objects indexed by grid square name (upper case)

aGridSquares, dictGridSquares = generateGridSquares()
assignGridInfo(aGridSquares, dictGridSquares)

# ===========================================================================

# Diagnostics - show square names in full grid array
def printFullGrid(a) :
    np.set_printoptions(linewidth=200, formatter = { 'object' : GridSquare.getPrintGridString})
    print(a[::-1,:])

# Diagnostics - plot national grid array 
def displayNationalGridPlot() :
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(6, 8))

    # Plot using a 100x100 scaled up version of the array so that we can show the axis tick points as being at
    # the southwest corner of the grid square rather than in the middle of it.
    scale = 100         
    # Where in each scaled-up square to put the text label:
    ntextLabelPos = 45
    etextLabelPos = 50

    a = np.empty((aGridSquares.shape[0]*scale, aGridSquares.shape[1]*scale, 3), dtype=int)
    for nscaled in range(a.shape[0]) :
        for escaled in range(a.shape[1]) :
            # Which national grid square are we looking at ?us
            n = nscaled // scale
            e = escaled // scale
            squareColour = (0x99,0xff,0x66) if aGridSquares[n,e].isRealSquare else (0x99, 0xCC, 0xFF)
            a[nscaled,escaled] = squareColour
            if nscaled % scale == ntextLabelPos and escaled % scale == etextLabelPos  :
                textcolour = "black" if aGridSquares[n,e].isRealSquare else "gray"
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

    plt.grid(True, color="gray", linewidth=1, linestyle="solid")
    im = ax.imshow(a, origin='lower')    
    plt.title("National Grid Squares")
    plt.show()

# 'main' handling - show the national grid as a plot
if __name__ == "__main__" :
    displayNationalGridPlot()

# ===========================================================================
# ===========================================================================

# Methods for external callers

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

# Get the name of the grid square north of this one (or an empty string if we're already on the edge)
def nextSquareNorth(squareName) :
    nextSqName = ""
    if squareName.upper() in dictGridSquares :
        sq = dictGridSquares[squareName.upper()]
        if sq.northingIndex+1 < aGridSquares.shape[0] :
            nextSq = aGridSquares[sq.northingIndex+1, sq.eastingIndex]            
            nextSqName = nextSq.name
    return nextSqName

# Get the name of the grid square east of this one (or an empty string if we're already on the edge)
def nextSquareEast(squareName) :
    nextSqName = ""
    if squareName.upper() in dictGridSquares :
        sq = dictGridSquares[squareName.upper()]
        if sq.eastingIndex+1 < aGridSquares.shape[1] :
            nextSq = aGridSquares[sq.northingIndex, sq.eastingIndex+1]            
            nextSqName = nextSq.name
    return nextSqName

# ===========================================================================

# Methods relating to 10km x 10km squares within the main National Grid squares (which are 100x100), 
# which are identified by XXen  where XX is the National Grid square, e is the easting digit and n is the northing
# digit. E.g. NY12

# Is this a valid 10x10 square identifier ? If so, return a dictionary of its properties; if not, return None.
# Check that the target 10x10km square is specified in the expected format, and uses the expected letters for
# a square in the Great Britain national grid.
#

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

