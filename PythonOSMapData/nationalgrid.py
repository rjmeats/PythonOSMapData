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

        # Description of the location covered by the square e.g. 'South Devon'
        self.label = '????'

    def setRealSquare(self, isRealSquare=True) :
        self.isRealSquare = isRealSquare

    def getName(self) :
        return self.name

    def setLabel(self, label) :
        self.label = label

    def getLabel(self) :
        return self.label

    # For diagnostic printing out
    def getPrintGridString(self) :
        if self.isRealSquare :
            return "{0:s}=({1:02d},{2:02d})".format(self.name, self.eastingIndex, self.northingIndex)
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

# Descriptive labels for squares
gridAreaNameDict = {
    'SV' : 'Isles of Scilly',
    'SW' : 'Cornwall',
    'SX' : 'South Devon',
    'SY' : 'Dorset Coast',
    'SZ' : 'Isle of Wight',
    'TV' : 'Beachy Head',
    'SR' : 'South-west Pembrokeshire',
    'SS' : 'North Devon and Swansea',
    'ST' : 'Bristol Channel',
    'SU' : 'Hampshire and Wiltshire',
    'TQ' : 'London, Surrey and Sussex',
    'TR' : 'Kent',
    'SM' : 'West Pembroke',
    'SN' : 'West Wales',
    'SO' : 'West Midlands, Welsh borders, Gloucester',
    'SP' : 'Birmingham, Oxfordshire and Warwickshire',
    'TL' : 'Hertfordshire and Cambridgeshire',
    'TM' : 'Essex and Suffolk',
    'SH' : 'Anglesey and Snowdonia',
    'SJ' : 'North-east Wales, Cheshire, Liverpool',
    'SK' : 'East Midlands',
    'TF' : 'The Wash',
    'TG' : 'Norfolk',
    'SC' : 'Isle of Man',
    'SD' : 'Lancashire',
    'SE' : 'Leeds, Bradford and York',
    'TA' : 'Humber Estuary and East Yorkshire coast',
    'NW' : 'Western edge of Galloway, south-west Scotland',
    'NX' : 'Galloway, south-west Scotland',
    'NY' : 'Cumbria, North Yorkshire and Borders',
    'NZ' : 'Newcastle and Durham',
    'OV' : 'East Yorkshire coast (miniscule)',
    'NR' : 'Islay, Jura, Kintyre',
    'NS' : 'Glasgow and Ayrshire',
    'NT' : 'Edinburgh and Borders',
    'NU' : 'Northumberland Coast',
    'NL' : 'Tiree and Barra (south west Hebrides)',
    'NM' : 'Mull, Oban and Mallaig',
    'NN' : 'Fort William, Pittlochry and Crieff',
    'NO' : 'Perth and Dundee',
    'NF' : 'North and South Uist, Outer Hebrides',
    'NG' : 'Skye and Wester Ross',
    'NH' : 'Inverness',
    'NJ' : 'Elgin and Aberdeen',
    'NK' : 'Peterhead, northeast of Aberdeen',
    'NA' : 'Western edge of Lewis, Outer Hebrides',
    'NB' : 'Lewis, Outer Hebrides',
    'NC' : 'Cape Wrath and northern Scotland',
    'ND' : 'Thurso and Wick, north-east Scotland',
    #'N[ABCD' : ''
    'HW' : 'North Rona, north-west of Cape Wrath',
    'HX' : 'Sule Skerry and Stack Skerry, west of Orkney',
    'HY' : 'Orkney',
    'HZ' : 'Fair Isle, between Shetland and Orkney',
    'HT' : 'Foula, westernmost of the Shetland Islands',
    'HU' : 'Shetland',
    'HP' : 'Unst, northernmost of the Shetland Islands',
    
    # incomplete
}

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
                if C not in lettersList :
                    sq.setLabel(gridAreaNameDict.get(sq.getName(), '????'))
            elif lettersList[0] == "+" :
                # A real squares
                sq.setRealSquare(C in lettersList)
                if C in lettersList :
                    sq.setLabel(gridAreaNameDict.get(sq.getName(), '????'))

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
import matplotlib.pyplot as plt
def prepareNationalGridPlot() :
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
    plt.title("National Grid Squares")
    im = ax.imshow(a, origin='lower')    

    return fig, ax

def displayPlot(fix, ax) :
    plt.show()

def getMatplotLibAsNumpyImage(fig, ax) :
    # See https://stackoverflow.com/questions/35355930/matplotlib-figure-to-image-as-a-numpy-array
    #ax.axis('off')
    fig.tight_layout(pad=2)

    # To remove the huge white borders
    ax.margins(0)

    fig.canvas.draw()
    image_from_plot = np.frombuffer(fig.canvas.tostring_rgb(), dtype=np.uint8)
    image_from_plot = image_from_plot.reshape(fig.canvas.get_width_height()[::-1] + (3,))
    return image_from_plot

# 'main' handling - show the national grid as a plot
if __name__ == "__main__" :
    fig, ax = prepareNationalGridPlot()
    displayPlot(fig, ax)
    #npimg = getNationalGridAsNumpyImage(fig, ax)
    #print(npimg.shape)

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

# Is this a valid 100x100 two-letter main grid square identifier, not entirely sea ?
def nonSeaGridSquareNameExists(squareName) :
    if squareName.upper() in dictGridSquares:
        sq = dictGridSquares[squareName.upper()]
        return sq.isRealSquare
    else :
        return False

def getNonSeaGridSquareNames() :
    return [k for k,v in dictGridSquares.items() if v.isRealSquare]    

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

    gridSquare = base10x10square['gridsquare']
    e = base10x10square['east'] + e_inc
    n = base10x10square['north'] + n_inc

    e = e % 10
    eastSquares = (base10x10square['east'] + e_inc - e) // 10
    for i in range(eastSquares) :
        gridSquare = nextSquareEast(gridSquare)

    n = n % 10
    northSquares = (base10x10square['north'] + n_inc - n) // 10
    for i in range(northSquares) :
        gridSquare = nextSquareNorth(gridSquare)

    if len(gridSquare) != 0 :
        name = "{0:s}{1:n}{2:n}".format(gridSquare, e, n)
    else :
        # Gone beyond the grid
        return None

    return check10x10SquareName(name)

