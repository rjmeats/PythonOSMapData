import numpy as np
import matplotlib.pyplot as plt

import altitudeRGB

# Output a few stats about the altitudes in the data array for this square
def analyseAltitudes(colourScheme, squareName, aData) :

    np.set_printoptions(linewidth=200)

    print()
    print("Analyis of altitudes:")
    
    # Get an ordered list (1D array) of unique altitude values in the array, and a parallel list of counts for that altitude
    aUniqueValues, aCounts = np.unique(aData, return_counts=True)
    print("- cells:", aData.size, "(", aData.shape[0], "x", aData.shape[1], ")")
    print("- unique altitude values:", len(aUniqueValues))
    print("- altitude values range:", aUniqueValues[0], "-", aUniqueValues[-1], "m")
    print("- altitude range:", round(aUniqueValues[-1] - aUniqueValues[0], 1), "m")
    print("- average altitude:", round(np.average(aData), 1), "m")

    # Get details of which altitude value has the most counts. Numpy nonzero returns a 2D array, we just take the first
    # (usually only) entry
    mostCommonIndex = np.nonzero(aCounts == max(aCounts))[0][0]
    print("- most common altitude value:", aUniqueValues[mostCommonIndex], "m :", aCounts[mostCommonIndex], "cases,",
                round(aCounts[mostCommonIndex]/aData.size * 100, 3), "%")

    # List negative and near zero altitude frequencies, and frequencies greater than one percent
    onePercentCount = aData.size / 100
    print("- cases below 1m or over 1% of the total:")
    printCount = 0
    for altIndex in range(len(aUniqueValues)) :
        alt = aUniqueValues[altIndex]
        if alt < 1 or aCounts[altIndex] >= onePercentCount:
            #print("  ", alt, aCounts[altIndex], round(aCounts[altIndex]*100/aData.size, 2), "%")
            print("  {0:5.1f}m  {1:5d}  = {2:5.1f} %".format(alt, aCounts[altIndex], round(aCounts[altIndex]*100/aData.size, 2)))
            printCount += 1
    if printCount == 0 :
        print("  [None]")

    # And generate a histogram showing altitude ranges, selecting the range bands according to how large an altitude range
    # we have to cover.
    maxAlt = aUniqueValues[-1]
    minAlt = aUniqueValues[0]
    bins = [] if minAlt > 0 else [minAlt]       # If there are negative altitudes, put these all in one big bin
    binEdgeAlt = 0
    binSize = 100 if maxAlt > 200 else 20
    
    # Make sure there's a bin edge after the maximum altitude
    while binEdgeAlt - maxAlt <= 2*binSize:
        bins.append(binEdgeAlt)
        binEdgeAlt += binSize

    # Set up colours to be used for each altitude in our histogram chart
    colours = []
    for alt in bins :
        if alt >= 0 :
            rgb = colourScheme.getRGBForLandAltitude(alt)
        else :
            rgb = colourScheme.getRGBForWaterAltitude(alt)

        # Convert to '#rrgbb' string format
        colours.append( "#{0:02x}{1:02x}{2:02x}".format(int(rgb[0]), int(rgb[1]), int(rgb[2])) )

    # Produce a histogram chart showing altitude bands and how many points in the square fall into them.
    hist, binEdges = np.histogram(aData, bins=bins)
    print("Histogram bin edges:", binEdges)
    print("Histogram counts:", hist)

    # Plot as a bar chart
    # NB There is one more edge value than the number of bins, so allow for this to keep arrays the same shape
    # as the histogram just calculated
    bins_range = np.arange(len(binEdges)-1)  
    plt.bar(bins_range, hist, align='edge', width=0.9, color=colours)
    # Label the x-axis with the bin limits
    plt.xticks(bins_range, binEdges)        
    # Need to reduce the size of x-axis labels if there are lots of altitudes.
    labelsize = 10 if maxAlt < 900 else 5
    plt.tick_params(axis='x', which='major', labelsize=labelsize)

    plt.ylabel('Cases')
    plt.xlabel('Altitude (m)')
    plt.title("Distribution of altitudes in National Grid square " + squareName.upper())

