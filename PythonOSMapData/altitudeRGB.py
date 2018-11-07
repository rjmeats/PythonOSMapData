# RGB Colouring based on altitude

fixedColours1 = [
    # Altitude(m) RGB for that altitude
    [-10,       (0xFF, 0xFF, 0xEE) ],          
    [0,         (0xFF, 0xFF, 0x99) ],          
    [60,        (0xFF, 0xE9, 0xB3) ],
    [180,       (0xFF, 0xD9, 0xB3) ],
    [300,       (0xFF, 0xCC, 0x99) ],
    [430,       (0xFF, 0xBF, 0x80) ],
    [610,       (0xFF, 0xB3, 0x66) ],
    [900,       (0xFF, 0x99, 0x33) ],
    [1350,      (0xB3, 0x59, 0x00) ],       # Ben Nevis = 1345 m
]

fixedColours2 = [
    # Altitude(m) RGB for that altitude
    [-10,       (0xFF, 0xFF, 0xEE) ],          
    [0,         (0xCC, 0xFF, 0xCC) ],          
    [180,       (0xFF, 0xE9, 0xB3) ],
    [300,       (0xFF, 0xCC, 0x99) ],
    [430,       (0xFF, 0xBF, 0x80) ],
    [610,       (0xFF, 0xB3, 0x66) ],
    [900,       (0xFF, 0x99, 0x33) ],
    [1350,      (0xB3, 0x59, 0x00) ],       # Ben Nevis = 1345 m
]

fixedColours = fixedColours2

minRange = min(fixedColours[i][0] for i in range(len(fixedColours)))
maxRange = max(fixedColours[i][0] for i in range(len(fixedColours)))

knownAltitudeRGBMappings = {}

def getRGBForAltitude(altitude, waterIndicator, minAltitude, maxAltitude) :

    rgb = [0x00, 0x00, 0x00]

    # Scale the actual altitude to a range 0-maxAltitude if the value is positive
    effectiveAltitude = altitude * (maxRange-0.1)/maxAltitude if altitude > 0 else altitude

    if waterIndicator > 0 :
        rgb = [0xDC, 0xF5, 0xF0]
        rgb = [0x99, 0xCC, 0xFF]
        rgb = [0xCC, 0xE5, 0xFF]
    elif altitude < -10 :
        # Doesn't make sense. Shows as black.
        rgb = [0x00, 0x00, 0x00]
    elif effectiveAltitude in knownAltitudeRGBMappings:
        # Already calculated the colour for this value
        rgb = knownAltitudeRGBMappings[effectiveAltitude]
    else :
        # Find where this altitude value fits in the fixed colours list, and produce
        # an interpolated colour for it from the fixed colour entries just before and after
        higherAltitudeIndex = -1
        for fixedColourIndex in range(len(fixedColours)) :
            if effectiveAltitude < fixedColours[fixedColourIndex][0] :          # <= ????
                higherAltitudeIndex = fixedColourIndex
                break

        if higherAltitudeIndex == -1 :
            # Altitude is higher than the maximum - doesn't make sense. Shows as black.
            rgb = [0x00, 0x00, 0x00]
        elif higherAltitudeIndex == 0 :
            # Altitude is lower than the minimum - doesn't make sense. Shows as black.
            rgb = [0x00, 0x00, 0x00]
        else :
            # Pull out the altitude and RGB values from the table for the entries bracketing our value
            lowerAlt, lowerRGB = fixedColours[higherAltitudeIndex-1] 
            higherAlt, higherRGB = fixedColours[higherAltitudeIndex]
            RGBdiffs = (lowerRGB[0]-higherRGB[0], lowerRGB[1]-higherRGB[1], lowerRGB[2]-higherRGB[2])
            factor = (effectiveAltitude - lowerAlt)/(higherAlt-lowerAlt)
            rgb[0] = lowerRGB[0] - (RGBdiffs[0] * factor)
            rgb[1] = lowerRGB[1] - (RGBdiffs[1] * factor)
            rgb[2] = lowerRGB[2] - (RGBdiffs[2] * factor)

        # Save the value for re-use in subsequent calls
        knownAltitudeRGBMappings[effectiveAltitude] = rgb

    return rgb

# Main program dumps out colour scale info

if __name__ == "__main__" :

    for i in range(len(fixedColours)) :
        print(fixedColours[i])
    
    print("Min / Max:", minRange, "/", maxRange)

    # Show colours v altitudes mapping using matplotlib
    import numpy as np
    import matplotlib.pyplot as plt

    # Negative altitude indexes are treated as being indexed from end of array. So
    # these are plotted at right hand side. Add a margin to allow them to be 
    # visibly separated in the plot from the positive values.
    plotHeight = 1000
    argb = np.empty([plotHeight, abs(minRange) + maxRange + 100, 3], dtype=int)

    for altitude in range(minRange, maxRange+1) :
        rgb = getRGBForAltitude(altitude, 0, minRange, maxRange)
        for i in range(argb.shape[0]) :
            argb[i, altitude] = rgb
        

    fig = plt.imshow(argb, origin='lower')
    plt.show()

