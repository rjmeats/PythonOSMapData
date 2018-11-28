# RGB Colouring based on altitude

import sys

class AltitudeColouring :

    def __init__(self, landMappings, waterColour, scalingRange=None) :
        self.landMappings = landMappings
        self.waterColour = waterColour
        self.scalingRange = scalingRange    # min and max of values to use for scaling to full range
        self.derive()

    def derive(self) :
        self.altitudes = [self.landMappings[i][0] for i in range(len(self.landMappings))]
        self.minRange = min(self.altitudes)
        self.maxRange = max(self.altitudes)
        self.positiveScalingFactor = (self.maxRange-0.1)/self.scalingRange[1] if self.scalingRange != None else 1
        self.negativeScalingFactor = (abs(self.minRange)-0.1)/abs(self.scalingRange[0]) if self.scalingRange != None else 1
        self.knownAltitudeRGBMappings = {}

    def getScaledEquivalent(self, scalingRange) :
        return AltitudeColouring(self.landMappings, self.waterColour, scalingRange)

    def dump(self) :
        for i in range(len(self.landMappings)) :
            print(self.landMappings[i])
        print("Min / Max:", self.minRange, "/", self.maxRange)
        print("Pos / Neg scaling factors:", self.positiveScalingFactor, "/", self.negativeScalingFactor)

    def getRGBForWaterAltitude(self, altitude) :
        return self.waterColour

    # Convert an altitude value to RGB
    def getRGBForLandAltitude(self, altitude) :

        rgb = [0x00, 0x00, 0x00]

        # Scale the actual altitude to a range 0-maxAltitude if the value is positive
        if altitude >= 0 :
            effectiveAltitude = altitude * self.positiveScalingFactor
        else :
            effectiveAltitude = altitude * self.negativeScalingFactor

        if effectiveAltitude in self.knownAltitudeRGBMappings:
            # Already calculated the colour for this value
            rgb = self.knownAltitudeRGBMappings[effectiveAltitude]
        elif effectiveAltitude < self.minRange or effectiveAltitude > self.maxRange :
            # Doesn't make sense. Shows as black.
            rgb = [0x00, 0x00, 0x00]
        else :
            # Find where this altitude value fits in the fixed colours list, and produce
            # an interpolated colour for it from the fixed colour entries just before and after
            higherAltitudeIndex = -1
            for fixedColourIndex in range(len(self.landMappings)) :
                if effectiveAltitude < self.landMappings[fixedColourIndex][0] :          # <= ????
                    higherAltitudeIndex = fixedColourIndex
                    break

            if higherAltitudeIndex == -1 :
                # Altitude is higher than the maximum - doesn't make sense. Shows as black.
                rgb = [0xFF, 0x00, 0x00]
            elif higherAltitudeIndex == 0 :
                # Altitude is lower than the minimum - doesn't make sense. Shows as black.
                rgb = [0x00, 0xFF, 0x00]
            else :
                # Pull out the altitude and RGB values from the table for the entries bracketing our value
                lowerAlt, lowerRGB = self.landMappings[higherAltitudeIndex-1] 
                higherAlt, higherRGB = self.landMappings[higherAltitudeIndex]
                RGBdiffs = (lowerRGB[0]-higherRGB[0], lowerRGB[1]-higherRGB[1], lowerRGB[2]-higherRGB[2])
                factor = (effectiveAltitude - lowerAlt)/(higherAlt-lowerAlt)
                rgb[0] = lowerRGB[0] - (RGBdiffs[0] * factor)
                rgb[1] = lowerRGB[1] - (RGBdiffs[1] * factor)
                rgb[2] = lowerRGB[2] - (RGBdiffs[2] * factor)

            # Save the value for re-use in subsequent calls
            self.knownAltitudeRGBMappings[effectiveAltitude] = rgb

        return rgb

# End of class

standardSchemeColours = [
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

standardScheme = AltitudeColouring(standardSchemeColours, (0xCC, 0xE5, 0xFF))

trialSchemeColours = [
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

trialScheme = AltitudeColouring(trialSchemeColours, (0xCC, 0xE5, 0xFF))

greySchemeColours = [
    # Altitude(m) RGB for that altitude
    [-10,       (0xFF, 0xFF, 0xEE) ],          
    [0,         (0xFF, 0xFF, 0xFF) ],          
    [1350,      (0x00, 0x00, 0x00) ],       # Ben Nevis = 1345 m
]

greyScheme = AltitudeColouring(greySchemeColours, (0xCC, 0xE5, 0xFF))

redSchemeColours = [
    # Altitude(m) RGB for that altitude
    [-10,       (0xFF, 0xFF, 0xEE) ],          
    [0,         (0xFF, 0xFF, 0xFF) ],          
    [1350,      (0xFF, 0x00, 0x00) ],       # Ben Nevis = 1345 m
]

redScheme = AltitudeColouring(redSchemeColours, (0xCC, 0xE5, 0xFF))

# Colour scheme class that allows easy reading of an altitude indicator via Matplot cursor
class NumericAltitudeColouring(AltitudeColouring) :

    def __init__(self) :
        pass

    def getScaledEquivalent(self, scalingRange) :
        return NumericAltitudeColouring()

    def dump(self) :
        pass

    def getRGB(self, altitude) :
        absa = abs(altitude)
        # Use R value for 100s of metrs and G for the tens and units, B for sign indicator
        if altitude >= 0 :
            return (absa // 100, absa % 100, 0)
        elif absa > 999 :
            # Off the map
            return (255, 255, 255)
        elif absa < 2.55 :
            return (absa * 100, absa, 255)
        elif absa < 10 :
            return (absa * 25, absa, 255)
        elif absa < 25.5 :
            return (absa * 10, absa, 255)
        else :
            # Assume nothing more than -25.5m. Enlarge scale to make small diffs easier to see.
            return (absa, absa, 255)

    def getRGBForWaterAltitude(self, altitude) :
        return self.getRGB(altitude)

    # Convert an altitude value to RGB
    def getRGBForLandAltitude(self, altitude) :
        return self.getRGB(altitude)


numericScheme = NumericAltitudeColouring()


colourSchemes = {
    "standard"  :   standardScheme,
    "trial"     :   trialScheme,
    "greyscale" :   greyScheme,
    "red"       :   redScheme,
    "numeric"   :   numericScheme
}


def getColourScheme(schemeName, scalingRange=None) :

    if schemeName in colourSchemes :
        baseScheme = colourSchemes[schemeName]
    else :
        print("*** Unknown colour scheme:", schemeName, file=sys.stderr)
        baseScheme = colourSchemes["standard"]

    if scalingRange == None :
        return baseScheme
    else :
        return baseScheme.getScaledEquivalent(scalingRange)
        #return AltitudeColouring(baseScheme.landMappings, baseScheme.waterColour, scalingRange)


# Main program dumps out colour scale info
if __name__ == "__main__" :

    # Show colours v altitudes mapping using matplotlib
    import numpy as np
    import matplotlib.pyplot as plt

    figureNo = 0
    for schemeName in colourSchemes.keys() :
        print("Scheme:", schemeName)
        scheme = colourSchemes[schemeName]
        scheme.dump()

        # Negative altitude indexes are treated as being indexed from end of array. So
        # these are plotted at right hand side. Add a margin to allow them to be 
        # visibly separated in the plot from the positive values.
        plotHeight = 1000
        minRange = -100
        maxRange = 1800
        argb = np.empty([plotHeight, maxRange + 100 + abs(minRange), 3], dtype=int)

        for altitude in range(maxRange+1) :
            rgb = scheme.getRGBForLandAltitude(altitude)
            for i in range(argb.shape[0]) :
                argb[i, altitude] = rgb

        # This shows -ve altitudes on the right of the plot (i.e. column offsets from the end of the np array)
        # with altitudes lower than the the minimum or higher than the maximum for the scheme shown as a black
        for altitude in range(minRange, 0) :
            rgb = scheme.getRGBForLandAltitude(altitude)
            for i in range(argb.shape[0]) :
                argb[i, altitude] = rgb

        figureNo += 1
        plt.figure(figureNo)
        fig = plt.imshow(argb, origin='lower')

        plt.gca().axes.get_yaxis().set_visible(False)
        plt.xlabel('Altitude (m)')
        plt.title("'" + schemeName + "'" + " colour scheme")

    # Show the figures, one per colouring scheme at the end.
    plt.show()