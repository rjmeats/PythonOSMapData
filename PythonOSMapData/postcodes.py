import os
import sys
import zipfile
import pickle
import argparse
import random

import pandas as pd

'''
Python utility to process Ordnance Survey 'OS Code-Point Open' postcode data for Great Britain.
Data is loaded into a Pandas dataframe from OS source data files, and the dataframe can then
be processed in various ways:
- save the data as a CSV file
- to look up details of a specific postcode
- to plot 'maps' showing the locations of postcodes
- to show aggregate statistics about postcodes
The utility runs as a command-line program from the main() function at the bottom of this module.
'''

# Local Python files to import
import postcodesgeneratedf as pcgen     # To populate a dataframe from source data files
import postcodesplot as pcplot          # To handle details of plotting postcode-based maps
import nationalgrid as ng               # To handle OS National Grid squares

#############################################################################################

# Postcode notes:
#
# .. using the 'NG2 6AG' postcode as an example.
#
# UK postcodes have a number of different detailed patterns, but the basic format is always:
#
#   <outward-code><inward-code>   e.g.
# 
# with optional space(s) between the outward and inward codes.
# 
# The outward code (e.g. 'NG2') format is:
#
#   <postcode-area><postcode-district>
# 
# and the inward code format (e.g. '6AG') is:
#
#   <postcode-sector><postcode-unit>
#  
# - <postcode-area> consist of one or two letters (e.g. 'NG' which relates to the Nottingham postcode area)
# - <postcode-district> can be a single digit, two digits or a digit followed by a letter, (e.g. '2')
# - <postcode-sector> is a single digit (eg. '6')
# - <postcode-unit> is two letters (e.g. 'AG')
#
# Note that the inward code format is always 3 characters long. The outward code can be 2, 3 or 4 characters long.
#
# References:
#
# https://www.ordnancesurvey.co.uk/documents/product-support/user-guide/code-point-open-user-guide.pdf
# https://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom

def normalisePostcodeFormat(postcode, verbose=False) :
    '''Returns a postcode string converted into a normalised format, for each of matching.'''

    # Some possible variability in post code strings which may represent the same postcode:
    # - leading and trailing whitespace
    # - use of upper or lower case letters
    # - the number of spaces (if any) between the outward and inward parts
    #
    # We normalise an input postcode string so that:
    # - leading and trailing whitespace is removed
    # - letters are in upper case
    # - any spaces within the postcode are initially removed
    #   - if the resulting length is 5 characters, two spaces are inserted before the inward code
    #   - if the resulting length is 6 characters, one space is inserted before the inward code
    #   - if the resulting length is 7 characters, no spaces are inserted
    #   - if the resulting length is less than 5 characters or more than 7 characters, this cannot
    #     be a valid postcode, don't do any more with them.
    # - this results in all valid postcodes having a normalised form which is 7 characters long
    # - this matches the default formatting which is used in the OS postcode data file

    # Convert to upper case, remove trailing spaces and internal spaces ...
    npc = postcode.upper().strip().replace(' ', '')

    # Insert space(s) before the last three characters for 5  and 6 character cases, to make the overall
    # length 7 characters.
    if len(npc) in [5,6] :
        spaces = ' '*(7-len(npc))
        npc = npc[0:-3] + spaces + npc[-3:]

    if verbose :
        print(f'Normalised postcode [{postcode}] to [{npc}]')

    return npc

#############################################################################################

def saveDataframeAsCSV(df, postcodeArea='all', outDir=None, verbose=False) :
    '''Saves postcode data in a dataframe to a CSV file, with the option to restrict the data 
       saved to a specific postcode area.'''

    if outDir == None :
        print()
        print(f'No output directory specified for saving CSV file.')
        return 1

    if postcodeArea == None :
        postcodeArea = 'all'

    # Generate a name to be used for the CSV file.
    outFile = f'{outDir}/postcodes.{postcodeArea.lower()}.csv'

    if not os.path.isdir(outDir) :
        print(f'.. creating output directory {outDir} ..')
        os.makedirs(outDir)

    # Work out which rows in the data from to save in the CSV file.
    if postcodeArea != 'all' :
        dfToSave = df [ df['PostcodeArea'] == postcodeArea.upper() ]
        print(f'.. using filtered data - just postcodes in the {postcodeArea.upper()} area : found {dfToSave.shape[0]} ..')
        if dfToSave.shape[0] == 0 :
            print()
            print(f'*** No data found for postcode area "{postcodeArea.upper()}" : no output file produced.')
            return 1
    else :
        dfToSave = df
        print(f'.. using unfiltered data - includes all {dfToSave.shape[0]} postcodes ..')

    # Generate the file using Pandas function.
    print(f'.. writing data to CSV file {outFile} .. ', flush=True, end='')
    dfToSave.to_csv(outFile, index=False)       # index=False => don't include the row ID value.
    print(f'done.')

    return 0

#############################################################################################

# ???? To be redone.
def produceStats(df, verbose=False) :

    print(f'############### Grouping by PostcodeArea, all columns ###############')
    print()
    dfAreaCounts = df.groupby('PostcodeArea').count()
    print(f'Shape is {dfAreaCounts.shape}')
    print()
    print(dfAreaCounts)

    groupByColumns = [ 'PostcodeArea', 'Quality', 'Country_code', 'Admin_county_code', 'Admin_district_code', 
                        'Admin_district_code', 'Admin_ward_code' ]

    # Just show counts of each distinct value, column by column
    for groupByColumn in groupByColumns :
        # Need a specific column to count, otherwise just get list of group-by-column values with no counts.
        print()
        print(f'############### Grouping by {groupByColumn}, count only ###############')
        print()
        dfDistinctColumnValueCounts = df.assign(count=1)[[groupByColumn, 'count']].groupby(groupByColumn).count()
        print(f'Shape is {dfDistinctColumnValueCounts.shape}')
        print()
        print(dfDistinctColumnValueCounts)
        print(dfDistinctColumnValueCounts[0])
        print(dfDistinctColumnValueCounts[1])
        

    # Just PostcodeArea = shows that just a list of distinct values is returned when grouping a column with itself.
    dfAreaCounts = df[['PostcodeArea']].groupby('PostcodeArea').count()
    print()
    print(f'############### Grouping by PostcodeArea with itself ###############')
    print()
    print(f'Shape is {dfAreaCounts.shape}')
    print()
    print(dfAreaCounts)

    return 0
    
#############################################################################################

def displayPostcodeInfo(df, postcode='NG2 6AG', verbose=False) :
    '''Displays data relating to the specified postcode. Returns 0 if the postcode is found, 1 if not.'''

    normalisedPostcode = normalisePostcodeFormat(postcode)

    print()
    print(f'Data for postcode {postcode}')
    if verbose: 
        print()
        print(f' - normalised format {normalisedPostcode}')

    print()

    founddf = df [df['Postcode'] == normalisedPostcode]
    if founddf.empty :
        print(f'*** Postcode not found : {normalisedPostcode}')
        return 1

    orientation='Vertical'
    if orientation == 'Vertical' :
        # Print vertically, so all columns are listed on separate lines
        pd.set_option('display.max_rows', 100)                  # Allow for lots of fields
        print(founddf.transpose())
    elif orientation == 'Horizontal' :
        # Print horizontally, no wrapping, just using the available space, omitting columns in the middle
        # if needed. (The default Pandas print formatting.)
        print(founddf)
    elif orientation == 'HorizontalWrap' :
        # Print horizontally, wrapping on to the next line, so all columns are listed.
        pd.set_option('display.expand_frame_repr', False)
        print(founddf)
    else :
        print(f'*** Unrecognised printing orientation: {orientation}')
        print()
        print(founddf)

    return 0

#############################################################################################

# Functions to handle the initial processing of the 'plot' command, to work out what 'place'
# to plot by processing the command line arguments in more detail.
#
# The plot command requires a 'place' argument from the command line, which is then used
# to work out what sort of plot to perform and on what part of the National Grid.
#
# The 'place' argument can take a number of different values as described in the help text.
#
# Future place options to add might be counties/wards/town names ...

def showPlaceArgUsage():
    '''Prints out usage text for the 'place' command line argument.'''
    print('Usage for the "place" argument:')
    print('''
    - 'pa:XX' plots postcodes for a specified postcode area XX (can be one or two letters long)
       For example 'pa:TQ' specifies a plot for the TQ (Torquay) Postcode Area
    - 'pc:XXXXXXX' plots the area around a specified postcode XXXXXXX (can be varous lengths and include spaces)
       For example 'pc:NG2 6AG' (surrounded by quotes because of the space) specifies a plot around Nottingham's 
       Trent Bridge postcode.
    - 'ng:XX' plots postcodes for a specified OS National Grid square XX (must consist of two letters)
       For example 'ng:TQ' specifies a plot of the TQ national grid square (roughly London and areas to its south)
    - for all the above, part after the colon can also be set to 'random' which causes a random item of the appropriate
      type to be used for the plot. 
      For example 'pc:random' selects a random postcode from the full set of postcodes and plots the area around it.
    - as well as the 'colon-based' formats above, some other forms are accepted where they are clear:
      - 'all' plots postcodes for the whole of Great Britain
      - 'random' chooses a random place type (pa/pc/ng) and a random item of this type to plot
      - a single character place argument is interpretted as a postcode area
        For example 'E'
      - a two character place argument is interpretted as either a postcode area or National Grid square. If the
        argument is valid for either then you need to use the colon-based form to clear up the ambiguity.
        For example 
        - 'DN' is interpretted as the DN postcode area (Doncaster), as there is no DN National Grid square.
        - 'NZ' is interpretted as the NZ National Grid square, as there is no NZ postcode area
        - 'TQ' is not processed, as it is a valid postcode area and a valid National Grid square
      - arguments of 5 or more characters are treated as postcodes
    ''')

def extractPlaceInfoFromPlaceArgs(df, placeArg, verbose=False) :
    '''Inspect the 'place' argument from the command line.
       Return a (place type, place value) tuple from it.'''

    failureIndicator = ('','')     # Returned when we find an error in the argument.
    placeType, placeValue = ('','')

    badArgument = False
    placeArg = placeArg.strip()

    if ':' in placeArg :
        # If the place argument has a ':' in it, then the format should be <type>:<value>
        argComponents = placeArg.split(':')
        if len(argComponents) != 2 :
            badArgument = True
        else :
            placeType, placeValue = argComponents[0].strip().lower(), argComponents[1].strip()
            if placeType.lower() not in ['pa', 'pc', 'ng']:
                badArgument = True
    else :
        # We need to deduce the place type
        if placeArg.lower() == 'all' :
            placeType, placeValue = 'all', 'GB'
        elif placeArg.lower() == 'random' :
            # Choose a random place type and value.
            placeType, placeValue = 'random', 'random'
        elif len(placeArg) == 1 :
            # Assume it's a postcode area
            placeType, placeValue = 'pa', placeArg
        elif len(placeArg) == 2 :
            # Work out if this can only be a postcode area or only a National Grid square, not both
            gridSquareExists = ng.nonSeaGridSquareNameExists(placeArg)
            postcodeAreaExists = checkPostcodeAreaExists(df, placeArg)
            if gridSquareExists and not postcodeAreaExists :
                placeType, placeValue = 'ng', placeArg
            elif not gridSquareExists and postcodeAreaExists :
                placeType, placeValue = 'pa', placeArg
            elif gridSquareExists and postcodeAreaExists :
                print()
                print(f'"{placeArg}" is an ambiguous place identifier - it is a Postcode Area and also a National Grid square.')
                print(f'Please use "pa:{placeArg}" or "ng:{placeArg}" to indicate which to use.')
                return failureIndicator
            else :
                print()
                print(f'"place" argument "{placeArg}" is not recognised as a Postcode Area or a National Grid square.')
                return failureIndicator
        elif len(placeArg) >= 5 :
            # Assume it's a postcode
            placeType, placeValue = 'pc', placeArg
        else :
            badArgument = True

    if badArgument :
        print()
        print(f'"place" argument "{placeArg}" is not recognised.')
        print()
        showPlaceArgUsage()
        return failureIndicator

    if verbose :
        print(f'Interpretted place argument "{placeArg}" as placetype={placeType} : placevalue={placeValue}')

    return (placeType, placeValue)


#############################################################################################

def plotPlace(df, placeType, placeValue, plotter='CV2', imageOutDir=None, verbose=False) :
    '''Dispatches plotting to detailed methods for each place type, after checking that the place is known.
       Returns 0 if successful, 2 if the place is not known.'''

    # Special value for the output directory to indicate that the output image file should not be saved.
    if imageOutDir != None and imageOutDir.lower() == 'none' :
        imageOutDir = None

    if placeType.lower() == 'random' :
        placeType, placeValue = getRandomPlaceType(['pa', 'pc', 'ng']), 'random'

    # If the place value specified is 'random', generate a random place value of the appropriate type.
    if placeValue.lower() == 'random' :
        if placeType == 'pa' :
            placeValue = getRandomPostcodeArea(df)
        elif placeType == 'pc' :
            placeValue = getRandomPostcode(df)
        elif placeType == 'ng' :
            placeValue = getRandomGridSquare(df)
        else :
            print(f'"random" option not supported for place type {placeType}')
            return 1
        print()
        print(f'Using random place "{placeType}:{placeValue}" ..')

    if placeType == 'all' :
        plotAllGB(df, plotter, imageOutDir, verbose)
    elif placeType == 'pa' :
        if checkPostcodeAreaExists(df, placeValue) :
            plotPostcodeArea(df, placeValue, plotter, imageOutDir, verbose)
        else :
            print()
            print(f'"{placeValue}" is not a known Postcode Area.')
            return 2
    elif placeType == 'pc' :
        if checkPostcodeExists(df, placeValue) :
            plotPostcode(df, placeValue, plotter, imageOutDir, verbose)
        else :
            print()
            print(f'"{placeValue}" is not a known Postcode.')
            return 2
    elif placeType == 'ng' :
        if ng.nonSeaGridSquareNameExists(placeValue) :
            plotGridSquare(df, placeValue, plotter, imageOutDir, verbose)
        else :
            print()
            print(f'"{placeValue}" is not a land-based National Grid square.')
            return 2

    return 0

def checkPostcodeAreaExists(df, code) :
    '''Does this Postcode Area exist in the dataframe ?'''
    return code.strip().upper() in df['PostcodeArea'].values

def checkPostcodeExists(df, code) :
    '''Does this Postcode exist in the dataframe ?'''
    return normalisePostcodeFormat(code) in df['Postcode'].values

def getRandomCodeFromDataframe(df, columnName) :
    '''Return a random value from the specified column of the dataframe.'''
    randomRow = random.randrange(0, df.shape[0])
    return df.loc[randomRow, columnName]

def getRandomPostcode(df) :
    '''Return a random Postcode value from the dataframe.'''
    return normalisePostcodeFormat(getRandomCodeFromDataframe(df, 'Postcode'))

def getRandomPostcodeArea(df) :
    '''Return a random Postcode Area value from the dataframe.'''
    return getRandomCodeFromDataframe(df, 'PostcodeArea')

def getRandomGridSquare(df) :
    '''Return a random land-based National Grid square name.'''
    l = ng.getNonSeaGridSquareNames()
    randomIndex = random.randrange(0, len(l))
    return l[randomIndex]

def getRandomPlaceType(placeTypeList) :
    '''Return a random item from a list of place types.'''
    randomIndex = random.randrange(0, len(placeTypeList))
    return placeTypeList[randomIndex]

# Functions to actually invoke the plotting functions, after working out:
# - what subset of the dataframe points to plot
# - where the extent of the plot boundary should be. The extent is specified as the bottom-left
#   and top-right corner points of a rectangle, identified by National Grid Eastings,Northings
#   coordinates
# - a title for the displayed plot
# - what filename to use for saving the output image, if required.

def getGridRange(dfArea, marginProportion=0, verbose=False) :
    '''Look through the postcode grid reference values in the dataframe and determine a rectangle to cover them all,
       with a margin around the edge (defaulting to no margin).
       Returns two grid points as Eastings/Northings tuples - one for the bottom-left corner of the rectangle, the
       other for the top-right.
    '''

    # Find the extreme easting and northing values for the data. Ignore Eastings of 0, this indicates no location info.
    dfArea = dfArea [ dfArea['Eastings'] != 0 ]

    # Get min/max of the coordinate columns. 
    # This produces a dataframe of the format:
    #      Eastings  Northings
    # min     63222       8478
    # max    655448    1213615
    #
    # which we can extract individual values from.
    #
    #(NB For reference, the Pandas syntax to get a single value from a dataframe is: minE = dfArea['Eastings'].min() )

    dfAgg = dfArea[['Eastings', 'Northings']].agg([min,max])

    # Pull out the individual values, convert to normal Python ints using .item() - may not be really necessary.
    minE = dfAgg.loc['min', 'Eastings'].item()
    maxE = dfAgg.loc['max', 'Eastings'].item()
    minN = dfAgg.loc['min', 'Northings'].item()
    maxN = dfAgg.loc['max', 'Northings'].item()

    # Work out the size of the margin to add around the minimal rectangle. Use the same margin on all sides,
    # based on the largest dimension.
    marginSize = int(max( (maxE - minE), (maxN - minN) ) * marginProportion)

    # And now produce the final bottom-left and top-right points including the margin.
    bottomLeft = ( minE-marginSize, minN-marginSize )
    topRight   = ( maxE+marginSize, maxN+marginSize )

    if verbose:
        print()
        print(f'GetGridRange:')
        print(f'- marginProportion = {marginProportion}, marginSize =  {marginSize}')    
        print(f'- minE = {minE}, maxE = {maxE}, minN = {minN}, maxN = {maxN}')
        print(f'- bottomLeft = {bottomLeft}')
        print(f'- topRight = {topRight}')

    return bottomLeft, topRight

def restrictToGridRectangle(df, bottomLeft, topRight) :
    '''Filters the dataframe so that only postcodes with coordindates within the specified rectangle
       of coordindates defined by the bottomLeft and topRight eastings/northing points are retained. 
       Returns the filtered dataframe.
    '''
    dfArea = df
    dfArea = dfArea [ (dfArea['Eastings']  >= bottomLeft[0]) & (dfArea['Eastings']  <= topRight[0])]
    dfArea = dfArea [ (dfArea['Northings'] >= bottomLeft[1]) & (dfArea['Northings'] <= topRight[1])]
    return dfArea

def plotAllGB(df, plotter='CV2', savefilelocation=None, verbose=False) :
    '''Set up a plot of all postcodes in Great Britain.'''

    if plotter == 'TK' :
        print()
        print(f'*** Plot of all GB using TK plotter not attempted - can be very slow. Try the CV2 plotter.')
        return 1

    # No filtering on the dataframe - plot all postcodes locations.
    dfArea = df
    bottomLeft, topRight = getGridRange(dfArea, marginProportion=0.1, verbose=verbose)
    title = 'Great Britain'
    colouringAreaType = 'pa'
    plotterObject = pcplot.getPlotter(plotter)
    img = plotterObject.plotSpecific(dfArea, title=title, bottomLeft=bottomLeft, topRight=topRight, colouringAreaType=colouringAreaType)
    
    # Save the image in a file.
    if savefilelocation != None :
        plotterObject.writeImageArrayToFile(f'{savefilelocation}/postcodes.allGB.png', img)

    return 0

# Get the town name associated with the postcode area, from the relevant column in the first row of the
# filtered dataframe. Need to work on reset-index version of the dataframe (not saved) in order to be
# able to use .loc to access the first row as index=0 - as dataframes are 'view' based, and so the 
# index values (unique ID) comes from the underlying based dataframe, and so index=0 will not necessarily
# be present in the view-based dataframe.
def getPostTown(df) :
    '''Returns the post town value from the first row of the specified dataframe.'''
    town = df.reset_index().loc[0,'Post Town']
    return town

def plotPostcodeArea(df, postcodeArea='TQ', plotter='CV2', savefilelocation=None, verbose=False) :
    '''Set up a plot of the postcodes in the specified postcode area.'''

    # Filter the dataframe to just hold postcodes in this postcode area
    dfArea = df [ df['PostcodeArea'] == postcodeArea.upper()]
    if dfArea.empty :
        # Shouldn't happen
        print(f'*** Postcode area {postcodeArea.upper()} not found in dataframe')
        return 1

    bottomLeft, topRight = getGridRange(dfArea, marginProportion=0.1, verbose=verbose)
    title = f'Postcode area {postcodeArea.upper()} [{getPostTown(dfArea)}]'

    colouringAreaType = 'pa'
    plotterObject = pcplot.getPlotter(plotter)
    img = plotterObject.plotSpecific(dfArea, title=title, bottomLeft=bottomLeft, topRight=topRight, colouringAreaType=colouringAreaType)
    if savefilelocation != None :
        filename = f'{savefilelocation}/postcodes.pa.{postcodeArea.lower()}.png'
        plotterObject.writeImageArrayToFile(filename, img)

    return 0

def plotGridSquare(df, sqName='TQ', plotter='CV2', savefilelocation=None, verbose=False) :
    '''Set up a plot of the postcodes in the specified National Grid square.'''

    sq = ng.dictGridSquares[sqName.upper()]        
    
    if not sq.isRealSquare :
        print(f'Grid square {sq.name} is not a land-based square ..')
    
    # The eastingIndex and northingIndex values are relative to the main 100km grid squares of the National Grid
    # Convert these to metres to get the National Grid coordindate equivalent.
    # Work out the National Grid coordindates of the bottom-left and top-right points of the square
    factor = 100 * 1000
    bottomLeft = (sq.eastingIndex * factor, sq.northingIndex * factor)
    topRight = ((sq.eastingIndex + 1) * factor, (sq.northingIndex+1) * factor)

    # Remove postcodes not located in the square
    dfArea = restrictToGridRectangle(df, bottomLeft, topRight)

    if verbose:
        print()
        print(f'plotGridSquare for sqName = "{sqName}"')
        print(f'- {sq.getPrintGridString()}')
        print(f'- {sq.getLabel()}')
        print(f'- eastingIndex = {sq.eastingIndex} : northingIndex = {sq.northingIndex} : mLength = {sq.mLength}')
        print(f'- bottomLeft = {bottomLeft} : topRight = {topRight}')
        print(f'- postcode count = {dfArea.shape[0]}')

    if dfArea.shape[0] == 0 :
        print()
        print(f'No postcodes to plot in grid square {sqName}')
        return 0

    # Now add a margin around the area.
    margin = 5 * 1000
    bottomLeft = (bottomLeft[0]-margin, bottomLeft[1]-margin)
    topRight =   (topRight[0]+margin,   topRight[1]+margin)
    
    title=f'National Grid square {sqName.upper()} [{sq.getLabel()}]'

    colouringAreaType = 'pa'
    plotterObject = pcplot.getPlotter(plotter)
    img = plotterObject.plotSpecific(dfArea, title=title, bottomLeft=bottomLeft, topRight=topRight, colouringAreaType=colouringAreaType)
    if savefilelocation != None :
        filename = f'{savefilelocation}/postcodes.ng.{sqName.lower()}.png'
        plotterObject.writeImageArrayToFile(filename, img)

    return 0

def getPostcodeLocationDescription(dfpc, verbose=False) :
    '''Extract fields from a dataframe expected to containing a record for a single postcode, and use them to briefly describe its location.'''

    df = dfpc.reset_index()
    (ward, district, county, country) = df.loc[0, ['Ward Name', 'District Name', 'County Name', 'Country Name'] ]

    if pd.isnull(ward):     ward = ''
    if pd.isnull(district): district = ''
    if pd.isnull(county):   county = ''
    if pd.isnull(country):  country = ''

    # Tidy up some trailing words in some of the fields to make them more readable.
    # Could do more here ...
    ward = ward.replace(' Ward', '').replace(' ED', '')
    district = district.replace(' (B)', '').replace(' District', '').replace(' London Borough', ', London')

    # Accumulate a description from these fields.
    desc = ward if ward != '' else '[No ward]'
    if district != '' :
        desc = f'{desc}, {district}'
    if county != '' :
        desc = f'{desc}, {county}'

    if verbose:
        print()
        print('Location description generation:')
        print()
        print(dfpc)
        print()
        print(f'- ward = [{ward}], district = [{district}], county = [{county}], country = [{country}]')
        print(f'- location description = [{desc}]')

    return desc

def plotPostcode(df, postcode, plotter='CV2', savefilelocation=None, verbose=False) :
    '''Set up a plot of the postcodes within a square centred on a specific postcode.'''

    formattedPostcode = normalisePostcodeFormat(postcode)
    dfpc = df [df['Postcode'] == formattedPostcode]
    if dfpc.empty :
        print(f'*** Postcode to plot {postcode} not found in dataframe')
        return 1
    elif dfpc.shape[0] != 1 :
        print(f'*** Unexpected dataframe size plotting Postcode {postcode} : {dfpc.shape[0]} instead of 1')
        return 1
    locationDesc = getPostcodeLocationDescription(dfpc, verbose)

    # Get the coordindates of this postcode. .item() converts numpy int to normal Python int
    dfpc = dfpc.reset_index()
    pcEasting  = dfpc.loc[0, 'Eastings'].item()
    pcNorthing = dfpc.loc[0, 'Northings'].item()

    # Calculate the NationalGrid coordindates of a square centred on our postcode of interest
    sqDimensions = 10 * 1000    # Metres
    bottomLeft = (int(pcEasting-sqDimensions/2), int(pcNorthing-sqDimensions/2))
    topRight =   (int(pcEasting+sqDimensions/2), int(pcNorthing+sqDimensions/2))

    # Remove postcodes not located in the square
    dfArea = restrictToGridRectangle(df, bottomLeft, topRight)

    # Put a space in the formatted code if not present, before the 'inward' part.
    displayablePostcode = formattedPostcode.upper()
    if not ' ' in displayablePostcode:
        displayablePostcode = f'{displayablePostcode[0:-3]} {displayablePostcode[-3:]}'

    title=f'Around postcode {displayablePostcode} [{locationDesc}]'

    # No margin added.

    if verbose:
        print()
        print(f'plotPostCode for postcode = "{formattedPostcode}"')
        print(f'- pcEasting = {pcEasting} : pcNorthing = {pcNorthing} : sqDimensions = {sqDimensions}')
        print(f'- bottomLeft = {bottomLeft} : topRight = {topRight}')
        print(f'- postcode count = {dfArea.shape[0]}')

    colouringAreaType = 'pa'
    plotterObject = pcplot.getPlotter(plotter)
    img = plotterObject.plotSpecific(dfArea, title=title, bottomLeft=bottomLeft, topRight=topRight, keyPostcode=formattedPostcode, 
                                colouringAreaType=colouringAreaType)

    if savefilelocation != None :
        filename = f'{savefilelocation}/postcodes.pc.{formattedPostcode.replace(" ", "").lower()}.png'
        plotterObject.writeImageArrayToFile(filename, img)

    return 0

#############################################################################################

# Default locations for various files - can be overridden from the command line options.
defaultDataDir = './OSData/Postcodes'       # Where the source data files are
defaultTmpDir = defaultDataDir + '/tmp'     # Working area, for unzipping and caching
defaultOutDir = './out'                     # Output file location.
defaultImageOutDir = './pngs'               # Location for image files produced.

#############################################################################################

# Generating the dataframe from source data files takes a few minutes, so we cache the dataframe
# generated in a file in the tmp directory, and read it in again next time we run the program if
# it's not a 'generate' command being processed.

def getCacheFilePath(tmpDir=defaultTmpDir) :
    '''Where is the cached dataframe file located ?'''
    return tmpDir + '/cached/df.cache'

def readCachedDataFrame(tmpDir=defaultTmpDir, cacheFile=None, verbose=False) :
    '''Read the cached dataframe pickle file back into a dataframe.
       The default file location can be overridden by the caller.
       Returns an empty dataframe if the file cannot be found.
    '''
    if cacheFile == None :
        cacheFile = getCacheFilePath(tmpDir)
    if os.path.isfile(cacheFile) :
        with open(cacheFile, 'rb') as f:
            print(f'Reading pre-existing dataframe from cache file {cacheFile} .. ', flush=True, end='')
            df = pickle.load(f)
            print(f'done.')
    else :
        print(f'*** No cache file {cacheFile} found.')
        df = pd.DataFrame()

    return df

def writeCachedDataFrame(df, tmpDir=defaultTmpDir, cacheFile=None, verbose=False) :
    '''Write the dataframe out to a cache as a pickle file.
       The default file location can be overridden by the caller.
    '''
    if cacheFile == None :
        cacheFile = getCacheFilePath(tmpDir)

    # Create the directory paths needed, if any.
    cacheFileDir = os.path.dirname(cacheFile)
    print()
    if not os.path.isdir(cacheFileDir) :
        os.makedirs(cacheFileDir)
        print(f'Created cache file location {cacheFileDir}.')

    with open(cacheFile, 'wb') as f:
        print(f'Writing dataframe to cache file {cacheFile} .. ', flush=True, end='')    
        pickle.dump(df, f)
        print(f'done.')

#############################################################################################

# Section covering command-line argument handling and the 'main' program.

def defineAllowedArguments() :
    '''Tell argparse about the allowed commands and options. Returns a parser object.'''

    # The program provides several different sub-commands, each with its own command line options:
    # - the 'generate' command reads the source data files and generates a dataframe, which is then cached.
    # - the other commands (e.g. 'info') load the cached dataframe (much quicker than generating it from source)
    #   and then read it to produce some sort of output.

    parser = argparse.ArgumentParser(description='OS Code-Point Postcode data processing program')
    subparsers = parser.add_subparsers(help='sub-command help')

    subparser = subparsers.add_parser('generate', help='Read OS data files to generate a cached dataframe for use with other commands')
    subparser.set_defaults(cmd='generate')
    subparser.add_argument('-d', '--datadir', default=defaultDataDir, help='Directory location of the source data files to be read')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('df_info', help='Show info about the Pandas dataframe structure')
    subparser.set_defaults(cmd='df_info')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('stats', help='Produce stats and aggregates relating to the postcodes dataset')
    subparser.set_defaults(cmd='stats')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('to_csv', help='Produce a csv file containing the postcodes dataset')
    subparser.set_defaults(cmd='to_csv')
    subparser.add_argument('-o', '--outdir', default=defaultOutDir, help='Specify the directory location for the output CSV file')
    subparser.add_argument('-a', '--area', help='A postcode area to filter the output by')
    addStandardArgumentOptions(subparser)
    
    subparser = subparsers.add_parser('info', help='Display info about a specified postcode')
    addPlotterArgumentOption(subparser)
    subparser.add_argument('postcode', help='the postcode of interest, in quotes if it contains any spaces')
    subparser.set_defaults(cmd='info')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('plot', help='Plot a map around the specified postcode.')
    addPlotterArgumentOption(subparser)
    subparser.add_argument('-o', '--outdir', default=defaultImageOutDir, 
                        help='Specify the directory location for the image file, or set to "none" to suppress file production')
    subparser.add_argument('-i', '--iterations', type=int, choices=range(1, 11), default=1,
                            help='Number of plots to perform - only applies to "random" plots')
    addStandardArgumentOptions(subparser)
    subparser.add_argument('place', help='Identifies the area to be plotted, in quotes if it contains any spaces')
    subparser.set_defaults(cmd='plot')

    return parser

def addStandardArgumentOptions(subparser) :
    '''Define command line options which can apply to any/all commands.'''

    # NB All these arguments are optional.
    subparser.add_argument('-v', '--verbose', action='store_true', help='Show detailed diagnostics')
    subparser.add_argument('-t', '--tmpdir', default=defaultTmpDir, 
                        help='Override the default temporary directory location (used for unzipping data and as the default cache location)')
    subparser.add_argument('-c', '--cachefile', help='Override the default location for the dataframe cache file.')

def addPlotterArgumentOption(subparser) :
    '''Define a command line option to indicate which graphics plotter to use for map plots.'''
    subparser.add_argument('-p', '--plotter', choices=['CV2', 'TK', 'Bokeh', 'Plotly'], default='CV2', help='Plot using CV2 (OpenCV) or TK or Bokeh')

def processCommand(parsedArgs) :
    '''Use the arguments returned by argparse to work out which sub-command to perform, and perform it.
    Returns 0 if the sub-command was successful, or 1 if there is an error.'''

    # The 'verbose' argument can apply to any of the sub-commands.
    verbose = parsedArgs.verbose
    if verbose :
        print()
        print(f'Command line arguments extracted by argparse:')
        print(parsedArgs)

    # The 'tmpDir' argument apply to several sub-commands, so do some common checks here.
    tmpDir = parsedArgs.tmpdir
    if not os.path.isdir(tmpDir) :
        print()
        print(f'*** Temporary directory {tmpDir} does not exist.')
        return 1

    if verbose :
        print()
        print(f'Running {parsedArgs.cmd} command ..')
        print()

    # There are two types of sub-command:
    # - 'generate' which reads source data files and creates a Pandas dataframe for use by other sub-commands.
    #   The dataframe is written to a cache file.
    # - all the other sub-commands operate on a dataframe produced by reading in the cached dataframe from file.
    status = 0
    if parsedArgs.cmd == 'generate' :
        # Generate and then cache the dataframe
        df = pcgen.generateDataFrameFromSourceData(parsedArgs.datadir, tmpDir, verbose)
        if df.empty :
            print()
            print('*** No dataframe generated from source data.')
            status = 1
        else :
            writeCachedDataFrame(df, tmpDir, parsedArgs.cachefile, verbose)
            status = 0
    else :
        # Retrieve the cached dataframe produced by a previous 'generate' sub-command into memory.
        df = readCachedDataFrame(tmpDir, parsedArgs.cachefile, verbose)
        if df.empty :
            print()
            print('*** No dataframe read from cache.')
            return 1

        # Process the specified sub-command using the dataframe read from cache.
        if parsedArgs.cmd == 'to_csv' :
            status = saveDataframeAsCSV(df, parsedArgs.area, parsedArgs.outdir, verbose)
        elif parsedArgs.cmd == 'info' :
            status = displayPostcodeInfo(df, parsedArgs.postcode, verbose)
        elif parsedArgs.cmd == 'stats' :
            status = produceStats(df, verbose)
        elif parsedArgs.cmd == 'plot' :
            # Work out how the 'place' argument from the command line is interpreted as a place type and value.
            (placeType, placeValue) = extractPlaceInfoFromPlaceArgs(df, parsedArgs.place, verbose)
            if placeType == '' :
                # Invalid place argumnent (details have already been reported).
                status = 1
            # For the 'random' option, we all a 'number' argument to allow multiple succesive random plots to be shown.
            
            iterations = parsedArgs.iterations
            if placeValue.lower() != 'random' and iterations != 1 :
                print(f'(Ignoring "iterations" argument for non-random place value)')
                iterations = 1

            for i in range(1, iterations+1) :
                if iterations > 1 :
                    print()
                    print(f'Plot {i} of {iterations}')
                status = plotPlace(df, placeType, placeValue, parsedArgs.plotter, parsedArgs.outdir, verbose=verbose)
        elif parsedArgs.cmd == 'df_info' :
            status = pcgen.displayBasicDataFrameInfo(df, verbose)
        else :
            print(f'Unrecognised sub-command: {parsedArgs.cmd}')
            status = 1
        
    return status

def main() :
    '''Main program processing: read command line arguments, and invoke functions to process the specified commands.
    Returns 0 if command was successful, 1 if not.'''
    # Use the Python argparse command-line options parser to 
    # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_args
    # https://docs.python.org/3/library/argparse.html

    # Set up the command line parser, and then invoke it against the command line provided (sys.argv). 
    parser = defineAllowedArguments()
    parsedArgs = parser.parse_args()

    # Check that there was a sub-command on the command line - argparse doesn't seem to have a way to do this
    # itself for a diverse set of sub-commands, so we have to query the parsedArgs object it returns.
    status = 0
    if not hasattr(parsedArgs, 'cmd') :
        print('No sub-command provded.')
        parser.print_usage()
        status = 1
    else :
        # Look in detail at the parsed arguments, and perform the relevant operation.
        status = processCommand(parsedArgs)

    return status

#############################################################################################

if __name__ == '__main__' :
    status = main()
    sys.exit(status)
