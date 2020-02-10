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



# Local Python files 
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
        print(founddf.transpose(copy=True))
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
# to plot, invoking a detailed plotting function in a separate module (which allows a choice
# of plotting tool, 'CV2' or 'TK').
#
# The plot command requires a 'place' argument from the command line, which is then used
# to work out what sort of plot to perform and on what part of the National Grid.
#
# The 'place' argument can take a number of different values:
#
# - one of several special values:
#   - 'all' : plot for the whole of Great Britain
#   - 'random_postcode' : plot for a postcode selected at random from the dataframe
#   - 'random_square'   : plot for a National Grid 100x100km square selected at random
# - a two-letter top-level National Grid square identifier XX
#   - input format is'NG:<XX>'
#   - or if there is no Postcode area with this value, can just use 'XX' 
# - a one or two-letter Postcode area identier YY
#   - input format is'Area:<YY>'
#   - or if there is no National Grid square with this value, can just use 'YY' 
# - if a two letter place argument can be both a National Grid Square or a Postcode
#   area, then the command asks for more specific input format to be used
# - a postcode - anything matching a postcode regular expression is treated as a possible postcode
# - ???? Potentially others - countries, counties, Post districts, city/town names
# - ???? Potentially an area specified using National Grid coordindates

def plotPlace(df, placeArg, plotter='CV2', imageOutDir=None, verbose=False) :
    '''Plots points on a 'map' image showing the position of each postcode.'''

    # Work out how the 'place' argument from the command line is interpreted as a place type and value.
    placeType,placeValue = extractTypeValueFromPlaceArgs(df, placeArg, verbose)
    if placeType == '' :
        # Invalid argumnent, already reported.
        return 1

    # Special value for the output directory to indicate that the output image file should not be saved.
    if imageOutDir.lower() == 'none' :
        imageOutDir = None

    if placeType == 'all' :
        plotAllGB(df, plotter, imageOutDir, verbose)
    elif placeType == 'pa' :
        if placeValue.lower() == 'random' :
            placeValue = getRandomPostcodeArea(df)
            print(f'Using random Postcode area "{placeValue}" ..')
        if checkPostcodeAreaExists(df, placeValue) :
            plotPostcodeArea(df, placeValue, plotter, imageOutDir, verbose)
        else :
            print()
            print(f'"{placeArg}" is not recognised as a Postcode Area.')
            return 1
    elif placeType == 'pc' :
        if placeValue.lower() == 'random' :
            placeValue = getRandomPostcode(df)
            print(f'Using random postcode "{placeValue}" ..')
        if checkPostcodeExists(df, placeValue) :
            plotPostcode(df, placeValue, plotter, imageOutDir, verbose)
        else :
            print()
            print(f'"{placeArg}" was not recognised as a Postcode.')
            return 1
    elif placeType == 'ng' :
        if placeValue.lower() == 'random' :
            placeValue = getRandomGridSquare(df)
            print(f'Using random National Grid square "{placeValue}" ..')
        if ng.nonSeaGridSquareNameExists(placeValue) :
            plotGridSquare(df, placeValue, plotter, imageOutDir, verbose)
        else :
            print()
            print(f'"{placeArg}" is not recognised as a National Grid square.')
            return 1
    else :
        print()
        print(f'"{placeType}" is not a recognised place type.')
        print()
        showPlaceArgUsage()
        return 1

    return 0

def extractTypeValueFromPlaceArgs(df, placeArg, verbose=False) :
    '''Inspect the 'place' argument from the command line and return a place type and place value.'''

    badReturn = ('','')     # For use when we find an error
    placeType = ''
    placeValue = ''

    badArgument = False
    placeArg = placeArg.strip()
    if ':' in placeArg :
        # If the place argument has a ':' in it, then the format should be <type>:<value>
        argComponents = placeArg.split(':')
        if len(argComponents) != 2 :
            badArgument = True
        else :
            placeType = argComponents[0].strip()
            placeValue = argComponents[1].strip()
    else :
        # We need to deduce the place type
        if placeArg.lower() == 'all' :
            placeType, placeValue = 'all', 'GB'
        elif len(placeArg) == 1 :
            # Assume it's a postcode area
            placeType, placeValue = 'pa', placeArg
        elif len(placeArg) == 2 :
            # Work out if this can only be a postcode area or only a National Grid square, not both
            gridSquareExists = ng.nonSeaGridSquareNameExists(placeArg)
            postcodeAreaExists = checkPostcodeAreaExists(df, placeArg)
            if gridSquareExists and not postcodeAreaExists :
                placeType, placeValue = 'ng', placeArg
            elif gridSquareExists and not postcodeAreaExists :
                placeType, placeValue = 'pa', placeArg
            elif gridSquareExists and postcodeAreaExists :
                print()
                print(f'"{placeArg}" is ambiguous - it matches a Postcode Area and a National Grid square.')
                print(f'Please use "pa:{placeArg}" or "ng:{placeArg}" to indicate which to use.')
                return badReturn
            else :
                print()
                print(f'"{placeArg}" is not recognised as a Postcode Area or a National Grid square.')
                return badReturn
        elif len(placeArg) >= 5 :
            # Assume it's a postcode
            placeType, placeValue = 'pc', placeArg
        else :
            badArgument = True

    if badArgument :
        print()
        print(f'"place" argument {placeArg} is not recognised.')
        print()
        showPlaceArgUsage()
        return badReturn

    if verbose :
        print(f'Interpretted place argument "{placeArg}" as placetype={placeType} : placevalue={placeValue}')

    return (placeType, placeValue)

def showPlaceArgUsage():
    print('Usage for the "place" argument:')
    # ????

def checkPostcodeAreaExists(df, code) :
    dfCheck = df [ df['PostcodeArea'] == code.strip().upper() ]
    #print(dfCheck[0:1].transpose(copy=True))
    return dfCheck.shape[0] > 0

def checkPostcodeExists(df, code) :
    normalisedCode = normalisePostcodeFormat(code)
    dfCheck = df [ df['Postcode'] == normalisedCode.strip().upper() ]
    print(dfCheck[0:1].transpose(copy=True))
    return dfCheck.shape[0] > 0

def getRandomPostcode(df) :
    pos = random.randrange(0, df.shape[0])
    postcode = df.loc[pos, 'Postcode']
    return postcode

def getRandomPostcodeArea(df) :
    pos = random.randrange(0, df.shape[0])
    postcodeArea = df.loc[pos, 'PostcodeArea']
    return postcodeArea

def getRandomGridSquare(df) :
    l = ng.getNonSeaGridSquareNames()
    pos = random.randrange(0, len(l))
    sq = l[pos]
    return sq

# Actually plotting ...

def plotPostcode(df, postcode, plotter='CV2', savefilelocation=None, verbose=False) :

    formattedPostcode = normalisePostcodeFormat(postcode)

    founddf = df [df['Postcode'] == formattedPostcode]
    if founddf.empty :
        print(f'*** Postcode not found in dataframe : {postcode}')
        return 1
    else :
        plotAround(df, postcode, founddf['Eastings'], founddf['Northings'], 10, plotter)
        return 0

def plotPostcodeArea(df, postcodeArea, plotter='CV2', savefilelocation=None, verbose=False) :
    # ????
    print()
    print(f'Plot postcode area not yet implemented - {postcodeArea}')

def plotAround(df, title, e, n, dimension_km, plotter, verbose=False) :
    dimension_m = dimension_km * 1000       # m
    bl = (int(e-dimension_m/2), int(n-dimension_m/2))
    tr = (int(e+dimension_m/2), int(n+dimension_m/2))
    print(f'bl = {bl} : tr = {tr}')

    pcplot.plotSpecific(df, title=title, canvas_h=800, density=1, bottom_l=bl, top_r=tr, plotter=plotter)

    # Save file option ?

def plotGridSquare(df, sqName='TQ', plotter='CV2', savefilelocation=None, verbose=False) :
    sq = ng.dictGridSquares[sqName.upper()]        
    print(f'Sq name = {sq.name} : e = {sq.eastingIndex} : n = {sq.northingIndex} : mlength {sq.mLength}')
    print(f'{sq.getPrintGridString()}')
    
    if not sq.isRealSquare :
        print(f'Square {sq.name} is all sea ..')
    
    bl = (sq.eastingIndex * 100 * 1000, sq.northingIndex * 100 * 1000)
    tr = ((sq.eastingIndex + 1) * 100 * 1000, (sq.northingIndex+1) * 100 * 1000)
    print(f'bl = {bl} : tr = {tr}')

    img = pcplot.plotSpecific(df, title=sqName, canvas_h=800, density=1, bottom_l=bl, top_r=tr, plotter=plotter)
    if savefilelocation != None :
        filename = savefilelocation + '/' + 'postcodes.' + sqName.lower() + '.' + plotter.lower() +'.png'
        pcplot.writeImageArrayToFile(filename, img, plotter=plotter)

def plotAllGB(df, plotter='CV2', savefilelocation=None, verbose=False) :
    if plotter == 'CV2' :
        img = pcplot.cv2plotSpecific(df, title='All GB', canvas_h=800, density=1)
        if savefilelocation != None :
            filename = savefilelocation + '/' + 'postcodes.allGB.cv.png'
            pcplot.writeImageArrayToFile(filename, img, plotter=plotter)
    else :
        pcplot.tkplotSpecific(df, title='All GB', canvas_h=800, density=10)

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

    subparser = subparsers.add_parser('plot', help='Plot a map around the specified postcode')
    addPlotterArgumentOption(subparser)
    subparser.add_argument('-o', '--outdir', default=defaultImageOutDir, 
                        help='Specify the directory location for the image file, or set to "none" to suppress file production')
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
    subparser.add_argument('-p', '--plotter', choices=['CV2', 'TK'], default='CV2', help='Plot using CV2 (OpenCV) or TK')

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
            status = plotPlace(df, parsedArgs.place, parsedArgs.plotter, parsedArgs.outdir)
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
