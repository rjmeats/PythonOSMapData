import os
import sys

import zipfile
import pandas as pd
import pickle
import argparse

import postcodesgeneratedf as pcgen
import postcodesplot as pcplot
import nationalgrid as ng

#############################################################################################

def saveDataframeAsCSV(df, outDir, postcodeArea='all', verbose=False) :

    if postcodeArea == None :
        postcodeArea = 'all'

    outFile = f'{outDir}/postcodes.{postcodeArea.lower()}.csv'

    print(f'Saving dataframe to CSV file {outFile} ..')
    if not os.path.isdir(outDir) :
        os.makedirs(outDir)
        print(f'.. created output location {outDir} ..')

    doSave = True
    if postcodeArea != 'all' :
        dfToSave = df [ df['PostcodeArea'] == postcodeArea.upper() ]
        print(f'.. filtering data to just {postcodeArea.upper()} postcodes : found {dfToSave.shape[0]} ..')
        if dfToSave.shape[0] == 0 :
            print()
            print(f'*** No data found for postcode area "{postcodeArea.upper()}" : no output file produced.')
            doSave = False
    else :
        dfToSave = df
        print(f'.. unfiltered data - includes all {dfToSave.shape[0]} postcodes ..')

    if doSave:
        dfToSave.to_csv(outFile, index=False)
        print(f'.. saved dataframe to CSV file {outFile}')

    return 0

#############################################################################################

def displayBasicDataFrameInfo(df, verbose=False) :
    """ See what the basic pandas info calls show about the dataframe. """

    print()
    print('###################################################')
    print('################## type and shape #################')
    print()
    print(f'type(df) = {type(df)} : df.shape : {df.shape}') 
    print()
    print('################## print(df) #####################')
    print()
    print(df)
    print()
    print('################## df.dtypes ##################')
    print()
    print(df.dtypes)
    print()
    print('################## df.info() ##################')
    print()
    print(df.info())
    print()
    print('################## df.head() ##################')
    print()
    print(df.head())
    print()
    print('################## df.tail() ##################')
    print()
    print(df.tail())
    print()
    print('################## df.index ##################')
    print()
    print(df.index)
    print()
    print('################## df.columns ##################')
    print()
    print(df.columns)
    print()
    print('################## df.describe() ##################')
    print()
    print(df.describe())
    print()
    print('################## df.count() ##################')
    print()
    print(df.count())
    print()
    print('###################################################')

    return 0

#############################################################################################

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


def displayPostcodeInfo(df, postcode='NG2 6AG', plotter='CV2', dimension=25, verbose=False) :
    formattedPostcode = normalisePostCodeFormat(postcode)

    print()
    print('###############################################################')
    print()
    print(f'Data for postcode {postcode}')
    print()
    format="Vertical"

    founddf = df [df['Postcode'] == formattedPostcode]
    if founddf.empty :
        print(f'*** Postcode not found : {formattedPostcode}')
        return 1

    if format == "Vertical" :
        # Print vertically, so all columns are listed
        print(founddf.transpose(copy=True))
    elif format == "Horizontal" :
        # Print horizontally, no wrapping, just using the available space, omitting columns in the middle
        # if needed. (The default setting.)
        print(founddf)
    elif format == "HorizontalWrap" :
        # Print horizontally, wrapping on to the next line, so all columns are listed
        pd.set_option('display.expand_frame_repr', False)
        print(founddf)
        pd.set_option('display.expand_frame_repr', True)        # Reset to default.
    else :
        print('f*** Unrecognised output row format: {format}')
        print(founddf)

    print()
    print('###############################################################')

    showPostcode(df, formattedPostcode, plotter, savefilelocation=None)

    return 0

def normalisePostCodeFormat(postCode) :
    pc = postCode.upper().strip().replace(' ', '')

    # Formats which we can use:
    # 'X9##9XX',
    # 'X99#9XX',
    # 'X9X#9XX',
    # 'XX9#9XX',
    # 'XX999XX',
    # 'XX9X9XX'
    # all 7 chars long
    # If more than 7, leave - must be wrong
    # If 6 chars long, put in a space in the middle
    # If 5 chars long, put in two spaces in the middle
    # If less than 5, leave - must be wrong
    # Don't bother checking letter/number aspects - just trying to smooth out spacing/case issues here.

    if len(pc) == 6 :
        pc = pc[0:3] + ' ' + pc[3:]
        print(f'6 : Modified {postCode} to {pc}')
    elif len(pc) == 5 :
        pc = pc[0:2] + '  ' + pc[2:]
        print(f'5 : Modified {postCode} to {pc}')

    return pc

def showPostcode(df, postCode, plotter='CV2', savefilelocation=None) :

    formattedPostCode = normalisePostCodeFormat(postCode)

    founddf = df [df['Postcode'] == formattedPostCode]
    if founddf.empty :
        print(f'*** Postcode not found in dataframe : {postCode}')
        return 1
    else :
        showAround(df, postCode, founddf['Eastings'], founddf['Northings'], 10, plotter)
        return 0

def showAround(df, title, e, n, dimension_km, plotter) :
    dimension_m = dimension_km * 1000       # m
    bl = (int(e-dimension_m/2), int(n-dimension_m/2))
    tr = (int(e+dimension_m/2), int(n+dimension_m/2))
    print(f'bl = {bl} : tr = {tr}')

    pcplot.plotSpecific(df, title=title, canvas_h=800, density=1, bottom_l=bl, top_r=tr, plotter=plotter)

    # Save file option ?

def showGridSquare(df, sqName='TQ', plotter='CV2', savefilelocation=None) :
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

def showAllGB(df, plotter='CV2', savefilelocation=None) :
    if plotter == 'CV2' :
        img = pcplot.cv2plotSpecific(df, title='All GB', canvas_h=800, density=1)
        if savefilelocation != None :
            filename = savefilelocation + '/' + 'postcodes.allGB.cv.png'
            pcplot.writeImageArrayToFile(filename, img, plotter=plotter)
    else :
        pcplot.tkplotSpecific(df, title='All GB', canvas_h=800, density=10)

######################################################################################

# Default locations for various files - can be overridden from the command line options.
defaultDataDir = "./OSData/PostCodes"       # Where the source data files are
defaultTmpDir = defaultDataDir + '/tmp'     # Working area, for unzipping and caching
defaultOutDir = "./out"                     # Output file location.
defaultImageOutDir = "./pngs"               # Output file location for image files.

######################################################################################

# Generating the dataframe from source data files takes a few minutes, so cache the dataframe
# in a file in the tmp directory, and read it in again for read-only commands which run against
# the dataframe.

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

######################################################################################

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
    Returns 0 if the sub-command was successful, 1 if not.'''

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

    # There are two types of sub-command:
    # - 'generate' which reads source data files and creates a Pandas dataframe for use by other sub-commands.
    #   The dataframe is written to a cache file.
    # - all the other sub-commands operate on a dataframe produced by reading in the cached dataframe from file.
    if parsedArgs.cmd == 'generate' :
        # Generate and then cache the dataframe
        df = pcgen.generateDataFrameFromSourceData(parsedArgs.datadir, tmpDir, verbose)
        if df.empty :
            print()
            print('*** No dataframe generated from source data.')
            return 1
        else :
            writeCachedDataFrame(df, tmpDir, parsedArgs.cachefile, verbose)
    else :
        # Retrieve the cached dataframe into memory, and then apply the relevant command to it.
        df = readCachedDataFrame(tmpDir, parsedArgs.cachefile, verbose)
        if df.empty :
            print()
            print('*** No dataframe read from cache.')
            return 1

        status = 0
        # Process the relevant sub-command using the dataframe read from cache.        
        if parsedArgs.cmd == 'info' :
            status = displayPostcodeInfo(df, postcode=parsedArgs.postcode, plotter=parsedArgs.plotter, dimension=10, verbose=verbose)
        elif parsedArgs.cmd == 'df_info' :
            status = displayBasicDataFrameInfo(df, verbose)
        elif parsedArgs.cmd == 'stats' :
            status = produceStats(df, verbose)
        elif parsedArgs.cmd == 'to_csv' :
            # Option to save all postcode data to CSV, or just for a specified postcode area.
            status = saveDataframeAsCSV(df, parsedArgs.outdir, parsedArgs.area, verbose)
        elif parsedArgs.cmd == 'plot' :
            print(f'Run plot command for "{parsedArgs.place}" ..')
            # Work out if the place is a postcode, a grid square, <others?> or 'all'
            # Allow for spaces, and case. Further options:
            # county, post area, post district, district/ward/borough etc, ONS unit code ?
            # Or just a rectangle identified using NG top-left, bottom right/dimensions.
            # Can these overlap ? E.g. Post area = NG square (populated). Certainly town names can clash.
            # Plotter control, dimensions control, control of whether or not to save as png and where
            
            imageOutDir = parsedArgs.outdir
            if imageOutDir.lower() == 'none' :
                imageOutDir = None
            if parsedArgs.place == 'all' :
                showAllGB(df, plotter=parsedArgs.plotter, savefilelocation=imageOutDir)
            elif ng.checkGridSquareName(parsedArgs.place) :
                showGridSquare(df, parsedArgs.place, plotter=parsedArgs.plotter, savefilelocation=imageOutDir)
            else :
                status = showPostcode(df, parsedArgs.place, plotter=parsedArgs.plotter, savefilelocation=imageOutDir)
            #else :
            #    # How best to handle this
            #    #print()
            #    #print(f'*** unrecognised place {parsedArgs.area} to plot')
            #    return
            # ???? Handle arbitrary areas ?
        else :
            print(f'Unrecognised command: {parsedArgs.cmd}')
            return 1

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

######################################################################################

if __name__ == '__main__' :
    status = main()
    sys.exit(status)
