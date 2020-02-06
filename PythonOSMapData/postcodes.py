import os
import sys

import zipfile
import pandas as pd
import pickle

import postcodesgeneratedf as pcgen
import postcodesplot as pcplot

#############################################################################################

def saveAsCSV(df, tmpDir) :
    outputDir = tmpDir + '/' + 'output'
    outFile = outputDir + '/' + 'postcodes.out.csv'

    print(f'Saving dataframe to CSV file {outFile} ..')
    if not os.path.isdir(outputDir) :
        os.makedirs(outputDir)
        print(f'.. created output location {outputDir} ..')

    df.to_csv(outFile, index=False)
    print(f'.. saved dataframe to CSV file {outFile}')

#############################################################################################

def displayBasicInfo(df) :
    """ See what the basic pandas info calls show about the dataframe. """

    print()
    print('###################################################')
    print()
    print(f'type(df) : {type(df)}') 
    print(f'df.shape : {df.shape}')
    print()
    print('################## df ##################')
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


#############################################################################################

def aggregate(df) :

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

#############################################################################################


def displayExample(df, example=None, plotter='CV2', dimension=25) :
    # Print out details of an example postcode (Trent Bridge).
    examplePostCode = 'NG2 6AG' if example == None else example

    formattedPostCode = normalisePostCodeFormat(examplePostCode)

    print()
    print('###############################################################')
    print()
    print(f'Values for example postcode {examplePostCode}')
    print()
    format="Vertical"

    founddf = df [df['Postcode'] == formattedPostCode]
    if founddf.empty :
        print(f'*** Postcode not found : {formattedPostCode}')
        return

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

    showPostcode(df, formattedPostCode, plotter, savefilelocation=None)

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
    #if plotter == 'CV2' :
    #    pcplot.cv2plotSpecific(df, title=title, canvas_h=800, density=1, bottom_l=bl, top_r=tr)
    #elif plotter == 'Tk' :
    #    pcplot.tkplotSpecific(df, title=title, canvas_h=800, density=1, bottom_l=bl, top_r=tr)

import nationalgrid as ng
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
        filename = savefilelocation + '/' + 'postcodes.' + sqName + '.' + plotter.lower() +'.png'
        pcplot.writeImageArrayToFileUsing(filename, img, plotter=plotter)

def showAllGB(df, plotter='CV2', savefilelocation=None) :
    if plotter == 'CV2' :
        img = pcplot.cv2plotSpecific(df, title='All GB', canvas_h=800, density=1)
        if savefilelocation != None :
            filename = savefilelocation + '/' + 'postcodes.allGB.cv.png'
            pcplot.writeImageArrayToFileUsingCV2(filename, img)
    else :
        pcplot.tkplotSpecific(df, title='All GB', canvas_h=800, density=10)

def getCacheFilePath(tmpDir) :
    return tmpDir + '/cached/df.cache'

def readCachedDataFrameFromFile(tmpDir, cacheFile=None) :

    if cacheFile == None :
        cacheFile = getCacheFilePath(tmpDir)
    print()
    print(f'Reading dataframe from cache file {cacheFile} ..')
    if os.path.isfile(cacheFile) :
        print(f'.. cache file {cacheFile} found ..')

        with open(cacheFile, 'rb') as f:
            response = pickle.load(f)
            print(f'.. read pre-existing response from cache file')
    else :
        print(f'*** No cache file {cacheFile} found.')
        response = pd.DataFrame()

    return response

def writeCachedDataFrame(df, tmpDir, cacheFile=None) :
    if cacheFile == None :
        cacheFile = getCacheFilePath(tmpDir)
    print()
    print(f'Writing dataframe to cache file {cacheFile} ..')
    
    cacheFileDir = os.path.dirname(cacheFile)
    if not os.path.isdir(cacheFileDir) :
        os.makedirs(cacheFileDir)
        print(f'.. created cache location {cacheFileDir}')

    # Use pickle to cache the response data.
    with open(cacheFile, 'wb') as f:
        pickle.dump(df, f)
        print(f'.. written dataframe as binary object to cache file {cacheFile}')

# Default location for files read and tmp working area
defaultOSZipFile = "./OSData/PostCodes/codepo_gb.zip"
defaultPostcodeAreasFile = "./OSData/PostCodes/postcode_district_area_lists.xls"
defaultTempDir = os.path.dirname(defaultOSZipFile) + '/tmp'

import argparse

def addStandardArgumentOptions(subparser) :
    subparser.add_argument('-v', '--verbose', action='store_true', help='Show some diagnostics')
    subparser.add_argument('-t', '--tempdir', 
                        help='Set the temporary directory location (used for unzipping data and as the default cache location)')

    subparser.add_argument('-c', '--cachefile', help='Specify the location for the dataframe cache file')

def addPlotterArgumentOption(subparser) :
    subparser.add_argument('-p', '--plotter', choices=['CV2', 'TK'], default='CV2', help='Plot using CV2 (OpenCV) or TK')

def main() :

    # https://docs.python.org/3/library/argparse.html#argparse.ArgumentParser.parse_args
    parser = argparse.ArgumentParser(description='OS Code-Point Postcode data processing program')
    subparsers = parser.add_subparsers(help='sub-command help')

    subparser = subparsers.add_parser('generate', help='read OS data files to generate a cached dataframe for use with other commands')
    subparser.set_defaults(cmd='generate')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('df_info', help='Show info about the Pandas dataframe structure')
    subparser.set_defaults(cmd='df_info')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('stats', help='Produce stats and aggregates relating to the postcodes dataset')
    subparser.set_defaults(cmd='stats')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('to_csv', help='Produce a csv file containing the postcodes dataset')
    subparser.set_defaults(cmd='to_csv')
    subparser.add_argument('-o', '--outfile', type=argparse.FileType('w'), help='Specify the location for the output CSV file)')
    addStandardArgumentOptions(subparser)
    
    subparser = subparsers.add_parser('info', help='Display info about a specified postcode')
    addPlotterArgumentOption(subparser)
    subparser.add_argument('postcode', help='the postcode of interest, in quotes if it contains any spaces')
    subparser.set_defaults(cmd='info')
    addStandardArgumentOptions(subparser)

    subparser = subparsers.add_parser('plot', help='Plot a map around the specified postcode')
    addStandardArgumentOptions(subparser)
    addPlotterArgumentOption(subparser)
    subparser.add_argument('place', help='Identifies the area to be plotted, in quotes if it contains any spaces')
    subparser.set_defaults(cmd='plot')

    parsed_args = parser.parse_args()
    #print(type(parsed_args))
    #print(parsed_args)
    d = vars(parsed_args)
    print(d)

    verbose = parsed_args.verbose
    cachefile = parsed_args.cachefile

    if parsed_args.tempdir != None :
        tmpDir = parsed_args.tempdir
        if not os.path.isdir(tmpDir) :
            print(f'Temporary directory {tmpDir} does not exist.')
            return 1
    else :
        tmpDir = defaultTempDir
    
    # ???? Allow these to be relocated ????
    OSZipFile = defaultOSZipFile
    postcodeAreasFile = defaultPostcodeAreasFile
    outputFileLocation = './pngs'

    if not 'cmd' in d:
        parser.print_help()
        return 1
    elif parsed_args.cmd == 'generate' :
        print('generate .. command')
        # Handle verbose, alternative file location options
        df = pcgen.regenerateDataFrame(OSZipFile, tmpDir, postcodeAreasFile, verbose=verbose)
        if not df.empty :
            writeCachedDataFrame(df, tmpDir, cachefile)
        else :
            return 1
    else :
        # Need to retrieve cached data
        df = readCachedDataFrameFromFile(tmpDir, cachefile)
        if df.empty :
            print()
            print('*** No dataframe read from cache')
            return 1
        
        if parsed_args.cmd == 'info' :
            print(f'info .. command for {parsed_args.postcode}')
            displayExample(df, example=parsed_args.postcode, plotter=parsed_args.plotter, dimension=10)
        elif parsed_args.cmd == 'df_info' :
            print('Run df_info command ...')
            displayBasicInfo(df)
        elif parsed_args.cmd == 'stats' :
            # Could have a geog component.
            print('Run stats command ...')
            aggregate(df)
        elif parsed_args.cmd == 'to_csv' :
            print('Run to_csv command ...')
            # ???? Where to save to ????
            saveAsCSV(df, tmpDir)
        elif parsed_args.cmd == 'plot' :
            print(f'Run plot command for "{parsed_args.place}" ..')
            # Work out if the place is a postcode, a grid square, <others?> or 'all'
            # Allow for spaces, and case. Further options:
            # county, post area, post district, district/ward/borough etc, ONS unit code ?
            # Or just a rectangle identified using NG top-left, bottom right/dimensions.
            # Can these overlap ? E.g. Post area = NG square (populated). Certainly town names can clash.
            # Plotter control, dimensions control, control of whether or not to save as png and where
            
            if parsed_args.place == 'all' :
                showAllGB(df, plotter=parsed_args.plotter, savefilelocation=outputFileLocation)
            elif ng.checkGridSquareName(parsed_args.place) :
                showGridSquare(df, parsed_args.place, plotter=parsed_args.plotter, savefilelocation=outputFileLocation)
            else :
                status = showPostcode(df, parsed_args.place, plotter=parsed_args.plotter, savefilelocation=outputFileLocation)
            #else :
            #    # How best to handle this
            #    #print()
            #    #print(f'*** unrecognised place {parsed_args.area} to plot')
            #    return
            # ???? Handle arbitrary areas ?
        else :
            print(f'Unrecognised command: {parsed_args.cmd}')
            return 1

    return 0    # Not always - set status

if __name__ == '__main__' :
    status = main()
    sys.exit(status)
