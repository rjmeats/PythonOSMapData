# Python reporting.

import pandas as pd

# In progress.

#############################################################################################

# ???? To be redone.
def produceStats(df, verbose=False) :

    print(f'############### Grouping by PostcodeArea, all columns ###############')
    print()
    dfAreaCounts = df.groupby('Postcode_area').count()
    print(f'Shape is {dfAreaCounts.shape}')
    print()
    print(dfAreaCounts)

    groupByColumns = [ 'Postcode_area', 'Quality', 'Country_code', 'Admin_county_code', 'Admin_district_code', 
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
    dfAreaCounts = df[['Postcode_area']].groupby('Postcode_area').count()
    print()
    print(f'############### Grouping by PostcodeArea with itself ###############')
    print()
    print(f'Shape is {dfAreaCounts.shape}')
    print()
    print(dfAreaCounts)

    return 0
    
#############################################################################################

def displayBasicDataFrameInfo(df, verbose=False) :
    '''See what some basic pandas info calls show about the dataframe.'''

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


# Code point Open User Guide explains the Quality values as follows:
# https://www.ordnancesurvey.co.uk/documents/product-support/user-guide/code-point-open-user-guide.pdf
# 
# 10 Within the building of the matched address closest to the postcode mean determined automatically by Ordnance Survey.
# 20 As above, but determined by visual inspection by NRS.
# 30 Approximate to within 50 m of true position (postcodes relating to developing sites may be within 100 m of true position).
# 40 The mean of the positions of addresses previously matched in PALF but that have subsequently been deleted or recoded (very rarely used).
# 50 Estimated position based on surrounding postcode coordinates, usually to 100 m resolution, but 10 m in Scotland.
# 60 Postcode sector mean (direct copy from PALF). See glossary for additional information.
# 90 No coordinates available.
#
# Observation of data shows that:
# - for values of 60 and 90, District and Ward information is always missing
# - for values of 90, Eastings and Northings values are set to 0
# - [County information is much more variable - absent for all 60 and 90 examples, but also in many 10,20,30 and 50.]
# for values 10,20,30,50 all information is present
# No cases of 40 were present in the data.
# Nearly all data is assigned as 10, with a small amount of 50 and 90, and traces of 20,30,60

# Perhaps redo this as a check of expectations rather than a report. (Or have a separate report.)
def examineLocationColumns(df, verbose=False) :

    if not verbose :
        return

    dfG = df[ ['Postcode', 'Postcode_area', 'Quality', 'Eastings', 'Northings', 'County Name', 'District Name', 'Ward Name'] ].groupby('Quality').count()
    print()
    print('Counts of non-null values by location quality:')
    print()
    print(dfG)

    dfG = df[ ['Postcode', 'Postcode_area', 'Quality', 'Eastings', 'Northings'] ].groupby('Quality').agg(
                Cases = ('Quality', 'count'),
                Min_E = ('Eastings', 'min'),
                Max_E = ('Eastings', 'max'),
                Min_N = ('Northings', 'min'),
                Max_N = ('Northings', 'max')
                    )
    print()
    print('Eastings and Northing ranges by location quality:')
    print()
    print(dfG)

    print()
    print('Quality=60 examples')
    print()
    print(df[ df['Quality'] == 60][0:10])
    print()
    print('Quality=90 examples:')
    print()
    print(df[ df['Quality'] == 90][0:10])

# Pulled out of addPostcodeBreakdown - should be part of some sort of analysis option.
def examinePostcodePatterns(df, verbose=True) :
    # This is more like data examination than useful processing info. Do some other way.
    if verbose :
        print()
        print(f'.. Postcodes grouped by pattern ..')
        dfG = df[['Postcode', 'Pattern']].groupby(['Pattern'], as_index=True).count()
        print()
        print(dfG)
        with pd.option_context('display.max_rows', 20000):
            print()
            print(f'.. Postcodes grouped by Area and Outward ..')
            dfG = df[['Postcode', 'Postcode_area', 'Post Town', 'Outward']].groupby(['Postcode_area', 'Post Town', 'Outward'], as_index=True).count()
            print()
            print(dfG)
            print()
            print(f'.. Unique Districts per Area ..')
            dfG = df[['Postcode', 'Postcode_area', 'Post Town', 'Outward']].groupby(['Postcode_area', 'Post Town'], as_index=True)['Outward'].nunique()
            print()
            print(dfG)



r"""

Reporting codes usage

    verbose = True
    if verbose :
        if reportCodeUsage > 0 :
            reportingColumns = ['Postcode', mainDataFrameCodeJoinColumn, lookupTableValueColumn]
            dfReportGroup = dfJoin[reportingColumns].groupby([mainDataFrameCodeJoinColumn, lookupTableValueColumn], as_index=False).count()
            print()
            print(f'{dfReportGroup.shape[0]} different {mainDataFrameCodeJoinColumn} values in use:')
            if dfReportGroup.shape[0] > reportCodeUsage :
                print(f'{indent}.. listing the first {reportCodeUsage} cases.')
            print()
            for index, row in dfReportGroup[0:reportCodeUsage].iterrows() :
                print(f'{indent}  {row.values[0]:10.10} {row.values[1]:30.30} {row.values[2]:7} rows')
            if dfReportGroup.shape[0] > reportCodeUsage :
                print(f'{indent}.. and {dfReportGroup.shape[0] - reportCodeUsage} more cases ..')


"""
