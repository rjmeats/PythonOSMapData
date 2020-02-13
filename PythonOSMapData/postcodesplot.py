import os

from cv2 import cv2
from tkinter import Tk, Canvas, mainloop

pcDefaultColour = "black"
pcDefaultColourRGB = (128,128,128)

def assignAreasToColourGroups(df, verbose=False) :
    # Determine extent of each Postcode Area

    if verbose :
            print('Determining Eastings and Northing ranges by Postcode area:')

    # Ignore 0s
    df = df [ df['Eastings'] != 0 ]
    dfAreaExtents = df[ ['PostcodeArea', 'Eastings', 'Northings'] ].groupby('PostcodeArea').agg(
                Cases = ('PostcodeArea', 'count'),
                Min_E = ('Eastings', 'min'),
                Max_E = ('Eastings', 'max'),
                Min_N = ('Northings', 'min'),
                Max_N = ('Northings', 'max')
                    )
    dfAreaExtents.sort_values('Min_E', inplace=True)
    if verbose :
        print()
        print('Eastings and Northing ranges by Postcode area:')
        print()
        print(type(dfAreaExtents))
        print()
        print(dfAreaExtents)

    # ???? Algorithm to assign areas to colour groups so that close areas don't use the same colour. 
    # For now just use lots of colours and rely on chance ! 
    # https://www.tcl.tk/man/tcl8.4/TkCmd/colors.htm
    # Doesn't seem to work very well = e.g. YO (York) and TS (Teeside) have same colour, and also WC and SE in London. Also
    # PE (Peterborough) and MK (Milton Keynes). IG/RM/SS form a triple ! Probably more ..
    # https://stackoverflow.com/questions/309149/generate-distinctly-different-rgb-colors-in-graphs has lots of colours about half way down
    availableColours = [ "red", "blue", "green", "yellow", "orange", "purple", "brown", 
                            "pink", "cyan2", "magenta2", "violet", "grey"]
    availableColoursRGB = [ (255,0,0), (0,0,255), (0,255,0), (255,255,0), (255,165,0), (160,32,240), (165,42,42), 
                            (255,192,203), (0,238,238), (238,0,238), (238,130,238), (190,190,190)]

    # Soften the colours
    ac2 = []
    for c in availableColoursRGB :
        n = [0,0,0]
        for i in (0,1,2) :
            ci = c[i]
            if ci < 255 :
                ci = int(ci + (255-ci) * 0.5)
            n[i] = ci
        ac2.append( (n[0],n[1],n[2]))

    if verbose :
        print(ac2)

    availableColoursRGB = ac2

    numGroups = len(availableColours)

    # Set up a list of lists, one per colour
    colourGroupsList = []
    for i in range(numGroups) :
        colourGroupsList.append([])

    # Spread the Postcode areas across these lists
    for index, (row) in enumerate(dfAreaExtents.iterrows()) :
        colourGroupsList[index % numGroups].append(row[0]) 

    # Produce a dictionary to map each Postcode Area to its colour
    d = {}
    dRGB = {}
    for i in range(numGroups) :
        for a in colourGroupsList[i] :
            d[a] = availableColours[i]
            dRGB[a] = availableColoursRGB[i]

    return d, dRGB


tkObjDict = {}
dfClickLookup = None

# https://stackoverflow.com/questions/20399243/display-message-when-hovering-over-something-with-mouse-cursor-in-python

def onObjectClick(event):                  
    print('Got tk object click', event.x, event.y)
    #print(event.widget.find_closest(event.x, event.y))
    objID = event.widget.find_closest(event.x, event.y)[0]
    #print(type(objID))
    pc = tkObjDict[objID]
    pcinfo = dfClickLookup [dfClickLookup['Postcode'] == pc]

    print(f'obj={objID} : pc={pc}')
    print(pcinfo)

def tkplotSpecific(df, title='', canvasHeight=800, bottomLeft=(0,0), topRight=(700000,1250000), density=100, keyPostcode=None) :

    canvasHeight, canvas_width, dfSlice = getScaledPlot(df, canvasHeight, bottomLeft, topRight, density)
    areaColourDict, areaColourDictRGB = assignAreasToColourGroups(df)

    global dfClickLookup
    dfClickLookup = df

    # Vary width of oval dot depending on density ????
    # Do we need to use 'oval's to do the plotting, rather than points ?

    # https://effbot.org/tkinterbook/canvas.htm
    master = Tk()
    w = Canvas(master, width=canvas_width, height=canvasHeight)
    master.title(title)
    w.pack()

    for index, r in enumerate(zip(dfSlice['e_scaled'], dfSlice['n_scaled'], dfSlice['Postcode'], dfSlice['PostcodeArea'])):
        (es, ns, pc, area) = r
        if index % (100000/density) == 0 :
            print(index, es, ns, pc)
        colour = areaColourDict.get(area, pcDefaultColour)
        objid = w.create_oval(es,canvasHeight-ns,es,canvasHeight-ns, fill=colour, outline=colour, width=2)
        w.tag_bind(objid, '<ButtonPress-1>', onObjectClick)    
        #print(f'Adding objid: {objid}')
        tkObjDict[objid] = pc

    mainloop()

import numpy as np

def newImageArray(y, x) :
    """ Create a new image array of dimensions [y,x,RGB], set to all white """
    return np.full((y,x,3), 255, dtype='uint8')

def convertToBGR(imgArray) :
    """ Converts a 3-D [y,x,RGB] numpy array to [y,x,BGR] format, (for use with CV2) """
    return imgArray[:,:,::-1]

def getScaledPlot(df, canvasHeight=1000, bottomLeft=(0,0), topRight=(700000,1250000), density=100) :
    e0 = bottomLeft[0]
    e1 = topRight[0]
    n0 = bottomLeft[1]
    n1 = topRight[1]
    e_extent = e1 - e0
    n_extent = n1 - n0

    print(f'canvasHeight = {canvasHeight} : bottomLeft = {bottomLeft} : topRight = {topRight}')
    canvasHeight = int(canvasHeight)                        # pixels
    canvas_width = int(canvasHeight * e_extent / n_extent)   # pixels
    scaling_factor = n_extent / canvasHeight            # metres per pixel

    print(f'.. scale = {scaling_factor} : canvas_width = {canvas_width}')

    # Could do bulk e/n scaling first too in bulk ?
    # Keep more Scotland (and perhaps Wales, and perhaps more generally remote areas) to maintain shape of landmass ?
    if density != 1 :
        dfSlice = df.copy().iloc[::density]
    else :
        dfSlice = df.copy()
    #print(dfSlice.dtypes)

    dfSlice['e_scaled'] = (dfSlice['Eastings'] - e0) // scaling_factor
    dfSlice['n_scaled'] = (dfSlice['Northings'] - n0) // scaling_factor
    #print(dfSlice.dtypes)
    # Above can produce float type columns for e_scaled and n_scaled, which ends up being used to plot a non-integer rectangle.
    # Convert to int for now, (= truncation?). Probably better to round/convert to int much later on when calling cv2.rectangle.
    dfSlice = dfSlice.astype({'e_scaled': 'int32', 'n_scaled': 'int32'})
    #print(dfSlice.dtypes)
    #dfSlice.astype(int)
    #print(dfSlice)

    dfSlice = dfSlice [ dfSlice['Eastings'] > 0 ]
    dfSlice = dfSlice [ (dfSlice['n_scaled'] >= 0) & (dfSlice['n_scaled'] <= canvasHeight)]
    dfSlice = dfSlice [ (dfSlice['e_scaled'] >= 0) & (dfSlice['e_scaled'] <= canvas_width)]

    # Why can't the above be combined into one big combination ?
    # Also, any way to check n_scale and e_scaled are in a range ?
    #dfSlice = dfSlice [ dfSlice['Eastings'] > 0 &
    #                    ((dfSlice['n_scaled'] >= 0) & (dfSlice['n_scaled'] <= canvasHeight)) &
    #                    ((dfSlice['e_scaled'] >= 0) & (dfSlice['e_scaled'] <= canvas_width)) ]

    return canvasHeight, canvas_width, dfSlice

# Map 
from cv2 import cv2

img = None
imgLookupIndex = None


def CV2ClickEvent(event, x, y, flags, param):
    if event == cv2.EVENT_LBUTTONDOWN:
        yy = img.shape[0] - y
        print (f'CV2 event: event={event}, x={x}, y={y} == {yy}, flags={flags}, param={param}')
        # y is first dimension of image - need to subtract from height
        # NB RGB in img array, convertToBGR converts just before CV2 sees it
        red = img[y,x,0]
        green = img[y,x,1]
        blue = img[y,x,2]
        print (f'{red} : {blue} : {green}')
        if red == 255 and green == 255 and blue == 255 :
            print('.. white')
        else :
            print('.. point - looking up')
            index = imgLookupIndex[yy,x]
            pcinfo = dfClickLookup.iloc[index]
            print(f'index={index}')
            print(pcinfo)

def cv2plotSpecific(df, title='', canvasHeight=800, bottomLeft=(0,0), topRight=(700000,1250000), density=100, keyPostcode=None) :

    canvasHeight, canvas_width, dfSlice = getScaledPlot(df, canvasHeight, bottomLeft, topRight, density)
    areaColourDict, areaColourDictRGB = assignAreasToColourGroups(df)

    print()
    print(dfSlice[0:1].T)

    global img
    global dfClickLookup
    global imgLookupIndex
    img = newImageArray(canvasHeight, canvas_width)
    imgLookupIndex = np.full((canvasHeight+2, canvas_width+2), 0, dtype='int32')   # +2s partly because of size of circle,
    # but also getting some out of bounds errors before [-1,0,1] adjustment was there - because ????
    dfClickLookup = dfSlice

    #print(dfSlice.dtypes)
    
    foundKey = False
    esKey = -1
    nsKey = -1
    areaKey = ''

    for index, r in enumerate(zip(dfSlice['e_scaled'], dfSlice['n_scaled'], dfSlice['Postcode'], dfSlice['PostcodeArea'])):
        (es, ns, pc, area) = r
        if index % (100000/density) == 0 :
            print(index, es, ns, pc)
            #print('...', type(index), type(es), type(ns), type(pc))
        colour = areaColourDictRGB.get(area, pcDefaultColourRGB)
        #circle radius = 0 == single pixel. 
        #                          x
        # radius=1 gives a cross: xxx
        #                          x
        # cv2.circle(img, center=(es, canvasHeight-ns), radius=0, color=colour, thickness=-1)

        if keyPostcode != None and pc == keyPostcode :
            esKey = es
            nsKey = ns
            areaKey = area
            foundKey = True

        x = 2   # Seems to work well
        cv2.rectangle(img, pt1=(es, canvasHeight-ns), pt2=(es+x, canvasHeight-ns+x), color=colour, thickness=-1)
        #if index % (1000/density) == 0 :
        #    print('...', (es, canvasHeight-ns), ' : ', (es+x, canvasHeight-ns+x))

        # Record item against a small 3x3 square of points, not just the central one. Why is this necessary,
        # how does it relate to the radius value ? What about close postcodes overwriting each other ?
        for i in [-1,0,1] :
            for j in [-1,0,1] :
                pass
                imgLookupIndex[ns+i, es+j] = index

    # Show a specific postcode more prominently. ???? Do this in TK ?
    if foundKey :
        overlay = img.copy()
        x = 30
        colour = areaColourDictRGB.get(areaKey, pcDefaultColourRGB)
        cv2.circle(overlay, center=(esKey, canvasHeight-nsKey), radius=x, color=colour, thickness=-1)
        alpha = 0.5
        img = cv2.addWeighted(overlay, alpha, img, 1-alpha, 0)

    #cvtitle = 'Title' if title == None else title
    #v_km = (topRight[0] - bottomLeft[0]) // 1000
    #h_km = (topRight[1] - bottomLeft[1]) // 1000
    #dimensions = f'{v_km} km x {h_km} km'
    #cvtitle = f'{cvtitle} : {dimensions}'
    # For CV2 we need to reverse the colour ordering of the array to BGR
    cv2.imshow(title, convertToBGR(img))
    cv2.setMouseCallback(title, CV2ClickEvent)
    cv2.waitKey(0)
    cv2.destroyAllWindows()

    return img

def plotSpecific(df, title=None, canvasHeight=1000, bottomLeft=(0,0), topRight=(700000,1250000), density=1, keyPostcode=None, plotter='CV2') :

    # ???? NB these dimensions include the margin - not aware of this aspect here. Is margin been defined too
    # early ? Especially noticeable for National Grid squares ought to br 100 x 100, but end up showing as larger.
    v_km = (topRight[0] - bottomLeft[0]) // 1000
    h_km = (topRight[1] - bottomLeft[1]) // 1000
    dimensions = f'{v_km} km x {h_km} km'
    fullTitle = dimensions if title == None else f'{title} : {dimensions}'

    img = None
    if plotter.upper() == 'CV2' :
        img = cv2plotSpecific(df, title=fullTitle, canvasHeight=800, density=1, bottomLeft=bottomLeft, topRight=topRight, keyPostcode=keyPostcode)
    elif plotter.upper() == 'TK' :
        tkplotSpecific(df, title=fullTitle, canvasHeight=800, density=1, bottomLeft=bottomLeft, topRight=topRight, keyPostcode=keyPostcode)
    else :
        print(f'*** Unknown plotter specified: {plotter}')

    return img

def writeImageArrayToFile(filename, img, plotter='CV2') :
    if plotter == 'CV2' :
        writeImageArrayToFileUsingCV2(filename, img)
    else :
        writeImageArrayToFileUsingTK(filename, img)

def writeImageArrayToFileUsingCV2(filename, img) :

    outDir = os.path.dirname(filename)
    if not os.path.isdir(outDir) :
        print(f'Creating directory {outDir} ..')
        os.makedirs(outDir)

    if cv2.imwrite(filename, convertToBGR(img)) :
        print(f'Image file saved as: {filename}')
    else :
        print(f'*** Failed to save image file as: {filename}')

def writeImageArrayToFileUsingTK(filename, img) :
    print()
    print(f'*** TK plotter save-to-file not implemented - use the CV2 plotter instead.')
