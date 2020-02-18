import os

from cv2 import cv2
from tkinter import Tk, Canvas, mainloop
import pandas as pd
import numpy as np

def getPlotter(plotter) :
    if plotter.upper() == 'CV2' :
        return CV2PostcodesPlotter()
    elif plotter.upper() == 'TK' :
        return TKPostcodesPlotter()
    else :
        print(f'*** Unknown plotter type: {plotter} - using CV2')
        return CV2PostcodesPlotter()

class postcodesPlotter() :
    
    pcDefaultColourRGB = (128,128,128)

    def areaTypeToColumnName(self, areaType) :
        if areaType.lower() == 'pa' :
            areaTypeColumn = 'PostcodeArea'
        elif areaType.lower() == 'ow' :
            areaTypeColumn = 'Outward'
        elif areaType.lower() == 'wd' :
            areaTypeColumn = 'Ward Name'
        elif areaType.lower() == 'pc' :
            areaTypeColumn = 'Postcode'
        else :
            print(f'*** Unrecognised areaType {areaType} when converting to a column name.')
            areaTypeColumn = 'PostcodeArea'

        return areaTypeColumn

    def rgbTupleToHexString(self, rgb) :
        r,g,b = rgb
        return f'#{r:02x}{g:02x}{b:02x}'

    def assignAreasToColourGroups(self, df, areaType='pa', verbose=False) :

        areaTypeColumn = self.areaTypeToColumnName(areaType)

        if verbose :
                print(f'Determining Eastings and Northing ranges by {areaTypeColumn}:')

        # Determine extent of each Postcode Area
        # Ignore 0s
        df = df [ df['Eastings'] != 0 ]
        dfAreaExtents = df[ [areaTypeColumn, 'Eastings', 'Northings'] ].groupby(areaTypeColumn).agg(
                    Cases = (areaTypeColumn, 'count'),
                    Min_E = ('Eastings', 'min'),
                    Max_E = ('Eastings', 'max'),
                    Min_N = ('Northings', 'min'),
                    Max_N = ('Northings', 'max')
                        )
        dfAreaExtents.sort_values('Min_E', inplace=True)
        if verbose :
            print()
            print(f'Eastings and Northing ranges by {areaTypeColumn}:')
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
        #availableColours = [ "red", "blue", "green", "yellow", "orange", "purple", "brown", 
        #                        "pink", "cyan2", "magenta2", "violet", "grey"]
        availableColoursRGB = [ (255,0,0), (0,0,255), (0,255,0), (255,255,0), (255,165,0), (160,32,240), (165,42,42), 
                                (255,192,203), (0,238,238), (238,0,238), (238,130,238), (190,190,190)]

        # Soften the RGB colours
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

        numGroups = len(availableColoursRGB)

        # Set up a list of lists, one per colour
        colourGroupsList = []
        # Candidate for defaultDict ?
        for i in range(numGroups) :
            colourGroupsList.append([])

        # Spread the Postcode areas across these lists
        for index, (row) in enumerate(dfAreaExtents.iterrows()) :
            colourGroupsList[index % numGroups].append(row[0]) 

        # Produce a dictionary to map each Postcode Area to its colour
        d = {}
        for i in range(numGroups) :
            for a in colourGroupsList[i] :
                d[a] = availableColoursRGB[i]

        return d

    def getScaledPlot(self, df, canvasHeight=800, bottomLeft=(0,0), topRight=(700000,1250000), density=100) :
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

    def __init__(self) :
        pass

    def _initialisePlot(self, title, canvasHeight, canvas_width) :
        self.title = title
        self.canvasHeight = canvasHeight
        self.canvas_width = canvas_width

    def plotSpecific(self, df, title=None, canvasHeight=800, bottomLeft=(0,0), topRight=(700000,1250000), density=1, 
                colouringAreaType = 'pa', keyPostcode=None) :

        # ???? NB these dimensions include the margin - not aware of this aspect here. Is margin been defined too
        # early ? Especially noticeable for National Grid squares ought to br 100 x 100, but end up showing as larger.
        v_km = (topRight[0] - bottomLeft[0]) // 1000
        h_km = (topRight[1] - bottomLeft[1]) // 1000
        dimensions = f'{v_km} km x {h_km} km'
        fullTitle = dimensions if title == None else f'{title} : {dimensions}'

        canvasHeight, canvas_width, dfSlice = self.getScaledPlot(df, canvasHeight, bottomLeft, topRight, density)
        colouringColumn = self.areaTypeToColumnName(colouringAreaType)
        areaColourDict = self.assignAreasToColourGroups(dfSlice, colouringAreaType)

        self._initialisePlot(fullTitle, canvasHeight, canvas_width)
        self.dfClickLookup = dfSlice

        foundKey = False
        esKey = -1
        nsKey = -1
        areaKey = ''

        for index, r in enumerate(zip(dfSlice['e_scaled'], dfSlice['n_scaled'], dfSlice['Postcode'], dfSlice[colouringColumn])):
            (es, ns, pc, area) = r
            if index % (100000) == 0 :
                print(index, es, ns, pc)
            rgbColour = areaColourDict.get(area, self.pcDefaultColourRGB)
            self._drawPostcode(index, es, ns, pc, area, rgbColour)

            if keyPostcode != None and pc == keyPostcode :
                esKey = es
                nsKey = ns
                areaKey = area
                foundKey = True

        # Show a specific postcode more prominently. ????
        if foundKey :
            rgbColour = areaColourDict.get(areaKey, self.pcDefaultColourRGB)
            self._highlightKeyPostcode(esKey, nsKey, keyPostcode, areaKey, rgbColour)

        self._displayPlot()

        return self._getImage()

    def _drawPostcode(self, index, es, ns, pc, area, rgbColour) :
        pass

    def _highlightKeyPostcode(self, esKey, nsKey, keyPostcode, areaKey, rgbColour) :
        pass

    def _getImage(self) :
        return None

    def _displayPlot(self) :
        pass

    def writeImageArrayToFile(self, filename, img) :
        pass

#############################################################################################

class TKPostcodesPlotter(postcodesPlotter) :

    def __init__(self) :
        # Avoid is unsubscriptable error bodge for now.
        super().__init__()
        self.tkObjDict = {}
        self.dfClickLookup = pd.DataFrame()

    # https://stackoverflow.com/questions/20399243/display-message-when-hovering-over-something-with-mouse-cursor-in-python

    def onTKObjectClick(self, event):                  
        print('Got tk object click', event.x, event.y)
        #print(event.widget.find_closest(event.x, event.y))
        objID = event.widget.find_closest(event.x, event.y)[0]
        #print(type(objID))
        pc = self.tkObjDict[objID]
        pcinfo = self.dfClickLookup [self.dfClickLookup['Postcode'] == pc]

        print(f'obj={objID} : pc={pc}')
        print(pcinfo)

    def _initialisePlot(self, title, canvasHeight, canvas_width) :
        super()._initialisePlot(title, canvasHeight, canvas_width)
        self.master = Tk()
        self.w = Canvas(self.master, width=canvas_width, height=canvasHeight)
        self.master.title(title)
        self.w.pack()

    def _displayPlot(self) :
        mainloop()

    def _getImage(self) :
        return None

    def _drawPostcode(self, index, es, ns, pc, area, rgbColour) :
        colour = self.rgbTupleToHexString(rgbColour)
        objid = self.w.create_oval(es,self.canvasHeight-ns,es,self.canvasHeight-ns, fill=colour, outline=colour, width=2)
        self.w.tag_bind(objid, '<ButtonPress-1>', self.onTKObjectClick)    
        #print(f'Adding objid: {objid}')
        self.tkObjDict[objid] = pc

    def _highlightKeyPostcode(self, es, ns, pc, area, rgbColour) :
        pass

    def writeImageArrayToFileUsing(self, filename, img) :
        print()
        print(f'*** TK plotter save-to-file not implemented - use the CV2 plotter instead.')

#############################################################################################

class CV2PostcodesPlotter(postcodesPlotter) :

    def newImageArray(self, y, x) :
        """ Create a new image array of dimensions [y,x,RGB], set to all white """
        return np.full((y,x,3), 255, dtype='uint8')

    def convertToBGR(self, imgArray) :
        """ Converts a 3-D [y,x,RGB] numpy array to [y,x,BGR] format, (for use with CV2) """
        return imgArray[:,:,::-1]

    def __init__(self) :
        # Avoid is unsubscriptable error bodge for now.
        super().__init__()
        self.img = self.newImageArray(10,10)    
        self.imgLookupIndex = np.full((10, 10), 0, dtype='int32')
        self.dfClickLookup = None

    def CV2ClickEvent(self, event, x, y, flags, param):
        if event == cv2.EVENT_LBUTTONDOWN:
            yy = self.img.shape[0] - y
            print (f'CV2 event: event={event}, x={x}, y={y} == {yy}, flags={flags}, param={param}')
            # y is first dimension of image - need to subtract from height
            # NB RGB in img array, convertToBGR converts just before CV2 sees it
            red = self.img[y,x,0]
            green = self.img[y,x,1]
            blue = self.img[y,x,2]
            print (f'{red} : {blue} : {green}')
            if red == 255 and green == 255 and blue == 255 :
                print('.. white')
            else :
                print('.. point - looking up')
                index = self.imgLookupIndex[yy,x]
                pcinfo = self.dfClickLookup.iloc[index]
                print(f'index={index}')
                print(pcinfo)

    def _initialisePlot(self, title, canvasHeight, canvas_width) :
        super()._initialisePlot(title, canvasHeight, canvas_width)
        self.img = self.newImageArray(canvasHeight, canvas_width)
        self.imgLookupIndex = np.full((canvasHeight+2, canvas_width+2), 0, dtype='int32')   # +2s partly because of size of circle,
        # but also getting some out of bounds errors before [-1,0,1] adjustment was there - because ????

    def _displayPlot(self) :
        # For CV2 we need to reverse the colour ordering of the array to BGR
        if self.title == '' :
            self.title = 'Title'
        cv2.imshow(self.title, self.convertToBGR(self.img))
        cv2.moveWindow(self.title, 200, 20)
        cv2.setMouseCallback(self.title, self.CV2ClickEvent)
        cv2.waitKey(0)
        cv2.destroyAllWindows()

    def _getImage(self) :
        return self.img

    def _drawPostcode(self, index, es, ns, pc, area, rgbColour) :
        #circle radius = 0 == single pixel. 
        #                          x
        # radius=1 gives a cross: xxx
        #                          x
        # cv2.circle(img, center=(es, canvasHeight-ns), radius=0, color=colour, thickness=-1)

        x = 2   # Seems to work well
        cv2.rectangle(self.img, pt1=(es, self.canvasHeight-ns), pt2=(es+x, self.canvasHeight-ns+x), color=rgbColour, thickness=-1)

        # Record item against a small 3x3 square of points, not just the central one. Why is this necessary,
        # how does it relate to the radius value ? What about close postcodes overwriting each other ?
        for i in [-1,0,1] :
            for j in [-1,0,1] :
                pass
                self.imgLookupIndex[ns+i, es+j] = index

    def _highlightKeyPostcode(self, es, ns, pc, area, rgbColour) :
        overlay = self.img.copy()
        x = 30
        cv2.circle(overlay, center=(es, self.canvasHeight-ns), radius=x, color=rgbColour, thickness=-1)
        alpha = 0.5
        self.img = cv2.addWeighted(overlay, alpha, self.img, 1-alpha, 0)

    def writeImageArrayToFileUsing(self, filename, img) :

        outDir = os.path.dirname(filename)
        if not os.path.isdir(outDir) :
            print(f'Creating directory {outDir} ..')
            os.makedirs(outDir)

        if cv2.imwrite(filename, self.convertToBGR(img)) :
            print(f'Image file saved as: {filename}')
        else :
            print(f'*** Failed to save image file as: {filename}')

