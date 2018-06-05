from collections import OrderedDict
import logging

import numpy as np

import opscore.protocols.types as types
from opscore.utility.qstr import qstr
import astropy.io.fits as pyfits

import fitsio

def extendHeader(cmd, header, cards):
    """ Add all the cards to the header. """

    for name, val, comment in cards:
        try:
            header.update(name, val, comment)
        except:
            cmd.warn('text="failed to add card: %s=%s (%s)"' % (name, val, comment))

def makeCard(cmd, name, value, comment=''):
    """ Creates a pyfits Card. Does not raise exceptions. """

    try:
        c = pyfits.Card(name, value, comment)
        return (name, value, comment)
    except:
        errStr = 'failed to make %s card from %s' % (name, value)
        cmd.warn('text=%s' % (qstr(errStr)))
        return ('comment', errStr, '')
        
def makeCardFromKey(cmd, keyDict, keyName, cardName, cnv=None, idx=None, comment='', onFail=None):
    """ Creates a pyfits Card from a Key. Does not raise exceptions. """

    try:
        val = keyDict[keyName]
    except KeyError as e:
        errStr = "failed to fetch %s" % (keyName)
        cmd.warn('text=%s' % (qstr(errStr)))
        return makeCard(cmd, cardName, onFail, errStr)

    try:
        if idx is not None:
            val = val[idx]
        else:
            val = val.getValue()
    except Exception as e:
        errStr = "failed to index %s by %s from %s for %s: %s" % \
            (val, idx, keyName, cardName, e)
        cmd.warn('text=%s' % (qstr(errStr)))
        return makeCard(cmd, cardName, onFail, errStr)

    if cnv is not None:
        try:
            val = cnv(val)
        except Exception as e:
            errStr = "failed to convert %s from %s for %s using %s: %s" % \
                (val, keyName, cardName, cnv, e)
            cmd.warn('text=%s' % (qstr(errStr)))
            return makeCard(cmd, cardName, onFail, errStr)
        
    return makeCard(cmd, cardName, val, comment)

def cardsFromModel(cmd, model, shortNames=False):
    """
    For a given actorkeys model, return a list of all the FITS cards listed therein.

    Args
    ----
    model : opscore.actor.Model
      Usually from self.actor.models[modelName]
    shortNames : bool
      Whether to limit ourselves to the sad short card names.

    Returns
    -------
    cards : array
       The cards, each as a dictionary suitable for fitsio

       We do something slightly iffy here: if we are asked to use
       shortNames, we add a "longName" slot in the dictionary. There
       is no guarantee that fitsio will contiue silently accepting
       extra keys. In that case the caller will need to pull the keys
       out.
    """

    logger = logging.getLogger('FITS')
    cards = []
    for mk, mv in model.keyVarDict.items():
        try:
            # When values are not current, they are structurally invalid. So iterate over the _types_,
            # then pick up the value only when necessary.
            for kv_i, kvt in enumerate(mv._typedValues.vtypes):
                if kvt.FITS is None:
                    continue

                shortCard, longCard = kvt.FITS

                # Hackery: bool cannot be subclassed, so we need to check the keyword class
                if issubclass(kvt.__class__, types.Bool):
                    baseType = bool
                else:
                    baseType = kvt.__class__.baseType

                logger.debug(f'FITS card:  {kv_i}({kvt.name}, {baseType} {kvt.__class__}) = {shortCard}, {longCard}"')

                if not mv.isCurrent:
                    logger.debug(f'text="SKIPPING NOT CURRENT {mk} = {mv}"')
                    value = "NO CURRENT VALUE"
                else:
                    # Now we can get the value
                    value=baseType(mv[kv_i])

                if shortNames:
                    card = dict(name=shortCard, value=value, longName=longCard)
                else:
                    card = dict(name=longCard, value=value)

                if kvt.units is not None:
                    comment = f'[{kvt.units}] '
                else:
                    comment = ''

                if kvt.help is not None:
                    comment += kvt.help

                if comment:
                    card['comment'] = comment

                cards.append(card)
        except Exception as e:
            cmd.warn(f'text="FAILED to generate FITS cards for {mk}: {e}"')
            cards.append(f'Failed to get FITS cards for MHS key {mk}')

    return cards

def makeNameTranslationHDU(cards):
    """ If we have both long and short FITS card names, generate the mapping for a translation HDU. """

    longToShort = OrderedDict()
    for c in cards:
        if 'longName' in c:
            longToShort[c['longName']] = c['name']
    if len(longToShort) == 0:
        return None

    longNames = [str(k) for k in longToShort.keys()]
    longNameLen = max([len(s) for s in longNames])
    mapping = np.zeros(len(longToShort), dtype=[('longName','S%d' % longNameLen),
                                                ('shortName', 'S8')])
    mapping['longName'] = longNames
    mapping['shortName'] = [str(v) for v in longToShort.values()]

    return mapping

def gatherHeaderCards(cmd, actor, modelNames=None, shortNames=False):
    """ Fetch and return all FITS cards defined in the given models.

    Args
    ----
    cmd : an actorcore Command
    actor : an actorcore Actor,
      Which contans .models.
    modelNames : None, or list of strings
      The actors to generate keys for. If None, all the models in actor.models
    shortNames : bool
      If True, use the short FITS names.

    Returns
    -------
    cards : list of fitsio-compatible card dictionaries.
      Suitable to get a header with "fitsio.FITSHDR(cards)"
    """

    logger = logging.getLogger('FITS')
    
    if modelNames is None:
        modelNames = actor.models.keys()

    allCards = []
    for modName in modelNames:
        logger.info(f'gathering cards from model {modName}')
        try:
            modCards = cardsFromModel(cmd, actor.models[modName], shortNames=shortNames)
            allCards.append(f'################################ Cards from {modName}')
            allCards.extend(modCards)
        except Exception as e:
            logger.warn(f' Failed to get FITS cards for actor {modName}: {e}')
            allCards.append(f' Failed to get FITS cards for actor {modName}')
            allCards.append(f' Exception: {e}')
            cmd.warn(f'text="FAILED to get FITS cards for actor {modName}: {e}"')

    return allCards

def startFitsFile(cmd, filename, cards,
                  img=None, clobber=False, doShortnameHDU=True):
    """ Write the PHDU and first HDU(s).

    Args
    ----
    cmd : an actorcore Command
    filename : string
       Full pathname for the FITS file.
    cards : list of fits cards
       fitsio compatible: strings or dictionaries
    img : None, or a 2-d image
    clobber : bool
       Whether to overwrite any existing file. In production, this should not happen.
    doShortnameHDU : bool
       Whether to create a translation HDU if we have short card names.

    """

    try:
        hdr = fitsio.FITSHDR()
    except Exception as e:
        hdr = None
        cmd.warn(f'text="FAILED to create FITS header!!!!: {e}')

    if hdr is not None:
        for c in cards:
            try:
                hdr.add_record(c)
            except Exception as e:
                cmd.warn(f'text="INVALID FITS card {c}: {e}')
                continue

    try:
        cmd.debug(f'text="starting FITS file {filename}"')
        ffile = fitsio.FITS(filename, mode='rw', clobber=clobber)
        compress = 'RICE' if img is not None else None
        ffile.write(data=img, header=hdr, compress=compress,
                    extname=('IMAGE' if img is not None else None))
        ffile.close()
        cmd.inform(f'text="wrote PHDU for {filename}"')
    except Exception as e:
        cmd.warn(f'text="FAILED to write {filename} PHDU: {e}"')
        return

    if doShortnameHDU:
        try:
            mapping = makeNameTranslationHDU(cards)
            if mapping is not None:
                fitsio.write(filename, data=mapping,
                             extname='shortNameMapping', table_type='ascii')
            cmd.diag(f'text="wrote long name translation HDU for {filename}"')
        except Exception as e:
            cmd.warn(f'text="FAILED to write translation HDU for {filename}: {e}"')

def simpleFits(cmd, actor, filename, modelNames=None, img=None, shortNames=False):
    allCards = gatherHeaderCards(cmd, actor, modelNames=modelNames,
                                 shortNames=shortNames)

    startFitsFile(cmd, filename, allCards, img=img, doShortNames=shortNames)

def mcpCards(models, cmd=None):
    """ Return a list of pyfits Cards describing the MCP state. """

    d = []

    mcpDict = models['mcp'].keyVarDict
    for lampKey in ('ffLamp', 'neLamp', 'hgCdLamp'):
        cardName = lampKey[:-4].upper()
        card = makeCardFromKey(cmd, mcpDict, lampKey, cardName,
                               cnv=_cnvListCard,
                               comment="%s lamps 1:on 0:0ff" % (cardName),
                               onFail="X X X X")
        d.append(card)

    def _cnvFFSCard(petals):
        """ Convert the mcp.ffsStatus keyword to what we want. """
        
        ffDict = {'01':'1', '10':'0'}
        return " ".join([str(ffDict.get(p,'X')) for p in petals])

    card = makeCardFromKey(cmd, mcpDict, 'ffsStatus', 'FFS',
                           cnv=_cnvFFSCard,
                           comment='Flatfield Screen 1:closed 0:open',
                           onFail='X X X X X X X X')
    d.append(card)

    return d

def tccCards(models, cmd=None):
    """ Return a list of pyfits Cards describing the TCC state. """

    cards = []

    tccDict = models['tcc'].keyVarDict

    try:
        objSys = tccDict['objSys']
        objSysName = str(objSys[0])
        objSysDate = float(objSys[1])
    except Exception as e:
        objSysName = 'unknown'
        objSysDate = 0.0
        if cmd:
            cmd.warn('text="could not get objsys and epoch from tcc.objSys=%s"' % (objSys))
    cards.append(makeCard(cmd, 'OBJSYS', objSysName, "The TCC objSys"))

    if objSysName in ('None', 'Mount', 'Obs', 'Phys', 'Inst'):
        cards.append(makeCard(cmd, 'RA', 'NaN', 'Telescope is not tracking the sky'))
        cards.append(makeCard(cmd, 'DEC', 'NaN', 'Telescope is not tracking the sky'))
        cards.append(makeCard(cmd, 'RADEG', 'NaN', 'Telescope is not tracking the sky'))
        cards.append(makeCard(cmd, 'DECDEG', 'NaN', 'Telescope is not tracking the sky'))
        cards.append(makeCard(cmd, 'SPA', 'NaN', 'Telescope is not tracking the sky'))
    else:
        cards.append(makeCardFromKey(cmd, tccDict, 'objNetPos', 'RA',
                                     cnv=_cnvPVTPosCard, idx=0,
                                     comment='RA of telescope boresight (deg)',
                                     onFail='NaN'))
        cards.append(makeCardFromKey(cmd, tccDict, 'objNetPos', 'DEC',
                                     cnv=_cnvPVTPosCard, idx=1,
                                     comment='Dec of telescope boresight (deg)',
                                     onFail='NaN'))
        cards.append(makeCardFromKey(cmd, tccDict, 'objPos', 'RADEG',
                                     cnv=_cnvPVTPosCard, idx=0,
                                     comment='RA of telescope pointing(deg)',
                                     onFail='NaN'))
        cards.append(makeCardFromKey(cmd, tccDict, 'objPos', 'DECDEG',
                                     cnv=_cnvPVTPosCard, idx=1,
                                     comment='Dec of telescope pointing (deg)',
                                     onFail='NaN'))
        cards.append(makeCardFromKey(cmd, tccDict, 'spiderInstAng', 'SPA',
                                     cnv=_cnvPVTPosCard,
                                     idx=0, comment='TCC SpiderInstAng',
                                     onFail='NaN'))


    cards.append(makeCardFromKey(cmd, tccDict, 'rotType', 'ROTTYPE',
                                 cnv=str,
                                 idx=0, comment='Rotator request type',
                                 onFail='UNKNOWN'))
    cards.append(makeCardFromKey(cmd, tccDict, 'rotPos', 'ROTPOS',
                                 cnv=_cnvPVTPosCard,
                                 idx=0, comment='Rotator request position (deg)',
                                 onFail='NaN'))

    offsets = (('boresight', 'BOREOFF', 'TCC Boresight offset, deg', False),
               ('objArcOff', 'ARCOFF',  'TCC ObjArcOff, deg', False),
               ('objOff',    'OBJOFF',  'TCC ObjOff, deg', False),
               ('calibOff',  'CALOFF',  'TCC CalibOff, deg', True),
               ('guideOff',  'GUIDOFF', 'TCC GuideOff, deg', True))
    for tccKey, fitsName, comment, doRot in offsets:
        cards.append(makeCardFromKey(cmd, tccDict, tccKey, fitsName+'X',
                                     cnv=_cnvPVTPosCard, idx=0,
                                     comment=comment,
                                     onFail='NaN'))
        cards.append(makeCardFromKey(cmd, tccDict, tccKey, fitsName+'Y',
                                     cnv=_cnvPVTPosCard, idx=1,
                                     comment=comment,
                                     onFail='NaN'))
        if doRot:
            cards.append(makeCardFromKey(cmd, tccDict, tccKey, fitsName+'R',
                                         cnv=_cnvPVTPosCard, idx=2,
                                         comment=comment,
                                         onFail='NaN'))
               
    cards.append(makeCardFromKey(cmd, tccDict, 'axePos', 'AZ', 
                                 cnv=float,
                                 idx=0, comment='Azimuth axis pos. (approx, deg)',
                                 onFail='NaN'))
    cards.append(makeCardFromKey(cmd, tccDict, 'axePos', 'ALT',
                                 cnv=float,
                                 idx=1, comment='Altitude axis pos. (approx, deg)',
                                 onFail='NaN'))
    cards.append(makeCardFromKey(cmd, tccDict, 'axePos', 'IPA',
                                 cnv=float,
                                 idx=2, comment='Rotator axis pos. (approx, deg)',
                                 onFail='NaN'))

    cards.append(makeCardFromKey(cmd, tccDict, 'secFocus', 'FOCUS',
                                 idx=0, cnv=float,
                                 comment='User-specified focus offset (um)',
                                 onFail='NaN'))
    try:
        secOrient = tccDict['secOrient']
        orientNames = ('piston','xtilt','ytilt','xtran', 'ytran')
        for i in range(len(orientNames)):
            cards.append(makeCard(cmd, 'M2'+orientNames[i], float(secOrient[i]), 'TCC SecOrient'))
    except Exception as e:
        cmd.warn("failed to generate the SecOrient cards: %s" % (e))

    try:
        primOrient = tccDict['primOrient']
        orientNames = ('piston','xtilt','ytilt','xtran', 'ytran')
        for i in range(len(orientNames)):
            cards.append(makeCard(cmd, 'M1'+orientNames[i], float(primOrient[i]), 'TCC PrimOrient'))
    except Exception as e:
        cmd.warn("failed to generate the PrimOrient cards: %s" % (e))

    cards.append(makeCardFromKey(cmd, tccDict, 'scaleFac', 'SCALE',
                                 idx=0, cnv=float,
                                 comment='User-specified scale factor',
                                 onFail='NaN'))
    return cards

def plateCards(models, cmd):
    """ Return a list of pyfits Cards describing the plate/cartrige/pointing"""
    
    nameComment = "impossible error handling guider.cartridgeLoaded keyword"
    try:
        try:
            cartridgeKey = models['guider'].keyVarDict['cartridgeLoaded']
        except:
            nameComment = "Could not fetch guider.cartridgeLoaded keyword"
            cmd.warn('text="Could not fetch guider.cartridgeLoaded keyword"')
            raise 

        cartridge, plate, pointing, mjd, mapping = cartridgeKey
        if plate <= 0 or cartridge <= 0 or mjd < 50000 or mapping < 1 or pointing == '?':
            cmd.warn('text="guider cartridgeKey is not well defined: %s"' % (str(cartridgeKey)))
            nameComment = "guider cartridgeKey %s is not well defined" % (str(cartridgeKey))
            name = '0000-00000-00'
        else:
            nameComment = 'The name of the currently loaded plate'
            name = "%04d-%05d-%02d" % (plate, mjd, mapping)
    except:
        cartridge, plate, pointing, mjd, mapping = -1,-1,'?',-1,-1
        name = '0000-00000-00'

    cards = []
    cards.append(makeCard(cmd, 'NAME', name, nameComment))
    cards.append(makeCard(cmd, 'PLATEID', plate, 'The currently loaded plate'))
    cards.append(makeCard(cmd, 'CARTID', cartridge, 'The currently loaded cartridge'))
    cards.append(makeCard(cmd, 'MAPID', mapping, 'The mapping version of the loaded plate'))
    cards.append(makeCard(cmd, 'POINTING', pointing, 'The currently specified pointing'))

    return cards
    
def _cnvListCard(val, itemCnv=int):
    """ Stupid utility to cons up a single string card from a list. """

    return " ".join([str(itemCnv(v)) for v in val])
    
def _cnvPVTPosCard(pvt, atTime=None):
    try:
        return pvt.getPos()
    except:
        return np.nan

def _cnvPVTVelCard(pvt):
    try:
        return pvt.getVel()
    except:
        return np.nan
