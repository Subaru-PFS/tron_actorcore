import logging
import pathlib
import string

import numpy as np

from opscore.protocols import types, keys
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

def getExpiredValue(keyType, key):
    """ Return a type-correct Expired value. """

    if issubclass(keyType.__class__, types.Bool):
        return False
    if keyType.baseType is int:
        return -9998
    if keyType.baseType is float:
        return -9998.0
    if keyType.baseType is str:
        return 'no available value'
    raise ValueError('unexpected type: %s' % (keyType.baseType))

def getInvalidValue(keyType, key):
    """ Return a type-correct Invalid value. """

    if issubclass(keyType.__class__, types.Bool):
        return False
    if keyType.baseType is int:
        return -9999
    if keyType.baseType is float:
        return -9999.0
    if keyType.baseType is str:
        return 'invalid value'
    raise ValueError('unexpected type: %s' % (keyType.baseType))

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
                try:
                    if not hasattr(kvt, 'FITS') or kvt.FITS is None:
                        continue

                    shortCard, longCard = kvt.FITS
                    if longCard == '':
                        longCard = shortCard
                    if shortCard.startswith('W_') and not longCard.startswith('W_'):
                        longCard = "W_" + longCard
                        
                    # Hackery: bool cannot be subclassed, so we need to check the keyword class
                    if issubclass(kvt.__class__, types.Bool):
                        baseType = bool
                    else:
                        baseType = kvt.__class__.baseType

                    logger.debug(f'FITS card:  {kv_i}({kvt.name}, {baseType} {kvt.__class__}) = {shortCard}, {longCard}"')

                    if not mv.isCurrent:
                        logger.debug(f'text="SKIPPING NOT CURRENT {mk} = {mv}"')
                        value = getExpiredValue(kvt, mv)
                    else:
                        rawVal = mv[kv_i]
                        if isinstance(rawVal, types.Invalid):
                            cmd.warn(f'text="FITS card {shortCard} from {mk}[{kv_i}] has the invalid value"') 
                            value = getInvalidValue(kvt, mv)
                        else:
                            try:
                                # Now we can get the value
                                value = baseType(rawVal)
                            except Exception as e:
                                cmd.warn(f'text="FAILED to convert card value {rawVal} for {mk}[{kv_i}], {kvt}: {e}"') 
                                value = getInvalidValue(kvt, mv)

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
                    cmd.warn(f'text="FAILED to generate FITS cards for {mk}[{kv_i}], {kvt}: {e}"')
                    cards.append(f'Failed to make FITS cards for MHS key {mk}[{kv_i}]')
                    
        except Exception as e:
            cmd.warn(f'text="FAILED to generate FITS cards for {mk}: {e}"')
            cards.append(f'Failed to get FITS cards for MHS key {mk}')

    return cards

def makeNameTranslationHDU(cards):
    """ If we have both long and short FITS card names, generate the mapping for a translation HDU. """

    longToShort = dict()
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
            allCards.append(dict(name='comment', value=f'################################ Cards from {modName}'))
            modCards = cardsFromModel(cmd, actor.models[modName], shortNames=shortNames)
            allCards.extend(modCards)
        except Exception as e:
            logger.warn(f' Failed to get FITS cards for actor {modName}: {e}')
            allCards.append(dict(name='comment', value=f' Failed to get FITS cards for actor {modName}'))
            cmd.warn(f'text="FAILED to get FITS cards for actor {modName}: {e}"')

    return allCards

def cardFormatsForModel(model):
    """
    For a given actorkeys model, return a dictionary describing the formats of all the FITS cards l.

    Args
    ----
    model : opscore.actor.Model
      Usually from self.actor.models[modelName]
    
    Returns
    -------
    cards : dict
      name, type, format, units, invalid, unavailable, comment
    """

    logger = logging.getLogger('FITS')
    cards = dict()
    for mk, mv in model.keyVarDict.items():
        try:
            # When values are not current, they are structurally invalid. So iterate over the _types_,
            # then pick up the value only when necessary.
            for kv_i, kvt in enumerate(mv._typedValues.vtypes):
                try:
                    if not hasattr(kvt, 'FITS') or kvt.FITS is None:
                        continue

                    shortCard, longCard = kvt.FITS
                    if longCard == '':
                        longCard = shortCard
                    if shortCard.startswith('W_') and not longCard.startswith('W_'):
                        longCard = "W_" + longCard

                    # Hackery: bool cannot be subclassed, so we need to check the keyword class
                    if issubclass(kvt.__class__, types.Bool):
                        baseType = bool
                    else:
                        baseType = kvt.__class__.baseType

                    if kvt.units is not None:
                        units = kvt.units
                    else:
                        units = '-'

                    if kvt.help is not None:
                        comment = kvt.help
                    else:
                        comment = '-'

                    if hasattr(kvt, 'enumLabels'):
                        values = kvt.enumLabels
                    else:
                        values = ''

                    reprFmt = kvt.strFmt
                    if not reprFmt:
                        if baseType == int:
                            reprFmt = '%d'
                        elif baseType == float:
                            reprFmt = "%-15G"
                        else:
                            reprFmt = ""
                            
                    logger.debug(f'FITS info:  {kv_i},{kvt.name},{baseType},{shortCard}, {longCard}"')
                    cards[shortCard] = dict(name=shortCard,
                                            longname=longCard,
                                            type=baseType.__name__,
                                            format=reprFmt,
                                            units=units,
                                            enum=values,
                                            keyword=f'{mv.name}[{kv_i}]',
                                            comment=comment)

                except Exception as e:
                    logger.warn(f'text="FAILED to generate FITS info for {mk}[{kv_i}], {kvt}: {e}"')
                    
        except Exception as e:
            logger.warn(f'text="FAILED to generate FITS info for {mk}: {e}"')

    return cards

def gatherHeaderFormats(cmd, actor, modelNames=None):
    """ Fetch and return the Subaru-required formatting table for all FITS cards defined in the given models.

    Args
    ----
    cmd : an actorcore Command
    actor : an actorcore Actor,
      Which contans .models.
    modelNames : None, or list of strings
      The actors to generate keys for. If None, all the models in actor.models
    longNames : bool
      Whether to generate a column for the long/HIERARCH FITS names

    Returns
    -------
    dict : all the fields Subaru wants.
    """

    logger = logging.getLogger('FITS')
    
    if modelNames is None:
        modelNames = actor.models.keys()

    allFormats = dict()
    for modName in modelNames:
        logger.info(f'gathering cards from model {modName}')
        try:
            modDict = cardFormatsForModel(actor.models[modName])
            for keyname in modDict:
                keyDict = modDict[keyname]
                keyDict['actor'] = modName
                key = keyDict['name']
                if key in allFormats:
                    raise KeyError(f"key {key} is already in the formats dist as: {allFormats[key]}")
                allFormats[key] = keyDict

        except Exception as e:
            logger.warn(f' Failed to get FITS card formats for actor {modName}: {e}')
            cmd.warn(f'text="FAILED to get FITS cards for actor {modName}: {e}"')
            
    return allFormats

def printHeaderFormats(cmd, actor, filename, longNames=True):
    formats = gatherHeaderFormats(cmd, actor)

    path = pathlib.Path(filename)
    suffix = path.suffix
    if suffix == '.csv':
        dlim = ','
        style = 'csv'
    elif suffix == '.md':
        dlim = ' | '
        style = 'markdown'
    else:
        raise ValueError("can only output to .csv or .md files")

    if longNames:
        fields = ('name', 'longname', 'type', 'format', 'units', 'actor', 'keyword', 'comment')
    else:
        fields = ('name', 'type', 'format', 'units', 'actor', 'keyword', 'comment')

    def _markdown_str(name, s):
        if name == 'comment':
            return s
        elif s == '':
            return s
        else:
            return f"``{s}``"
    
    def _fmt(c, style=style, fields=fields):
        if c['enum'] != '':
            fields = list(fields)
            c_i = fields.index('comment')
            fields[c_i] = 'enum'
            
        if style == 'csv':
            return dlim.join([c[f] for f in fields])
        formattedFields = [_markdown_str(f, c[f]) for f in fields]
        return dlim.join(formattedFields)

    def _getActorkeyVersion(actor):
        try:
            return str(actor.models[actor.name].version_ics_actorkeys[0])
        except:
            return 'git'
        
    with open(filename, "w") as of:
        if style == 'markdown':
            of.write(f'## {actor.productName} FITS cards, generated from ``ics_actorkeys`` {_getActorkeyVersion(actor)}\n\n')
                                     
        of.write(dlim.join([f.capitalize() for f in fields]))
        of.write('\n')
        tr = str.maketrans(string.ascii_lowercase, '-'*26)
        of.write(dlim.join([f.translate(tr) for f in fields]))
        of.write('\n')
        
        for k in sorted(formats.keys()):
            c = formats[k]
            try:
                of.write(_fmt(c))
            except Exception as e:
                cmd.warn('text=%s' % (qstr(f"boom: {str(e)} on {c}")))
            of.write('\n')

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

