class FetchVisitFromGen2(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return f"{self.__class__.__name__}({self.reason})"


def fetchVisitFromGen2(self, cmd=None, designId=None, timeLim=10):
    """Actually get a new visit from Gen2.
    What PFS calls a "visit", Gen2 calls a "frame".
    """
    gen = self.bcast if cmd is None else cmd

    if 'gen2' not in self.models.keys():
        self.addModels(['gen2'])
        gen.inform('text="connecting gen2 model"')

    designArg = 'designId=0x%016x' % designId if designId is not None else ''

    ret = self.cmdr.call(actor='gen2', cmdStr=f'getVisit caller={self.name} {designArg}'.strip(),
                         timeLim=timeLim, forUserCmd=cmd)

    if ret.didFail:
        raise FetchVisitFromGen2(f"failed to get a visit number in {timeLim}s !")

    visit = self.models['gen2'].keyVarDict['visit'].valueList[0]
    return int(visit)
