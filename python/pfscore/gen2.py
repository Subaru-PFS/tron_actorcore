def fetchVisitFromGen2(cls, cmd=None):
    """Actually get a new visit from Gen2.
    What PFS calls a "visit", Gen2 calls a "frame".
    """
    if 'gen2' not in cls.models.keys():
        cls.addModels(['gen2'])
        gen = cls.bcast if cmd is None else cmd
        gen.inform('text="connecting gen2 model"')

    ret = cls.cmdr.call(actor='gen2', cmdStr='getVisit', timeLim=10.0, forUserCmd=cmd)
    if ret.didFail:
        raise RuntimeError("Failed to get a visit number in 10s!")

    visit = cls.models['gen2'].keyVarDict['visit'].valueList[0]
    return int(visit)