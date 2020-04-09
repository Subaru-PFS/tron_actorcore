from pfs.utils.opdb import opDB

def fetchVisitFromGen2(self, cmd=None):
    """Actually get a new visit from Gen2.
    What PFS calls a "visit", Gen2 calls a "frame".
    """
    if 'gen2' not in self.models.keys():
        self.addModels(['gen2'])
        gen = self.bcast if cmd is None else cmd
        gen.inform('text="connecting gen2 model"')

    ret = self.cmdr.call(actor='gen2', cmdStr='getVisit', timeLim=10.0, forUserCmd=cmd)
    if ret.didFail:
        raise RuntimeError("Failed to get a visit number in 10s!")

    visit = int(self.models['gen2'].keyVarDict['visit'].valueList[0])
    opDB.insert('pfs_visit', pfs_visit_id=visit, pfs_visit_description='sps')

    return visit