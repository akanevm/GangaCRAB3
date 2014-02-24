from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from GangaCRAB3.Lib.ConfParams import CRAB, TASK 


class CRABDataset(Dataset):
    """Dataset definition for CRAB jobsets."""
    schemadic = {}
    schemadic.update(CRAB().schemadic)
    schemadic.update(TASK().schemadic)
    #schemadic['target_site'] = SimpleItem(defvalue=None,
    #                                      typelist=['type(None)', 'str'],
    #                                      doc='Target site name for the job.')
    #schemadic['taskname'] = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc='TaskName of the submitted task, returned from CRAB3 server')
    _schema = Schema(Version(1, 0), schemadic)
    _category = 'datasets'
    _name = 'CRABDataset'

    def __init__(self):
        super(CRABDataset, self).__init__()
