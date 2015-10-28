from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class CRABConfig(GangaObject):


    _schema = Schema(Version(2,6), {

        'General' : ComponentItem('General',doc=''), 
        'JobType' : ComponentItem('JobType',doc=''),
        'Data'    : ComponentItem('Data',doc=''),
        'User'    : ComponentItem('User',doc=''),
        'Debug'   : ComponentItem('Debug',doc=''),
        'Site'    : ComponentItem('Site',doc='')

    })

    _category = 'CRABConfig'
    _name = 'CRABConfig'
