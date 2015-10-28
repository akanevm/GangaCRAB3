from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class User(GangaObject):


    _schema = Schema(Version(1,6), {

        'voGroup' : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="The VO group that should be used with the proxy and under which the task should be submitted."),
        'voRole'  : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="The VO role that should be used with the proxy and under which the task should be submitted." )

    })

    _category = 'User'
    _name = 'User'
