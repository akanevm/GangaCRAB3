from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class Debug(GangaObject):


    _schema = Schema(Version(1,6), {

        'oneEventMode' : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'], protected=0,copyable=1,doc="For experts use only."),
        'ASOURL'       : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="For experts use only."),
        'scheddName'   : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="For experts use only."),
        'extraJDL'     : SimpleItem(defvalue=[None],typelist=['type(None)', 'str'], sequence=1,protected=0,copyable=1,doc="For experts use only."),
        'collector'    : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="For experts use only. ")

    })

    _category = 'Debug'
    _name = 'Debug'
