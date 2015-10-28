from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class General(GangaObject):


    _schema = Schema(Version(1,6), {

    'requestName'      : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="A name the user gives to it's request/task. In particular, it is used by CRAB to create a project directory (named crab_<requestName>) where files corresponding to this particular task will be stored. Defaults to <time-stamp>, where the time stamp is of the form <YYYYMMDD>_<hhmmss> and corresponds to the submission time."),
    'workArea'         : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="The area (full or relative path) where to create the CRAB project directory. If the area doesn't exist, CRAB will try to create it using the mkdir command. Defaults to the current working directory."),
    'transferOutputs'  : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'], protected=0,copyable=1,doc="Whether or not to transfer the output files to the storage site. If set to False, the output files are discarded and the user can not recover them. Defaults to True."),
    'transferLogs'     : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'], protected=0,copyable=1,doc="Whether or not to copy the jobs log files to the storage site. If set to False, the log files are discarded and the user can not recover them. Notice however that a short version of the log files containing the first 1000 lines and the last 3000 lines are still available through the monitoring web pages. Defaults to False."),
    'failureLimit'     : SimpleItem(defvalue=None,typelist=['type(None)', 'int'], protected=0,copyable=1,doc="The number of jobs that may fail permanently before the entire task is cancelled. Disabled by default."),
    'instance'         : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="The CRAB server instance where to submit the task. For experts use only."),
    'activity'         : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc="The activity name used when reporting to Dashboard. For experts use only. ")

    })

    _category = 'General'
    _name = 'General'
