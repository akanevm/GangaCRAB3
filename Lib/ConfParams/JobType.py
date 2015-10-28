from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class JobType(GangaObject):

    _doc = []
    _doc.append("Specifies if this task is running an analysis ('Analysis') on an existing dataset or is running MC event generation ('PrivateMC').")
    _doc.append("The name of the CMSSW parameter-set configuration file that should be run via cmsRun. Defaults to 'pset.py'.")
    _doc.append("This parameter should be set to 'lhe' when running MC generation on LHE files.")
    _doc.append("List of parameters to pass to the CMSSW parameter-set configuration file, as explained here. For example, if set to ['myOption','param1=value1','param2=value2'], then the jobs will execute cmsRun JobType.psetName myOption param1=value1 param2=value2.")
    _doc.append("List of private input files (and/or directories) needed by the jobs. They will be added to the input sandbox. The input sandbox can not exceed 100 MB. The input sandbox is shipped with each job. The input files will be placed in the working directory where the users' application (e.g. cmsRun) is launched.")
    _doc.append("Whether to disable or not the automatic recognition of output files produced by PoolOutputModule or TFileService in the CMSSW parameter-set configuration. If set to True, it becomes the user's responsibility to specify in the JobType.outputFiles parameter all the output files that need to be collected. Defaults to False.")
    _doc.append("List of output files that need to be collected. If disableAutomaticOutputCollection = False (the default), output files produced by PoolOutputModule or TFileService in the CMSSW parameter-set configuration are automatically recognized by CRAB and don't need to be included in this parameter.")
    _doc.append("When JobType.pluginName = 'PrivateMC', this parameter specifies how many events should a luminosity section contain. Note that every job starts with a fresh luminosity section, which may lead to unevenly sized luminosity sections if Data.unitsPerJob is not a multiple of this parameter. Defaults to 100.")
    _doc.append("Whether to allow or not using a CMSSW release possibly not available at sites. Defaults to False.")
    _doc.append("Maximum amount of memory (in MB) a job is allowed to use. Defaults to 2000.")
    _doc.append("The maximum runtime (in minutes) per job. Jobs running longer than this amount of time will be removed. Defaults to 1315 (21 hours 55 minutes).")
    _doc.append("Number of requested cores per job. Defaults to 1.")
    _doc.append("Task priority among the user's own tasks. Higher priority tasks will be processed before lower priority. Two tasks of equal priority will have their jobs start in an undefined order. The first five jobs in a task are given a priority boost of 10. Defaults to 10.")
    _doc.append("A user script that should be run on the worker node instead of the default cmsRun. It is up to the user to setup the script properly to run on the worker node enviroment. CRAB guarantees that the CMSSW environment is setup (e.g. scram is in the path) and that the modified CMSSW parameter-set configuration file will be placed in the working directory with name PSet.py. The user must ensure that a properly named framework job report file will be written; this can be done e.g. by calling cmsRun within the script as cmsRun -j FrameworkJobReport.xml -p PSet.py. The script itself will be added automatically to the input sandbox. Output files produced by PoolOutputModule or TFileService in the CMSSW parameter-set configuration file will be automatically collected (CRAB3 will look in the framework job report). The user needs to specify other output files to be collected in the JobType.outputFiles parameter.")
    _doc.append("Additional arguments (in the form param=value) to be passed to the script specified in the JobType.scriptExe parameter.")
    _doc.append("Determine if the python folder in the CMSSW release ($CMSSW_BASE/python) is included in the sandbox or not. Defaults to False.")
    _doc.append("Name of a plug-in provided by the user and which should be run instead of the standard CRAB plug-in Analysis or PrivateMC. Can not be specified together with pluginName; is either one or the other. Not supported yet.")

    _schema = Schema(Version(1,6), {

        'pluginName'                       : SimpleItem(defvalue=None,typelist=['type(None)', 'str'],protected=0,copyable=1,doc=_doc[0]),
        'psetName'                         : SimpleItem(defvalue=None,typelist=['type(None)', 'str'],protected=0,copyable=1,doc=_doc[1]),
        'generator'                        : SimpleItem(defvalue=None,typelist=['type(None)', 'str'],protected=0,copyable=1,doc=_doc[2]),
        'pyCfgParams'                      : SimpleItem(defvalue=[None],typelist=['type(None)', 'str'],sequence=1,protected=0,copyable=1,doc=_doc[3]),
        'inputFiles'                       : SimpleItem(defvalue=[None],typelist=['type(None)', 'str'],sequence=1,protected=0,copyable=1,doc=_doc[4]),
        'disableAutomaticOutputCollection' : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'],protected=0,copyable=1,doc=_doc[5]),
        'outputFiles'                      : SimpleItem(defvalue=[None],typelist=['type(None)', 'str'],sequence=1,protected=0,copyable=1,doc=_doc[6]),
        'eventsPerLumi'                    : SimpleItem(defvalue=None,typelist=['type(None)', 'int'],protected=0,copyable=1,doc=_doc[7]),
        'allowUndistributedCMSSW'          : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'],protected=0,copyable=1,doc=_doc[8]),
        'maxMemoryMB'                      : SimpleItem(defvalue=None,typelist=['type(None)', 'int'],protected=0,copyable=1,doc=_doc[9]),
        'maxJobRuntimeMin'                 : SimpleItem(defvalue=None,typelist=['type(None)', 'int'],protected=0,copyable=1,doc=_doc[10]),
        'numCores'                         : SimpleItem(defvalue=None,typelist=['type(None)', 'int'],protected=0,copyable=1,doc=_doc[11]),
        'priority'                         : SimpleItem(defvalue=None,typelist=['type(None)', 'int'],protected=0,copyable=1,doc=_doc[12]),
        'scriptExe'                        : SimpleItem(defvalue=None,typelist=['type(None)', 'str'],protected=0,copyable=1,doc=_doc[13]),
        'scriptArgs'                       : SimpleItem(defvalue=None,typelist=['type(None)', 'str'],protected=0,copyable=1,doc=_doc[14]),
        'sendPythonFolder'                 : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'],protected=0,copyable=1,doc=_doc[15]),
        'externalPluginFile'               : SimpleItem(defvalue=None,typelist=['type(None)', 'str'],protected=0,copyable=1,doc=_doc[16])

    })

    _category = 'JobType'
    _name = 'JobType'
