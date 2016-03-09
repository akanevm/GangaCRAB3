from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class Data(GangaObject):

    _doc = []
    _doc.append("When running an analysis over a dataset registered in DBS, this parameter specifies the name of the dataset. The dataset can be an official CMS dataset or a dataset produced by a user.")
    _doc.append("Allow CRAB to run over (the valid files of) the input dataset given in Data.inputDataset even if its status in DBS is not VALID. Defaults to False.")
    _doc.append("When running an analysis over private input files or running MC generation, this parameter specifies the primary dataset name that should be used in the LFN of the output/log files and in the publication dataset name (see Data handling in CRAB).")
    _doc.append("The URL of the DBS reader instance where the input dataset is published. The URL is of the form 'https://cmsweb.cern.ch/dbs/prod/<instance>/DBSReader', where instance can be global, phys01, phys02 or phys03. The default is global instance. The aliases global, phys01, phys02 and phys03 in place of the whole URLs are also supported (and indeed recommended to avoid typos). For datasets that are not of USER tier, CRAB only allows to read them from global DBS.")
    _doc.append("Mode to use to split the task in jobs. When JobType.pluginName = 'Analysis', the splitting mode can either be 'FileBased', 'LumiBased', or 'EventAwareLumiBased' (for Data, the recommended mode is 'LumiBased'). For 'EventAwareLumiBased', CRAB will split the task by luminosity sections, where each job will contain a varying number of luminosity sections such that the number of events analyzed by each job is roughly unitsPerJob. When JobType.pluginName = 'PrivateMC', the splitting mode can only be 'EventBased'.")
    _doc.append("Suggests (but not impose) how many units (i.e. files, luminosity sections or events [1] -depending on the splitting mode-) to include in each job.")
    _doc.append("Mandatory when JobType.pluginName = 'PrivateMC', in which case the parameter tells how many events to generate in total. When JobType.pluginName = 'Analysis', this parameter tells how many files (when Data.splitting = 'FileBased'), luminosity sections (when Data.splitting = 'LumiBased') or events [1] (when Data.splitting = 'EventAwareLumiBased') to analyze (after applying the lumi-mask and/or run range filters).")
    _doc.append("Adds corresponding parent dataset in DBS as secondary input source. Allows to gain access to more data tiers than present in the current dataset. This will not check for parent dataset availability; jobs may fail with xrootd errors or due to missing dataset access. Defaults to False.")
    _doc.append("An extension of the Data.useParent parameter. Allows to specify any grandparent dataset in DBS (same instance as the primary dataset) as secondary input source. CRAB will internally set this dataset as the parent and will set Data.useParent = True. Therefore, Data.useParent and Data.secondaryDataset can not be used together a priori.")
    _doc.append("A lumi-mask to apply to the input dataset before analysis. Can either be a URL address or the path to a JSON file on disk. Default to an empty string (no lumi-sections filter).")
    _doc.append("The runs and/or run ranges to process (e.g. '193093-193999,198050,199564'). It can be used together with a lumi-mask. Defaults to an empty string (no run filter).")
    _doc.append("The first part of the LFN of the output files (see Data handling in CRAB). Accepted values are /store/user/<username>[/<subdir>*] (the trailing / after <username> can not be omitted if a subdir is not given) and /store/group/<groupname>[/<subgroupname>*] (and /store/local/<dir>[/<subdir>*] if Data.publication = False). Defaults to /store/user/<username>/.")
    _doc.append("Whether to publish or not the EDM output files (i.e. output files produced by PoolOutputModule) in DBS. Notice that for publication to be possible, the corresponding output files have to be transferred to the permanent storage element. Defaults to False.")
    _doc.append("The URL of the DBS writer instance where to publish. The URL is of the form 'https://cmsweb.cern.ch/dbs/prod/<instance>/DBSWriter', where instance can so far only be phys03, and therefore it is set as the default, so the user doesn't have to specify this parameter. The alias phys03 in place of the whole URL is also supported.")
    _doc.append("A custom string used in both, the LFN of the output files (even if Data.publication = False) and the publication dataset name (if Data.publication = True) (see Data handling in CRAB).")
    _doc.append("If Data.outLFNDirBase starts with /store/group/<groupname>, use the groupname instead of the username in the publication dataset name (see Data handling in CRAB). This feature allows different users running the same workflow to publish in the same dataset. Defaults to False.")
    _doc.append("Defaults to False. Set to True to allow the jobs to run at any site, regardless of where the input dataset is hosted (this parameter has effect only when Data.inputDataset is used). Remote file access is done using Xrootd. The parameters Site.whitelist and Site.blacklist are still respected. This parameter is useful to allow the jobs to run on other sites when for example a dataset is hosted only on sites which are not running CRAB jobs. When set to True, it is strongly recommended that the user provides also a white-list of sites where the jobs could run that are physically close to the site that hosts the input dataset. For example, if the input dataset is hosted by a site in the USA, a reasonable site white-list would be ['T2_US_*']. This is to reduce file access latency.")
    _doc.append("This parameter serves to run an analysis over a set of (private) input files, as opposed to run over an input dataset from DBS. One has to provide in this parameter the list of input files: Data.userInputFiles = ['file1', 'file2', 'etc'], where 'fileN' can be an LFN, a PFN or even an LFN + Xrootd redirector. One could also have a local text file containing the list of input files (one file per line; don't include quotation marks nor commas) and then specify in this parameter the following: Data.userInputFiles = open('/path/to/local/file.txt').readlines(). When this parameter is used, the only allowed splitting mode is 'FileBased'. Also, since there is no input dataset from where to extract the primary dataset name, the user should use the parameter Data.primaryDataset to define it; otherwise CRAB will use 'CRAB_UserFiles' as the primary dataset name. This parameter can not be used together with Data.inputDataset. CRAB will not do any data discovery, meaning that most probably jobs will not run at the sites where the input files are hosted (and therefore they will be accessed via Xrootd). But since it is in general more efficient to run the jobs at the sites where the input files are hosted, it is strongly recommended that the user forces the jobs to be submitted to these sites using the Site.whitelist parameter. ")


    _schema = Schema(Version(2,6), {

        'inputDataset'              : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[0]),
        'allowNonValidInputDataset' : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'], protected=0,copyable=1,doc=_doc[1]),
        'primaryDataset'            : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[2]),
        'inputDBS'                  : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[3]),
        'splitting'                 : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[4]),
        'unitsPerJob'               : SimpleItem(defvalue=None,typelist=['type(None)', 'int'], protected=0,copyable=1,doc=_doc[5]),
        'totalUnits'                : SimpleItem(defvalue=None,typelist=['type(None)', 'int'], protected=0,copyable=1,doc=_doc[6]),
        'useParent'                 : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'], protected=0,copyable=1,doc=_doc[7]),
        'secondaryDataset'          : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[8]),
        'lumiMask'                  : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[9]),
        'runRange'                  : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[10]),
        'outLFNDirBase'             : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[11]),
        'publication'               : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'], protected=0,copyable=1,doc=_doc[12]),
        'publishDBS'                : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[13]),
        'outputDatasetTag'           : SimpleItem(defvalue=None,typelist=['type(None)', 'str'], protected=0,copyable=1,doc=_doc[14]),
        'publishWithGroupName'      : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'], protected=0,copyable=1,doc=_doc[15]),
        'ignoreLocality'            : SimpleItem(defvalue=None,typelist=['type(None)', 'bool'], protected=0,copyable=1,doc=_doc[16]),
        'userInputFiles'            : SimpleItem(defvalue=[None],typelist=['type(None)', 'str'],sequence=1,protected=0,copyable=1,doc=_doc[17]),

    })

    _category = 'Data'
    _name = 'Data'

