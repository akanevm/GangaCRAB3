from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class Site(GangaObject):


    _schema = Schema(Version(2,6), {

        'storageSite' : SimpleItem(defvalue = None, typelist=['type(None)', 'str'], protected=0,copyable=1,doc="Site where the output files should be permanently copied to. See the note on 'Storage site' below."),
        'whitelist'   : SimpleItem(defvalue = [None],typelist=['type(None)', 'str'],sequence=1,protected=0,copyable=1,doc="A user-specified list of sites where the jobs can run. For example: ['T2_CH_CERN','T2_IT_Bari',...]. Jobs will not be assigned to a site that is not in the white list."),
        'blacklist'   : SimpleItem(defvalue = [None],typelist=['type(None)', 'str'],sequence=1,protected=0,copyable=1,doc="A user-specified list of sites where the jobs should not run. Useful to avoid jobs to run on a site where the user knows they will fail (e.g. because of temporary problems with the site).")
	

    })

    _category = 'Site'
    _name = 'Site'
