#
# CRAB Application
# 
# 08/06/10 @ ubeda
#
import subprocess

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *
from Ganga.Utility.logging import getLogger

from GangaCRAB3.Lib.ConfParams import *
from GangaCRAB3.Lib.CRABTools.CRABServer import *

import Ganga.Utility.Config

logger = getLogger()

class CRABApp(IApplication):

  comments=[]

  schemadic = {}

  # is_prepared is needed for GangaRobot on Ganga 5.7.0 and later.
  schemadic['is_prepared']    = SharedItem(defvalue=None,
                                           strict_sequence=0,
                                           visitable=1,
                                           copyable=1,
                                           typelist=['type(None)','bool','str'],
                                           protected=0,
                                           doc='Location of shared resources. Presence of this attribute implies the application has been prepared.')

  _schema = Schema(Version(1,0), schemadic)
  _category = 'applications'
  _name = 'CRABApp' 

  def __init__(self):
    logger.info("init crab3app")
    super(CRABApp,self).__init__()


  def master_configure(self):
    logger.info("master_configure crab3app")
    job = self.getJobObject()
    try:
        userproxy = os.environ['X509_USER_PROXY']
        job.backend.userproxy = userproxy
    except KeyError:
        logger.error('X509_USER_PROXY not set')
        return

    return (1,None)

  def configure(self,masterappconfig):
    logger.info("configure crab3app")
    return (None,None)

  def prepare(self):
    logger.info("prepare CRABApp")
