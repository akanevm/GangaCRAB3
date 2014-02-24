from ConfigParser import ConfigParser
from Ganga.Core import BackendError
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.Utility import Config
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Shell import Shell
from GangaCRAB3.Lib.CRABTools.CRABServer import CRABServer
from GangaCRAB3.Lib.CRABTools.CRABServerError import CRABServerError
from xml.dom.minidom import parse

import datetime
import os

logger = getLogger()

class CRABBackend(IBackend):
    """Backend implementation for CRAB."""
    schemadic = {}
    schemadic['verbose'] = SimpleItem(defvalue=1,
                                      typelist=['int'],
                                      doc='Set to 0 to disable CRAB logging')
    schemadic['statusLog'] = SimpleItem(defvalue=0,
                                        typelist=['int'],
                                        doc='Set to 1 to keep -status logs')


    def __init__(self):
        super(CRABBackend, self).__init__()
        config = Config.getConfig('CMSSW')
        shell = Shell(os.path.join(config['CMSSW_SETUP'], 'CMSSW_generic.sh'),
                      [config['CMSSW_VERSION'], config['CRAB_VERSION']])
        self.crab_env = shell.env


