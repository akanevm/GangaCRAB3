import os
import Ganga.Utility.Config
import Ganga.Utility.logging
import platform

"""
## CMSSW parameters
configCMSSW=Ganga.Utility.Config.makeConfig('CMSSW','Parameters for CMSSW')

dscrpt = 'The version CMSSW used for job submission.'
configCMSSW.addOption('CMSSW_VERSION','CMSSW_3_7_0',dscrpt)
dscrpt = 'The CMSSW setup script used for env configuration.'

config = Ganga.Utility.Config.getConfig('System')
ganga_pythonpath = "/data/hc/external/ganga/install/HEAD/python" #config['GANGA_PYTHONPATH']

configCMSSW.addOption('CMSSW_SETUP','%s/GangaCRAB3/scripts/'%(ganga_pythonpath),dscrpt)
dscrpt = 'The location of the CMSSW Framework.'
configCMSSW.addOption('location','~/',dscrpt)

configMetrics = Ganga.Utility.Config.makeConfig('Metrics','List of desired metrics.')
dscrpt = 'The location of the metrics.cms list.'
configMetrics.addOption('location','%s/GangaGRAB3/metrics.ini'%(ganga_pythonpath),dscrpt)

dscrpt = 'The version CRAB used for job submission.'
configCMSSW.addOption('CRAB_VERSION','CRAB_2_7_5',dscrpt)
"""
config = Ganga.Utility.Config.getConfig('System')
ganga_pythonpath = "/data/hc/external/ganga/install/HEAD/python" #config['GANGA_PYTHONPATH']

configMetrics = Ganga.Utility.Config.makeConfig('Metrics','List of desired metrics.')
dscrpt = 'The location of the metrics.cms list.'
configMetrics.addOption('location','%s/GangaGRAB3/metrics.ini'%(ganga_pythonpath),dscrpt)


def getEnvironment( config = {} ):
    import sys
    import os.path
    import os
    import PACKAGE

    PACKAGE.standardSetup()

    #config = Ganga.Utility.Config.getConfig('CRAB3')
    #for el in config:
    #    print el
    #crab3_setup_script = config['crab3_setup_script']
    #from Ganga.Utility.Shell import Shell
    #shell = Shell('/data/hc/apps/cms/config/crab3_setup.sh')

    """
    PACKAGE.standardSetup()

    arch = platform.machine()
    if not arch == 'x86_64':
        print 'GangaCRAB3> [ERROR] %s not supported. Different than 64 bits.'%(arch)
        return
    print 'GangaCRAB3> [INFO] not supported different OS than SLC5'

    config = Ganga.Utility.Config.getConfig('CMSSW')
    cmssw_version = config['CMSSW_VERSION']
    cmssw_setup = config['CMSSW_SETUP']
    crab_version = config['CRAB_VERSION']
    cmssw_setup_script = os.path.join(cmssw_setup,'CMSSW_3_7_0.sh')
    if not os.path.exists(cmssw_setup_script):
        print 'GangaCRAB3> [ERROR] CMSSW setup script not found: "%s"'%(cmssw_setup_script)
        return

    location = config['location']

    cmsswhome = os.path.join(location,cmssw_version)
    if not os.path.exists(cmsswhome):
        print 'GangaCRAB3> [ERROR] CMSSW location not found: "%s"'%(cmsswhome)
        return

#    from Ganga.Utility.Shell import Shell
#    shell = Shell(cmssw_setup_script)   
    """
    """
    config = Ganga.Utility.Config.getConfig('CRAB_CFG')
    try:
        userproxy = os.environ['X509_USER_PROXY']
        config.addOption('userproxy', userproxy, 'user proxy to submit')
        print 'GangaCRAB3> using userproxy: %s' % userproxy
    except KeyError:
        print 'GangaCRAB3> [ERROR] X509_USER_PROXY not set'
        return
    """
    print 'GangaCRAB3> [INFO] getEnvironment : done'   
    #return shell.env
    return {}

def loadPlugins( config = {} ):
    import Lib.CRABTools
    import Lib.Utils
    import Lib.ConfParams

    crab_cfg_configs = {}

    for params in [Lib.ConfParams.Data(), Lib.ConfParams.User(), Lib.ConfParams.General(), Lib.ConfParams.Site(), Lib.ConfParams.Debug(), Lib.ConfParams.JobType()]:

      section = params.__class__.__name__
      crab_cfg_configs[section] = Ganga.Utility.Config.makeConfig('CRABConfig_%s' % (section), 'Parameters for %s.' % (section) )

      for k in params._schema.datadict.keys():
       crab_cfg_configs[section].addOption(k, None, '%s' % (k) )

    print 'GangaCRAB3> [INFO] loadPlugins : done'

