from ConfigParser import ConfigParser
from Ganga.Core import BackendError
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Schema import * 
from Ganga.Utility import Config
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Shell import Shell
#from GangaCRAB3.Lib.CRABTools.CRABServer import CRABServer
#from GangaCRAB3.Lib.CRABTools.CRABServerError import CRABServerError
from GangaCRAB3.Lib.ConfParams import Data, User, General, Debug, JobType
from xml.dom.minidom import parse

import datetime
import os
import sys

from Ganga.GPIDev.Lib.Job.Job import Job
import sys, traceback
 
logger = getLogger()

# If you want crabCommand to be quiet:
#from CRABClient.UserUtilities import setConsoleLogLevel, LOGLEVEL_MUTE
#setConsoleLogLevel(LOGLEVEL_MUTE)
# With this function you can change the console log level at any time.



class CRABBackend(IBackend):
    """Backend implementation for CRAB."""
    schemadic = {}
    schemadic['verbose']        = SimpleItem(defvalue=1,
                                      typelist=['int'],
                                      doc='Set to 0 to disable CRAB logging')
    schemadic['statusLog']      = SimpleItem(defvalue=0,
                                        typelist=['int'],
                                        doc='Set to 1 to keep -status logs')
    schemadic['report']         = SimpleItem(defvalue={})
    schemadic['fjr']            = SimpleItem(defvalue={})
    schemadic['crab_env']       = SimpleItem(defvalue={})
    schemadic['userproxy']      = SimpleItem(defvalue={})
    schemadic['taskname']       = SimpleItem(defvalue=None, typelist=['type(None)', 'str'], doc='taskname of the master job')
    schemadic['requestname']    = SimpleItem(defvalue=None, typelist=['type(None)', 'str'], doc='unique taskname of the master job')
    schemadic['crabid']         = SimpleItem(defvalue=None, typelist=['type(None)', 'str'], doc='id of the crab task subjob')

    schemadic['CRABConfig']     = ComponentItem('CRABConfig',doc='CRAB task submission configuration')

    _schema = Schema(Version(2, 6), schemadic)
    _category = 'backends'
    _name = 'CRABBackend'

    def __init__(self):
        logger.info("crabbackend init")
        super(CRABBackend, self).__init__()
        #config = Config.getConfig('CMSSW')
        #shell = Shell(os.path.join(config['CMSSW_SETUP'], 'CMSSW_generic.sh')) 
        #shell = Shell(os.path.join(config['CMSSW_SETUP'], 'CMSSW_generic.sh'),
        #              [config['CMSSW_VERSION'], config['CRAB_VERSION']])
        #self.crab_env = shell.env

        """
        config = Config.getConfig('CRAB_CFG')
        self.server_name = config['server_name']
        self.apiresource = config['apiresource']
        self.userproxy = config['userproxy']
        self.asyncdest = config['asyncdest']
        logger.info("asyncdest %s" % self.asyncdest )
        """


    def prepare_job_config(self, job):
        """ Generates a CRAB config object from the Ganga job configuration. """

        from WMCore.Configuration import Configuration
        job_config = Configuration()

        for section in job.backend.CRABConfig._schema.datadict.keys():

            section_config = getattr(job.backend.CRABConfig, section)
            ganga_section_config = Config.getConfig('CRABConfig_%s' % section)
            task_section_config = job_config.section_(section)

            for parameter_name, parameter_type in section_config._schema.allItems():
                parameter_value = getattr(section_config, parameter_name)

                if parameter_value not in (None, [None]):
                    
                    # CRAB Config doesn't like Ganga sequence type instead of Lists
                    if parameter_type._meta['sequence']:
                        parameter_value = list(parameter_value)

                    task_section_config.__setattr__(parameter_name, parameter_value)

                # Updating configuration in case of Ganga inline options specified                
                ganga_option = ganga_section_config[parameter_name]
                if ganga_option:
                    task_section_config.__setattr__(parameter_name, ganga_option)

        # Some internal configuration
        job_config.General.workArea = job.outputdir 
 
        return job_config

    def master_submit(self, rjobs, subjobconfigs, masterjobconfig):
        """Perform de submission of the master job (the CRAB task)."""

        job = self.getJobObject()

        # DEBUG 
        logger.info("Same? %s %s" % (rjobs[0].id, job.id))

        if rjobs[0]:

            job_config = self.prepare_job_config(job)
            # DEBUG
            for section in job_config._internal_sections:
                section = getattr(job_config, section)
                print section.dictionary_()

            from CRABAPI.RawCommand import crabCommand
            from CRABClient.ClientExceptions import ConfigurationException
            import httplib

            try:
                res = crabCommand('submit', config = job_config, proxy = '/data/hc/apps/cms/config/x509up_production2')
                job.backend.requestname = res['requestname']
                job.backend.taskname    = res['uniquerequestname']
                job.updateStatus('submitted')

            except httplib.HTTPException as e:
                logger.error(e.result)
                return False
            except ConfigurationException as ce:
                # From CRAB3 error message: Error loading CRAB cache file. Try to do 'rm -rf /root/.crab3' and run the crab command again.
                import subprocess
                import uuid
                randomstring = str(uuid.uuid4().get_hex().upper()[0:6])
                subprocess.call(["mv", "/root/.crab3", "/tmp/.crab3."+randomstring])
                try:
                    statusresult = crabCommand('submit', config = job_config, proxy = '/data/hc/apps/cms/config/x509up_production2')
                    logger.info("CRAB3 Status result: %s" % statusresult)
                    job.backend.requestname = res['requestname']
                    job.backend.taskname    = res['uniquerequestname']
                    job.updateStatus('submitted')

                except httplib.HTTPException as e:
                    logger.error(e.result)

        else:
            logger.warning('No rjobs found')

        return True 

    def master_resubmit(self, rjobs):
        """Performs the resubmission of all the jobs in a jobset."""
        logger.warning("Resubmit not yet implemented.")


    def master_kill(self):
        """ Kills a job & subjobs """

        job = self.getJobObject()

        from CRABAPI.RawCommand import crabCommand
        from CRABClient.ClientExceptions import ConfigurationException
        import httplib
 
        if not job.backend.requestname:
            logger.warning("Couldn't find request name for job %s. Skipping" % s)
            return False

        crab_work_dir = os.path.join(job.outputdir, job.backend.requestname)


        try:
            crabCommand('kill', dir = crab_work_dir, proxy = '/data/hc/apps/cms/config/x509up_production2')
  
            if len(job.subjobs):
                for s in job.subjobs:
                    if not s.status in ['completed','failed']:
                        s.updateStatus('killed')
            else:
                if not job.status in ['completed','failed']:
                    job.updateStatus('killed')

            job.updateMasterJobStatus()


        except httplib.HTTPException as e:
            logger.error("Error while killing job %s" % job.id)
            logger.error(e.result)
            return False
        except ConfigurationException as ce:
           # From CRAB3 error message: Error loading CRAB cache file. Try to do 'rm -rf /root/.crab3' and run the crab command again.
            import subprocess
            import uuid
            randomstring = str(uuid.uuid4().get_hex().upper()[0:6])
            subprocess.call(["mv", "/root/.crab3", "/tmp/.crab3."+randomstring])
            try:
                statusresult = crabCommand('kill', dir = crab_work_dir, proxy = '/data/hc/apps/cms/config/x509up_production2')
 
                if len(job.subjobs):
                    for s in job.subjobs:
                        if not s.status in ['completed','failed']:
                            s.updateStatus('killed')
                else:
                    if not job.status in ['completed','failed']:
                        job.updateStatus('killed')
 
                job.updateMasterJobStatus()
            except httplib.HTTPException as e:
                logger.error(e.result)

        return True


    def postMortem(self,job):

        logger.info('postMortem')

        return 1

    def getStats(self, jobstatus):
        """ Retrieve jobs statistics. """

        job = self.getJobObject()

        for metric, value in jobstatus.items():
           job.backend.report[metric] = value

        logger.info(job.backend.report)
           

    def updateSubjobStatus(self, jobstatus):

        """
        GANGA_S = ['completed','failed','killed','new','running','submitted','submitting']
        STATUS  = {'A':'aborted',
                   'C':'created',
                   'CS':'created on the server',
                   'DA':'failed',
                   'E':'cleared',
                   'K':'killed',
                   'R':'running',
                   'SD':'done',
                   'SR':'ready',
                   'S':'submitted on the server',
                   'SS':'scheduled',
                   'SU':'submitted',
                   'SW':'waiting',
                   'W':'declared',
                   'UN':'undefined',
                   }
        """
        state = jobstatus['State']
        job = self.getJobObject()

        if state in ['cooloff', 'unsubmitted', 'idle']:
            if job.status not in ['submitted']:
                job.updateStatus('submitted')

        elif state in ['running', 'trasferring']:
            if job.status in ['submitting']:
                job.updateStatus('submitted')
            elif job.status not in ['running']:
                job.updateStatus('running')

        elif state in ['failed', 'held']:
            if job.status in ['submitting']:
                job.updateStatus('submitted')
            elif job.status not in ['failed']:
                job.updateStatus('failed')

        elif state == 'killed':
           if job.status in ['submitting']:
                job.updateStatus('submitted')
           elif job.status not in ['killed']:
                job.updateStatus('killed')

        elif state=='finished':
            if job.status in ['submitting']:
                job.updateStatus('submitted')
            elif job.status not in['completed']:
                job.backend.getStats(jobstatus)
                job.updateStatus('completed')
                #logger.info('retrieving job output for job %s' % job.id) 

        else:
            logger.warning('UNKNOWN JOB STATUS "%s". Cannnot update job status.' % state)  


    def master_updateMonitoringInformation(jobs):
        """Updates the statuses of the list of jobs provided by issuing crab -status."""
        logger.info('Updating the monitoring information of ' + str(len(jobs)) + ' jobs')

        from CRABAPI.RawCommand import crabCommand
        from CRABClient.ClientExceptions import ConfigurationException
        import httplib

        for j in jobs:

            logger.info('Updating monitoring information for job %d (%s)' % (j.id, j.status))
            if not j.backend.requestname:
                logger.warning("Couldn't find request name for job %s. Skipping" % s)
                continue
            crab_work_dir = os.path.join(j.outputdir, j.backend.requestname)
            logger.info('crab_work_dir: %s' % crab_work_dir)

            statusresult = {}
            try:
                statusresult = crabCommand('status', dir = crab_work_dir, proxy = '/data/hc/apps/cms/config/x509up_production2', long=True)
                logger.info("CRAB3 Status result: %s" % statusresult)
            except httplib.HTTPException as e:
                logger.error(e.result)
            except ConfigurationException as ce:
                # From CRAB3 error message: Error loading CRAB cache file. Try to do 'rm -rf /root/.crab3' and run the crab command again.
                import subprocess
                import uuid
                randomstring = str(uuid.uuid4().get_hex().upper()[0:6])
                subprocess.call(["mv", "/root/.crab3", "/tmp/.crab3."+randomstring])
                try:
                    statusresult = crabCommand('status', dir = crab_work_dir, proxy = '/data/hc/apps/cms/config/x509up_production2', long=True)
                    logger.info("CRAB3 Status result: %s" % statusresult)
                except httplib.HTTPException as e:
                    logger.error(e.result)            

            try:
               jobsdict = statusresult['jobs']
            except KeyError:
               jobsdict = {}

            if jobsdict:
                logger.info('There are subjob statuses for job %s' % j.id)
                if not j.subjobs:
                    logger.warning('No subjob object for job %s' % j.id)
                    subjoblist = [None] * len(jobsdict)
                    #j.subjobs = [None] * len(jobsdict)
                    #subjob_index = 0
                    for crabid, status in jobsdict.items():
                        crabid = int(crabid)
                        jobstatus = status['State']
                        logger.info('Creating subjob')
                        sj = Job()
                        sj.copyFrom(j)
                        sj.backend.crabid = crabid
                        sj.inputdata = None
                        sj.id = crabid-1
                        sj.updateStatus('submitting')
                        sj.backend.updateSubjobStatus(status)
                        subjoblist[crabid-1] = sj

                    for newsubjob in subjoblist:
                      j.subjobs.append(newsubjob)
                    logger.info('New subjobs for job %s: %s' % (j.id, j.subjobs))

                    #j.subjobs.sort(key=lambda subjob: subjob.id)

                else:
                    for crabid, status in jobsdict.items():
                        crabid = int(crabid)
                        j.subjobs[crabid-1].backend.updateSubjobStatus(status)

                #j.updateStatus('running')

            else:
                logger.info('There are no subjobs for job %s' % (j.id))
                #logger.info('Checking task status from report: %s' % statusresult['status'])
                logger.info('Checking task status from report')
                try:
                    taskstatus = statusresult['status']
                    if taskstatus in ['FAILED', 'SUBMITFAILED']:
                        logger.info('Job failed: %s' % dictresult)
                        j.updateStatus('failed')
                except KeyError:
                    pass

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)

