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
import sys

from Ganga.GPIDev.Lib.Job.Job import Job
import sys, traceback
 
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
    schemadic['report'] = SimpleItem(defvalue={})
    schemadic['fjr'] = SimpleItem(defvalue={})
    schemadic['crab_env'] = SimpleItem(defvalue={})
    schemadic['taskname'] = SimpleItem(defvalue=None, typelist=['type(None)', 'str'], doc='taskname of the master job')
    schemadic['crabid'] = SimpleItem(defvalue=None, typelist=['type(None)', 'str'], doc='id of the crab task subjob')
    schemadic['server_name']    = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc='')
    schemadic['apiresource']    = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc='')
    schemadic['userproxy']      = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc='')
    schemadic['asyncdest']      = SimpleItem(defvalue=None, typelist=['type(None)','str'], doc='')

    _schema = Schema(Version(1, 0), schemadic)
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

        config = Config.getConfig('CRAB_CFG')
        self.server_name = config['server_name']
        self.apiresource = config['apiresource']
        self.userproxy = config['userproxy']
        self.asyncdest = config['asyncdest']
        logger.info("asyncdest %s" % self.asyncdest )
        #self.server = CRABServer()


    def master_submit(self, rjobs, subjobconfigs, masterjobconfig):
        """Perform de submission of the master job (the CRAB task)."""
        logger.info("in master_submit rjobs[0]: %s" % rjobs[0]) 

        job = self.getJobObject()
         
        if rjobs[0]:
            #job = rjobs[0]#.master
            #for subjob in job.subjobs:
            #    subjob.updateStatus('submitting')

            try:
                server = CRABServer()
                server.submit(job)
            except CRABServerError:
                logger.error('Submission through CRAB failed.')
                #for subjob in job.subjobs:
                #    subjob.rollbackToNewState()
                #job.updateMasterJobStatus()
                #logger.info('All subjobs have been reverted to "new".')
                return False

            # This will perform a crab -status and parse the XML.
            self.master_updateMonitoringInformation((job,))

            # Forcing all the jobs to be submitted, so the monitoring loops
            # keeps issuing calls after to update.
            #for subjob in job.subjobs:
            #    if subjob.status in ('submitting'):
            #        subjob.updateStatus('submitted')
            
            #dictresult, status, reason = CRABServer().status(job)
            #jobList = dictresult['result'][0]['jobList']


            #job.updateMasterJobStatus()
        else:
            logger.warning('Not submitting job without subjobs.')

        return True

    def master_resubmit(self, rjobs):
        """Performs the resubmission of all the jobs in a jobset."""
        if rjobs[0]:
            job = rjobs[0].master
            for subjob in job.subjobs:
                subjob.updateStatus('submitting')

            try:
                CRABServer().resubmit(job)
            except CRABServerError:
                logger.error('Resubmission through CRAB failed.')
                for subjob in job.subjobs:
                    subjob.rollbackToNewState()
                job.updateMasterJobStatus()
                logger.info('All subjobs have been reverted to "new".')
                return False

            # This will perform a crab -status and parse the XML.
            self.master_updateMonitoringInformation((job,))

            # Forcing all the jobs to be submitted, so the monitoring loops
            # keeps issuing calls after to update.
            for subjob in job.subjobs:
                if subjob.status in ('submitting'):
                    subjob.updateStatus('submitted')

            job.updateMasterJobStatus()
        else:
            logger.warning('Not resubmitting job without subjobs.')
        return True

    def master_kill(self):

        #Kills a job & subjobs
        job = self.getJobObject()
        server = CRABServer()

        try:
            server.kill(job)    
        except Exception as e:
            logger.warning('Killing the job using CRAB failed.')
            logger.warning(e)
            logger.warning(sys.exc_info()[0])
            return 1

        if len(job.subjobs):
            for s in job.subjobs:
                if not s.status in ['completed','failed']:
                    s.updateStatus('killed')   
        else:
            if not job.status in ['completed','failed']:
                job.updateStatus('killed')

        job.updateMasterJobStatus()        

        try:
            server.status(job)
        except:
            logger.warning('Get job status from CRAB failed. Job may have not be killed.')

        return 1

    def postMortem(self,job):

        logger.info('postMortem')

        #Gets post Mortem imformation of failed job
        server = CRABServer()
        try:
            server.postMortem(job)
        except:
            logger.warning('PostMortem retrival with CRAB failed.')

        job.updateMasterJobStatus()
        return 1

    def parseResults(self):

        """
        job = self.getJobObject()   

        server = CRABServer()
        try:
            server.status(job)
            server.getOutput(job) 
        except:
            logger.error('Could not get the output of the job.')
            # Let's not raise this yet (in case of a double call).
            # raise CRABServerError('Impossible to get the output of the job')

        workdir = job.inputdata.ui_working_dir
        index = int(job.id) + 1
        doc_path = '%s/res/crab_fjr_%d.xml'%(workdir,index)

        if not os.path.exists(doc_path):
            logger.error('FJR %s not found.'%(doc_path))
            return

        try:
            doc = parse(doc_path)   
        except:
            logger.error("Could not parse document. File not present?")
            return
        status = doc.firstChild.getAttribute("Status")

        if status in ["Failed"]:
            self.postMortem(job)
            job.updateStatus('failed')
        elif status in ["Success"]:
            if job.status == 'submitting':
                job.updateStatus('submitted')
            job.updateStatus('completed')
        else:
            logger.warning("UNKNOWN PARSE STATUS: "+str(status))

        config = Config.getConfig('Metrics')
        location = config['location']
        if not os.path.exists(location):
            raise BackendError(0,'Location %s file doesnt exist.'%(location))

        config = ConfigParser()
        config.read(location)      

        #Iterate over all them
        SECTIONS = config.sections()
        if 'report' in SECTIONS:
            SECTIONS.remove('report')

        # Only five sections work here...
        for section in SECTIONS:

            if not job.backend.fjr.has_key(section):
                job.backend.fjr[section] = {}

            performancereport = doc.getElementsByTagName("PerformanceReport")[0]
            performancesummary = performancereport.getElementsByTagName("PerformanceSummary")
            for pfs in performancesummary:
                if pfs.getAttribute("Metric") == section:
                    metrics = pfs.getElementsByTagName("Metric")
                    for metric in metrics:
                        name = metric.getAttribute("Name")
                        if config.has_option(section,name):
                            # Due to the names with minus intead of underscore, we have to do thiw walkarround
                            # to send them to the DB.
                            name = config.get(section,name)
                            if name:
                                job.backend.fjr[section][name] = metric.getAttribute("Value")
        """
        job = self.getJobObject()   
        
        server = CRABServer()
        try:
            server.getOutput(job) 
        except Exception as e:
            logger.error(e)
            logger.error('Could not get the output of the job.')
            return False
            # Let's not raise this yet (in case of a double call).
            # raise CRABServerError('Impossible to get the output of the job')

        workdir = job.outputdir
        index = job.backend.crabid
        doc_path = '%s/FrameworkJobReport-%d.xml'%(workdir,index)

        if not os.path.exists(doc_path):
            logger.error('FJR %s not found.'%(doc_path))
            return False

        try:
            doc = parse(doc_path)   
        except:
            logger.error("Could not parse document. File not present?")
            return False
        status = doc.firstChild.getAttribute("Status")

        config = Config.getConfig('Metrics')
        location = config['location']
        if not os.path.exists(location):
            raise BackendError(0,'Location %s file doesnt exist.'%(location))

        config = ConfigParser()
        config.read(location)      

        #Iterate over all them
        SECTIONS = config.sections()
        if 'report' in SECTIONS:
            SECTIONS.remove('report')

        # Only five sections work here...
        for section in SECTIONS:

            if not job.backend.fjr.has_key(section):
                job.backend.fjr[section] = {}

            for performancereport in doc.getElementsByTagName("PerformanceReport"):
                performancesummary = performancereport.getElementsByTagName("PerformanceSummary")[0]
                if performancesummary.getAttribute("Metric") == section:
                    metrics = performancesummary.getElementsByTagName("Metric")
                    for metric in metrics:
                        name = metric.getAttribute("Name")
                        if config.has_option(section,name):
                            # Due to the names with minus intead of underscore, we have to do thiw walkarround
                            # to send them to the DB.
                            name = config.get(section,name)
                            if name:
                                job.backend.fjr[section][name] = metric.getAttribute("Value")
 
        logger.info("Parsed output for job %s" % job.backend.crabid)
        logger.info(job.backend.fjr)
        return True    

    def checkReport(self, report):

        job = self.getJobObject()     
        """
        config = Config.getConfig('Metrics')
        location = config['location']
        if not os.path.exists(location):
            raise BackendError(0,'Location %s file doesnt exist.'%(location))

        config = ConfigParser()
        config.read(location)      

        PARAMS = [('status','status')]

        if config.has_section('report'):
            PARAMS += config.items('report')
        else:
            logger.warning('No report in metrics')

        for n,v in PARAMS:
            if v:
                job.backend.report[v] = jobDoc.getAttribute(v)
        """
        job.backend.report['status'] = report[0]

    def checkStatus(self):

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

        job = self.getJobObject()
        try:
            status = job.backend.report['status']
            logger.debug('job status: %s' % status)      
        except:
            logger.warning('Missing the status for the job %d while checking' % job.id)
            return

        #if status in ['cooloff'] and job.status not in ['submitting']:
        #    job.updateStatus('submitting')

        if status in ['cooloff', 'unsubmitted', 'idle'] and job.status not in ['submitted']:
            job.updateStatus('submitted')

        elif status in ['running', 'trasferring']:
            if job.status in ['submitting']:
                job.updateStatus('submitted')
            elif job.status not in ['running']:
                job.updateStatus('running')

        elif status in ['failed', 'held']:
            if job.status in ['submitting']:
                job.updateStatus('submitted')
            elif job.status not in ['failed']:
                job.updateStatus('failed')

        elif status=='finished':
            if job.status in ['submitting']:
                job.updateStatus('submitted')
            elif job.status not in['completed']:
                job.backend.parseResults()
                job.updateStatus('completed')
                logger.info('retrieving job output for job %s' % job.id) 
  

        """
        if status=='R' and job.status != 'killed':
            if job.status in ['submitting','new']:
                # The job has to pass by this status at least once.
                job.updateStatus('submitted')
            elif job.status != 'running':
                # A job could come from the died (i.e. completed -> running).
                job.updateStatus('running')
        elif status in ['UN','C','CS','W'] and job.status not in ['submitting','new','killed']:
            logger.warning('The job is an invalid status (%s - %s), it will  be reverted.' % (status, job.status))
            job.rollbackToNewState()
        elif status in ['SU','S','SR','SS','SW'] and job.status not in ['submitted','killed']:
            job.updateStatus('submitted')
        elif status == 'SD' and job.status not in ['completed','failed','killed']:
            logger.info('Retrieving %d.'%(job.id))
            job.backend.parseResults()
            # The job can be done, but failed...
            # So, let's update the status retrieved from the output file.
            if job.status not in ['completed','failed','killed']:
                logger.warning('Processing a done job ended in not final status. Parhaps the output getting failed?')
        elif status in ['A','DA'] and job.status not in ['failed','killed']:
            self.postMortem(job)
            job.updateStatus('failed')
        elif status == 'K' and job.status not in ['killed']:
            job.updateStatus('killed')
        elif status == 'E' and job.status not in ['completed','failed','killed']:
            logger.info('Job %d has been purged.'%(job.id))
            job.backend.parseResults()
            # We have to set this now (can be repeated) in case output retrieval fails.
            if job.status not in ['completed','failed','killed']:
                self.postMortem(job)
                job.updateStatus('failed')
        else:
            if not STATUS.has_key(status):  
                logger.warning('UNKNOWN STATUS: ' + str(status))

        # Check the CRAB created jobs that are set as submitted... for a timeout
        if status in ['C','CS','W'] and job.status in ['submitted']:
            try:
                # If submission time is more than one hour ago, a problem happened...
                if datetime.datetime.utcnow() - job.time.timestamps['submitted'] > datetime.timedelta(hours=0.5):
                    logger.info('Submission for job %d failed (timeout).' % job.id)
                    job.updateStatus('failed')
            except:
                logger.warning('Error while retrieving submit time for job %d.' % job.id)
        """

    def checkMasterStatus(self, status):
        loger.info(status)
        job = self.getJobObject()
        if status in ['NEW', 'QUEUED']:
            job.updateStatus('submitting')
        elif status in ['SUBMITTED']:
            job.updateStatus('submitted')
        elif status in ['FAILED']:
            logger.info('about to put at failed')
            job.updateStatus('failed')

    def master_updateMonitoringInformation(jobs):
        """Updates the statuses of the list of jobs provided by issuing crab -status."""
        logger.info('Updating the monitoring information of ' + str(len(jobs)) + ' jobs')
        try:
            server = CRABServer()
            for j in jobs:
                logger.debug('Updating monitoring information for job %d (%s)' % (j.id, j.status))
                try:
                    dictresult, status, reason = server.status(j)
                    logger.info('CRAB3 server call answer status: %s - reason: %s' % (status, reason))
                    joblist = sorted(dictresult['result'][0]['jobList'], key=lambda x:x[1])
                except KeyError:
                    logger.info('Get status for job %d didn\'t return job list, skipping job for now.' % j.id)
                    
                    continue
                except: 
                    logger.error('Get status for job %d failed, skipping.' % j.id)
                    raise

                if joblist:
                    logger.info('There are subjob statuses for job %s' % j.id)
                    logger.info('j: %s' % dir(j))
                    if not j.subjobs:
                        logger.warning('No subjob object for job %s' % j.id)
                        j.subjobs = []
                        for i in xrange(len(joblist)):
                            subjob = joblist[i]
                            index  = int(subjob[1])
                            logger.info('Processing subjob %d, %s' % (index, subjob))
                            sj = Job()
                            sj.copyFrom(j)
                            sj.backend.crabid = index
                            sj.inputdata = None
                            sj.id = i
                            sj.updateStatus('submitting')
                            sj.backend.checkReport(subjob)
                            sj.backend.checkStatus()
                            j.subjobs.append(sj)
                        #j.subjobs = sorted(j.subjobs, key=lambda x: x.backend.id) 
                        #j._commit()
                    else:
                        for subjob in joblist:
                            index  = int(subjob[1])
                            logger.debug('Found subjob %s searching with index %s' % (j.subjobs[index-1].backend.crabid, index))
                            j.subjobs[index-1].backend.checkReport(subjob)                   
                            j.subjobs[index-1].backend.checkStatus()

                    j.updateStatus('running')
                    #j.updateMasterJobStatus()
                else:
                    logger.info('There are no subjobs for job %s' % (j.id))
                    logger.info('checking task status from report: %s' % dictresult['result'][0]['status'])
                    taskstatus = dictresult['result'][0]['status']
                    if taskstatus in ['FAILED']:
                        logger.info('Job failed: %s' % dictresult)
                        j.updateStatus('failed')
        except Exception as e:
            logger.error(e)
            traceback.print_exc(file=sys.stdout)

    master_updateMonitoringInformation = staticmethod(master_updateMonitoringInformation)

