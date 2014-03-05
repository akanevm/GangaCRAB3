from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import Schema, Version
from Ganga.Utility.logging import getLogger
#from GangaCMS.Lib.CRABTools.CRABServerError import CRABServerError
#from origRemoteCopy import remote_copy as remoteCopy
from RESTInteractions import HTTPRequests
from httplib import HTTPException

import datetime
import os
import shlex
import shutil
import subprocess
import time
import urllib
import sys
import json
import hashlib
from optparse import OptionParser, OptionGroup

logger = getLogger()

class CRABServerError(Exception):
    pass

class ResponseHeader(object):
    """ResponseHeader parses HTTP response header"""
    def __init__(self, response):
        super(ResponseHeader, self).__init__()
        self.header = {}
        self.parse(response)
        self.reason = ''
        self.fromcache = False
     
    def parse(self, response):
        """Parse response header and assign class member data"""
        for row in response.split('\r'):
            row = row.replace('\n', '')
            if not row:
                continue
            if row.find('HTTP') != -1 and \
                row.find('100') == -1: #HTTP/1.1 100 found: real header is later
                res = row.replace('HTTP/1.1', '')
                res = res.replace('HTTP/1.0', '')
                res = res.strip()
                status, reason = res.split(' ', 1)
                self.status = int(status)
                self.reason = reason
                continue
            try:
                key, val = row.split(':', 1)
                self.header[key.strip()] = val.strip()
            except:
                pass

class CRABServer(GangaObject):
    """Helper class to launch CRAB commands."""
    _schema = Schema(Version(0, 0), {})
    _hidden = True

    def checkX509(self):
        """
        """
        if os.environ.has_key('X509_USER_PROXY'):
            cert = os.environ['X509_USER_PROXY']
            key = os.environ['X509_USER_PROXY']
            cacert = os.environ["X509_CERT_DIR"]
        else:
            print 'error $X509_USER_CERT not defined'
            sys.exit(1)
        return key, cert, cacert

    def createArchive(self, cfgOutName , name = '', mode = 'w:gz'):
        """
        create the archive to upload
        """
         
        if not name:
            import uuid
            name = os.path.join(os.getcwd(), str(uuid.uuid4()) +'default.tgz')
         
        import tarfile
        print 'opening tar file'
        tarfile = tarfile.open(name=name , mode=mode, dereference=True)
        print 'adding %s to the tarball' % cfgOutName
        tarfile.add(cfgOutName, arcname='PSet.py')
         
        #checkSum
        print 'calculating the checksum'
        lsl = [(x.name, int(x.size), int(x.mtime), x.uname) for x in tarfile.getmembers()]
        # hasher = hashlib.md5(str(lsl))
        hasher = hashlib.sha256(str(lsl))
        checksum = hasher.hexdigest()
        #end
        tarfile.close()
         
        return name, checksum
    
    def uploadArchive(self, cfgOutName, url ):
        """
        Upload the tarball to the User File Cache
        """
        archiveName, checksum = self.createArchive( cfgOutName )
         
        params = [('hashkey', checksum)]
         
        print "uploading archive to cache %s " % archiveName
         
        result = self.uploadFile(archiveName, url, fieldName='inputfile', params=params, verb = 'PUT')
        if 'hashkey' not in result:
            print ("Failed to upload source files: %s" % str(result))
            sys.exit(1)
        
        os.remove(archiveName) 
        return url, str(result['hashkey']) + '.tar.gz', checksum

    def uploadFile(self, fileName, url, fieldName = 'file1', params = [], verb = 'POST'):
        """
        Upload a file with curl streaming it directly from disk
        """
        ckey, cert, capath = self.checkX509()
         
        from httplib import HTTPException
        # import tempfile
        try:
            import cStringIO as StringIO
        except:
            import StringIO
        import pycurl
        c = pycurl.Curl()
        if verb == 'POST':
            c.setopt(c.POST, 1)
        elif verb == 'PUT':
            c.setopt(pycurl.CUSTOMREQUEST, 'PUT')
        else:
            raise HTTPException("Verb %s not sopported for upload." % verb)
        print url
        c.setopt(c.URL, url)
        fullParams = [(fieldName, (c.FORM_FILE, fileName))]
        fullParams.extend(params)
        c.setopt(c.HTTPPOST, fullParams)
        bbuf = StringIO.StringIO()
        hbuf = StringIO.StringIO()
        c.setopt(pycurl.WRITEFUNCTION, bbuf.write)
        c.setopt(pycurl.HEADERFUNCTION, hbuf.write)
        if capath:
            c.setopt(pycurl.CAPATH, capath)
            c.setopt(pycurl.SSL_VERIFYPEER, True)
        else:
            c.setopt(pycurl.SSL_VERIFYPEER, False)
        if ckey:
            c.setopt(pycurl.SSLKEY, ckey)
        if cert:
            c.setopt(pycurl.SSLCERT, cert)
        c.perform()
        hres = hbuf.getvalue()
        bres = bbuf.getvalue()
        rh = ResponseHeader(hres)
        c.close()
        print ' '
        print 'status is %s' % rh.status
        print ' '
        if rh.status < 200 or rh.status >= 300:
            exc = HTTPException(bres)
            setattr(exc, 'req_data', fullParams)
            setattr(exc, 'url', url)
            setattr(exc, 'result', bres)
            setattr(exc, 'status', rh.status)
            setattr(exc, 'reason', rh.reason)
            setattr(exc, 'headers', rh.header)
            print exc.req_data
            print exc.status
            print exc.reason
            raise exc
         
        return json.loads(bres)['result'][0]
    


    def _send(self, cmd, operation, env):
        """Launches a command and waits for output."""
        try:
            logger.debug('Launching a CRAB command: %s' % cmd)
            init = datetime.datetime.now()
            p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, bufsize=-1, env=env)
            stdout, stderr = p.communicate()
            p.wait()
            end = datetime.datetime.now()
            logger.debug('Finished CRAB command: %s' % cmd)
            logger.info('%s took %d sec.' % (operation, (end - init).seconds))

            if p.returncode != 0:
                raise CRABServerError('CRAB %s returned %s' % (operation,
                                                               p.returncode))
        except OSError, e:
            logger.error(stdout)
            logger.error(stderr)
            raise CRABServerError(e, 'OSError %s crab job(s).' % operation)

    def _send_with_retry(self, cmd, operation, env, retries=3, delay=60):
        """Wrapper to add some retries to the CRAB command launching."""
        assert retries > 0
        assert delay >= 0

        for _ in range(retries):
            try:
                self._send(cmd, operation, env)
                return
            except CRABServerError:
                time.sleep(delay)

        raise CRABServerError('CRAB %s failed %d times' % (operation, retries))

    def _encodeRequest(self, configreq):
        """ Used to encode the request from a dict to a string. Include the code needed for transforming lists in the format required by
            cmsweb, e.g.:   adduserfiles = ['file1','file2']  ===>  [...]adduserfiles=file1&adduserfiles=file2[...]
        """
        listParams = ['adduserfiles', 'addoutputfiles', 'sitewhitelist', 'siteblacklist', 'blockwhitelist', 'blockblacklist',
                      'tfileoutfiles', 'edmoutfiles', 'runs', 'lumis'] #TODO automate this using ClientMapping
        encodedLists = ''
        for lparam in listParams:
            if lparam in configreq:
                if len(configreq[lparam])>0:
                    encodedLists += ('&%s=' % lparam) + ('&%s=' % lparam).join( map(urllib.quote, configreq[lparam]) )
                del configreq[lparam]
        encoded = urllib.urlencode(configreq) + encodedLists
        return encoded


    def create(self, job):
        """Create a new CRAB jobset."""
        cfgfile = os.path.join(job.inputdir, 'crab.cfg')
        if not os.path.isfile(cfgfile):
            raise CRABServerError('File "%s" not found.' % cfgfile)

        # Clean up the working dir for the CRAB UI.
        shutil.rmtree(job.inputdata.ui_working_dir, ignore_errors=True)

        cmd = 'crab -create -cfg %s' % cfgfile
        self._send_with_retry(cmd, 'create', job.backend.crab_env)
        return True

    def submit(self, job):
        """Submit a new task to CRAB3 """
        logger.info('userproxy: %s' % job.backend.userproxy)
        logger.info('server_name: %s' % job.backend.server_name)
        logger.info('apiresource: %s' % job.backend.apiresource)

        server = HTTPRequests(job.backend.server_name, job.backend.userproxy)
        resource = job.backend.apiresource+'workflow'
        
        try:
            cachefilename = self.uploadArchive(job.inputdata.pset, job.inputdata.cacheurl)[1]
        except HTTPException, e:
            logger.error(type(e))
            logger.error(dir(e))
            logger.error(e.req_headers)
            logger.error(e.req_data)
            logger.error(e.reason)
            logger.error(e.message)
            logger.error(e.headers)
            logger.error(e.result)
            logger.error(e.status)
            logger.error(e.url)
            logger.error(e.args)
            raise CRABServerError("Error uploading cache")

        specFields = ['workflow',
                      'cacheurl',
                      'publishname',
                      'savelogsflag',
                      'nonprodsw',
                      'tfileoutfiles',
                      'asyncdest',
                      'oneEventMode',
                      'algoargs',
                      'totalunits',
                      'cachefilename', 
                      'jobarch', 
                      'publication',
                      'splitalgo',
                      'jobsw',
                      'dbsurl',
                      'addoutputfiles',
                      'edmoutfiles',
                      'adduserfiles',
                      'jobtype',
                      'siteblacklist',
                      'sitewhitelist',
                      'userdn',
                      'userhn',
                      'vorole',
                      'vogroup',
                      'runs',
                      'lumis',
                      'maxjobruntime',
                      'numcores',
                      'maxmemory',
                      'priority',
                      'lfnprefix',
                      'saveoutput',
                      'faillimit',
                      'ignorelocality',
                      'inputdata',
                      'publishdbsurl'
                     ]
        spec = {}
        for field in specFields:
            if getattr(job.inputdata, field) not in [None, [None]]:
                spec[field] = getattr(job.inputdata, field)

        spec['cachefilename'] = cachefilename
       
        if job.backend.asyncdest:
            spec['asyncdest'] = job.backend.asyncdest
 
        """ 
        spec = {'workflow': job.inputdata.workflow,
                'cacheurl': job.inputdata.cacheurl,
                'publishname': job.inputdata.publishname,
                'savelogsflag': job.inputdata.savelogsflag,
                'nonprodsw': job.inputdata.nonprodsw,
                'tfileoutfiles': job.inputdata.tfileoutfiles,
                'asyncdest': job.inputdata.asyncdest,
                'oneEventMode': job.inputdata.oneEventMode,
                'algoargs': job.inputdata.algoargs,
                'totalunits': job.inputdata.totalunits,
                'cachefilename': cachefilename,
                'jobarch': job.inputdata.jobarch,
                'publication': job.inputdata.publication,
                'splitalgo': job.inputdata.splitalgo,
                'jobsw': job.inputdata.jobsw,
                'dbsurl': job.inputdata.dbsurl,
                'addoutputfiles': job.inputdata.addoutputfiles,
                'edmoutfiles': job.inputdata.edmoutfiles,
                'adduserfiles': job.inputdata.adduserfiles,
                'inputdata': job.inputdata.inputdata,
                'jobtype': job.inputdata.jobtype 
               }
        """
        logger.debug('spec = %s ' % spec)
        #spec = {'workflow': 'crab_20140129_174310', 'cacheurl': 'https://cmsweb.cern.ch/crabcache/file', 'publishname': '161f88b7224ebec344e685476aab1797', 'savelogsflag': 0, 'nonprodsw': 0, 'tfileoutfiles': [], 'asyncdest': 'T2_IT_Pisa', 'oneEventMode': 0, 'algoargs': 10, 'totalunits': 0, 'cachefilename': 'b813537a3eded4fb83426fa1e0d0fd6f09fe17f344371d1ad5d64607e3a1c44e.tar.gz', 'jobarch': 'slc5_amd64_gcc462', 'publication': 0, 'splitalgo': 'FileBased', 'jobsw': 'CMSSW_5_3_4', 'dbsurl': 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet', 'addoutputfiles': [], 'edmoutfiles': ['myroot.root'], 'adduserfiles': [], 'inputdata': '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO', 'jobtype': 'Analysis'}

        try:
            dictresult, status, reason = server.put( resource, data = self._encodeRequest(spec) )
            logger.debug("dictresult %s, status %s, reason: %s" % (dictresult, status,reason))
            job.backend.taskname = dictresult['result'][0]['RequestName']
            return True
        except HTTPException, e:
            logger.error(type(e))
            logger.error(dir(e))
            logger.error(e.req_headers)
            logger.error(e.req_data)
            logger.error(e.reason)
            logger.error(e.message)
            logger.error(e.headers)
            logger.error(e.result)
            logger.error(e.status)
            logger.error(e.url)
            logger.error(e.args)
            raise CRABServerError("Error submitting task")

        return False

    def status(self, job):
        """Get the status of a jobset."""
        """
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -status -c %s' % job.inputdata.ui_working_dir
        self._send_with_retry(cmd, 'status', job.backend.crab_env)
        return True
        """
        #from RESTInteractions import HTTPRequests

        logger.info('checkin status')

        try:
            server = HTTPRequests(job.backend.server_name, job.backend.userproxy)
            resource = job.backend.apiresource+'workflow'
            dictresult, status, reason = server.get(resource, data = { 'workflow' : job.backend.taskname})
            logger.info("status %s, reason %s" % (status, reason))
            return dictresult, status, reason

        except HTTPException, e:
            print type(e)
            print dir(e)
            print e.req_headers
            print e.req_data
            print e.reason
            print e.message
            print e.headers
            print e.result
            print e.status
            print e.url
            print e.args

            raise e 



    def kill(self, job):
        """Kill all the jobs on the task."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        if not job.master:
            cmd = 'crab -kill all -c %s' % job.inputdata.ui_working_dir
        else:
            cmd = 'crab -kill %d -c %s' % (int(job.id) + 1,
                                           job.inputdata.ui_working_dir)
        self._send_with_retry(cmd, 'kill', job.backend.crab_env)
        return True

    def resubmit(self, job):
        """Resubmit an already created job."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -resubmit %d -c %s' % (int(job.id) + 1,
                                           job.inputdata.ui_working_dir)
        self._send_with_retry(cmd, 'resubmit', job.backend.crab_env)
        return True

    def getOutput(self, job):
        """Retrieve the output of the job."""
        """
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir "%s" not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -getoutput %d -c %s' % (int(job.id) + 1,
                                            job.inputdata.ui_working_dir)
        self._send_with_retry(cmd, 'getoutput', job.backend.crab_env)
        # Make output files coming from the WMS readable.
        for root, _, files in os.walk(os.path.join(job.inputdata.ui_working_dir,
                                                   'res')): # Just 'res'.
            for f in files:
                os.chmod(os.path.join(root, f), 0644)
        """
        logger.info('geting Output for jon %s:%s' % (job.backend.taskname, job.backend.crabid)) 
        inputlist =  [  ('workflow', job.backend.taskname)]
        inputlist.extend([('subresource', 'logs')])
        inputlist.extend( [('jobids', job.backend.crabid)] )

        #srv='hammer-crab3.cern.ch'#  'cmsweb-testbed.cern.ch'
        #proxypath= '/afs/cern.ch/user/r/riahi/public/proxy'#'/afs/cern.ch/user/s/spiga/public/PerValentaina/proxy'
        #resource='/crabserver/dev/workflow'
        #server = HTTPRequests(srv, proxypath)

        server = HTTPRequests(job.backend.server_name, job.backend.userproxy)

        resource = job.backend.apiresource+'workflow'

        try:
            dictresult, status, reason = server.get(resource, data = inputlist)

        except HTTPException, e:
            print type(e)
            print dir(e)
            print e.req_headers
            print e.req_data
            print e.reason
            print e.message
            print e.headers
            print e.result
            print e.status
            print e.url
            print e.args

            #sys.exit(1)
        logger.info('dictresult: %s' % dictresult)
        #input = [{u'checksum': {u'adler32': u'6d1096fe', u'cksum': 3701783610, u'md5': u'6d1096fe'}, u'size': 213441, u'pfn': u'srm://srm.ihepa.ufl.edu:8443/srm/v2/server?SFN=/cms/data/store/temp/user/spiga.75e763e2996c16e51aadd11c9203b9b48af92491/GenericTTbar/140207_160506_crab_20140129_174310/161f88b7224ebec344e685476aab1797/0000/log/cmsRun_6.log.tar.gz'}]
        #rcopy = remoteCopy(input, job.inputdata.ui_working_dir, logger)
        #rcopy()       
        return True

    def postMortem(self, job):
        """Retrieves the postmortem information."""
        if not os.path.exists(job.inputdata.ui_working_dir):
            raise CRABServerError('Workdir %s not found.' %
                                  job.inputdata.ui_working_dir)

        cmd = 'crab -postMortem %d -c %s' % (int(job.id) + 1,
                                             job.inputdata.ui_working_dir)
        self._send_with_retry(cmd, 'postMortem', job.backend.crab_env)
        return True
