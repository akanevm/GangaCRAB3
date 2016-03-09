Job (
  application = CRABApp (), 
  backend = CRABBackend (
    CRABConfig = CRABConfig(
    General = General(
      requestName = 'HCAPItest',
      transferOutputs = False,
      transferLogs = False),
    JobType = JobType(
      psetName = '/data/hc/apps/cms/inputfiles/usercode/pf2pat_v7_cfg.py',
      pluginName = 'Analysis'),
    Data    = Data(
      inputDataset = '/GenericTTbar/HC-CMSSW_7_0_4_START70_V7-v1/GEN-SIM-RECO',
      splitting = 'LumiBased',
      unitsPerJob = 10,
      totalUnits = 20,
      ignoreLocality = True,
      publication = False,
      outputDatasetTag = '0ea12bcd230936c2556840cb8452714d'),
    User    = User(
      voRole='production'),
    Debug   = Debug(
      extraJDL = ['+CRAB_NoWNStageout=1']),
    Site    = Site(
      blacklist = ['T3*'],
      storageSite = 'T2_CH_CERN')
    
    )
  )
)
