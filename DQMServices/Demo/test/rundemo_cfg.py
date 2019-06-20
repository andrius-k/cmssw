from builtins import range
import FWCore.ParameterSet.Config as cms
from DQMServices.Core.DQMEDAnalyzer import DQMEDAnalyzer
from DQMServices.Core.DQMEDHarvester import DQMEDHarvester

process = cms.Process("TEST")

process.options = cms.untracked.PSet()
process.options.numberOfThreads = cms.untracked.uint32(1)
process.options.numberOfStreams = cms.untracked.uint32(1)

process.source = cms.Source("EmptySource", numberEventsInRun = cms.untracked.uint32(10),
                            firstLuminosityBlock = cms.untracked.uint32(1),
                            firstEvent = cms.untracked.uint32(1),
                            numberEventsInLuminosityBlock = cms.untracked.uint32(5))

process.maxEvents = cms.untracked.PSet( input = cms.untracked.int32(10) )

process.ana1 = DQMEDAnalyzer("DemoNormalDQMEDAnalyzer",
  folder = cms.string("DemoSubsystem1"),
)

process.ana2 = DQMEDAnalyzer("DemoNormalDQMEDAnalyzer",
  folder = cms.string("DemoSubsystem2"),
)

process.ana3 = cms.EDAnalyzer("DemoGlobalDQMEDAnalyzer",
  folder = cms.string("DemoSubsystem3"),
)

process.ana4 = cms.EDAnalyzer("DemoGlobalDQMEDAnalyzer",
  folder = cms.string("DemoSubsystem4"),
)

process.harv1 = DQMEDHarvester("DemoHarvester",
  target = cms.string("DemoSubsystem1"),
)

process.harv2 = DQMEDHarvester("DemoHarvester",
  target = cms.string("DemoSubsystem3"),
)

process.harv3 = DQMEDHarvester("DemoHarvester",
  target = cms.string("DemoSubsystem1_lumisummary"),
)

process.demo_reco_dqm = cms.Task(process.ana1, process.ana2)
process.demo_harvesting = cms.Task(process.harv1, process.harv2)

process.p = cms.Path(process.demo_reco_dqm, process.demo_harvesting)
# process.p = cms.Path()
process.e = cms.EndPath(process.harv3);

process.out = cms.OutputModule(
  "DQMRootOutputModule",
  fileName = cms.untracked.string("dqm_file1.root")
)

process.dqmSaver = cms.EDAnalyzer("DQMFileSaver",
    convention = cms.untracked.string('Offline'),
    fileFormat = cms.untracked.string('ROOT'),
    producer = cms.untracked.string('DQM'),
    workflow = cms.untracked.string('/A/B/C'),
    dirName = cms.untracked.string('.'),
)

#process.o = cms.EndPath(process.out + process.dqmSaver)

process.add_(cms.Service("Tracer"))

# from FWCore.ParameterSet.Utilities import convertToUnscheduled
# process = convertToUnscheduled(process)

