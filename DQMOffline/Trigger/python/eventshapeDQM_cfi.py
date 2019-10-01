import FWCore.ParameterSet.Config as cms

from DQMServices.Core.DQMEDAnalyzer import DQMEDAnalyzer
eventshapeDQMQ2top05cent1030 = DQMEDAnalyzer('EventShapeDQM',
        triggerResults = cms.InputTag('TriggerResults','','HLT'),
        EPlabel = cms.InputTag("hltEvtPlaneProducer"),
        triggerPath = cms.string('HLT_HIQ2Top005_Centrality1030_v'),
        order = cms.int32(2),
        EPidx = cms.int32(8), #HF2
        EPlvl = cms.int32(0)
)

eventshapeDQMQ2bottom05cent1030 = DQMEDAnalyzer('EventShapeDQM',
        triggerResults = cms.InputTag('TriggerResults','','HLT'),
        EPlabel = cms.InputTag("hltEvtPlaneProducer"),
        triggerPath = cms.string('HLT_HIQ2Bottom005_Centrality1030_v'),
        order = cms.int32(2),
        EPidx = cms.int32(8), #HF2
        EPlvl = cms.int32(0)
)

eventshapeDQMQ2top05cent3050 = DQMEDAnalyzer('EventShapeDQM',
        triggerResults = cms.InputTag('TriggerResults','','HLT'),
        EPlabel = cms.InputTag("hltEvtPlaneProducer"),
        triggerPath = cms.string('HLT_HIQ2Top005_Centrality3050_v'),
        order = cms.int32(2),
        EPidx = cms.int32(8), #HF2
        EPlvl = cms.int32(0)
)

eventshapeDQMQ2bottom05cent3050 = DQMEDAnalyzer('EventShapeDQM',
        triggerResults = cms.InputTag('TriggerResults','','HLT'),
        EPlabel = cms.InputTag("hltEvtPlaneProducer"),
        triggerPath = cms.string('HLT_HIQ2Bottom005_Centrality3050_v'),
        order = cms.int32(2),
        EPidx = cms.int32(8), #HF2
        EPlvl = cms.int32(0)
)

eventshapeDQMQ2top05cent5070 = DQMEDAnalyzer('EventShapeDQM',
        triggerResults = cms.InputTag('TriggerResults','','HLT'),
        EPlabel = cms.InputTag("hltEvtPlaneProducer"),
        triggerPath = cms.string('HLT_HIQ2Top005_Centrality5070_v'),
        order = cms.int32(2),
        EPidx = cms.int32(8), #HF2
        EPlvl = cms.int32(0)
)

eventshapeDQMQ2bottom05cent5070 = DQMEDAnalyzer('EventShapeDQM',
        triggerResults = cms.InputTag('TriggerResults','','HLT'),
        EPlabel = cms.InputTag("hltEvtPlaneProducer"),
        triggerPath = cms.string('HLT_HIQ2Bottom005_Centrality5070_v'),
        order = cms.int32(2),
        EPidx = cms.int32(8), #HF2
        EPlvl = cms.int32(0)
)



eventshapeDQMSequence = cms.Sequence(eventshapeDQMQ2top05cent1030 * eventshapeDQMQ2bottom05cent1030 * eventshapeDQMQ2top05cent3050 * eventshapeDQMQ2bottom05cent3050 * eventshapeDQMQ2top05cent5070 * eventshapeDQMQ2bottom05cent5070 )
