import FWCore.ParameterSet.Config as cms

from DQMServices.Core.DQMEDAnalyzer import DQMEDAnalyzer
heavyionUCCDQMHIUCC100 = DQMEDAnalyzer('HeavyIonUCCDQM',
        triggerResults = cms.InputTag('TriggerResults','','HLT'),
        caloMet = cms.InputTag('hltMetForHf'),
        pixelCluster = cms.InputTag('hltHISiPixelClusters'),
        triggerPath = cms.string('HLT_HIUCC100_v'),
        nClusters = cms.int32(100),
        minClusters = cms.int32(50000),
        maxClusters = cms.int32(100000),
        nEt = cms.int32(100),
        minEt = cms.double(4000),
        maxEt = cms.double(8000)
)

heavyionUCCDQMHIUCC020 = DQMEDAnalyzer('HeavyIonUCCDQM',
        triggerResults = cms.InputTag('TriggerResults','','HLT'),
        caloMet = cms.InputTag('hltMetForHf'),
        pixelCluster = cms.InputTag('hltHISiPixelClusters'),
        triggerPath = cms.string('HLT_HIUCC020_v'),
        nClusters = cms.int32(100),
        minClusters = cms.int32(50000),
        maxClusters = cms.int32(100000),
        nEt = cms.int32(100),
        minEt = cms.double(4000),
        maxEt = cms.double(8000)
)



HeavyIonUCCDQMSequence = cms.Sequence(heavyionUCCDQMHIUCC100 * heavyionUCCDQMHIUCC020)
