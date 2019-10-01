import FWCore.ParameterSet.Config as cms

from DQM.Physics.topSingleLeptonDQM_miniAOD_cfi import *
from DQM.Physics.singleTopDQM_miniAOD_cfi import *

topPhysicsminiAOD = cms.Sequence(    topSingleMuonMediumDQMminiAOD 
                                     *topSingleElectronMediumDQMminiAOD 
     				     *singleTopMuonMediumDQMminiAOD
   				     *singleTopElectronMediumDQMminiAOD
                                 )
