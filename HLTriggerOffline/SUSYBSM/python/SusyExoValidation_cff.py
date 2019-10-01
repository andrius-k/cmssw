from HLTriggerOffline.SUSYBSM.SUSYBSM_HT_MET_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_MET_BTAG_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_inclusiveHT_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_inclusiveMET_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_MET_MUON_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Mu_HT_SingleLepton_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Mu_HT_MET_SingleLepton_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Mu_HT_BTag_SingleLepton_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Mu_HT_Control_SingleLepton_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Ele_HT_SingleLepton_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Ele_HT_MET_SingleLepton_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Ele_HT_BTag_SingleLepton_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Ele_HT_Control_SingleLepton_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_MET_MUON_ER_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_MET_HT_MUON_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_MET_HT_MUON_ER_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_MET_HT_MUON_BTAG_cff import *
from HLTriggerOffline.SUSYBSM.razorHemispheres_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_Razor_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_caloHT_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_PhotonHT_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_HLT_PhotonMET_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_HLT_HT_DoubleMuon_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_HLT_HT_DoubleElectron_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_HLT_HT_MuEle_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_HLT_Muon_BJet_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_HLT_Electron_BJet_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_alphaT_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_MuonFakes_cff import *
from HLTriggerOffline.SUSYBSM.SUSYBSM_ElecFakes_cff import *


HLTSusyExoValSeq = cms.Sequence(SUSY_HLT_HT_MET +
                                SUSY_HLT_InclusiveHT + 
                                SUSY_HLT_InclusiveMET +
                                SUSYHLTMETBTAG +
                                SUSY_HLT_MET_MUON +
                                SUSY_HLT_Mu_HT_SingleLepton +
                                SUSY_HLT_Mu_HT_MET_SingleLepton +
                                SUSYHLTMuHTBTagSingleLepton +
                                SUSYHLTMuHTControlSingleLepton +
                                SUSY_HLT_Ele_HT_SingleLepton +
                                SUSY_HLT_Ele_HT_MET_SingleLepton +
                                SUSYHLTEleHTBTagSingleLepton +
                                SUSYHLTEleHTControlSingleLepton +
                                SUSYHLTMETMUONER +
                                SUSYHLTMETHTMUON +
                                SUSYHLTMETHTMUONER +
                                SUSYHLTMETHTMUONBTAG +
                                cms.ignore(hemispheres)+ #for razor triggers
                                cms.ignore(caloHemispheres)+ #for razor triggers
                                SUSYHLTRazorHbbRsq0p02MR3002CSV0p70p4 +
                                SUSYHLTRazorHbbRsq0p02MR3002CSV0p7 +
                                SUSYHLTRazorHbbRsq0p02MR4002CSV0p7 +
                                SUSYHLTRazorHbbRsq0p02MR4502CSV0p7 +
                                SUSYHLTRazorHbbRsq0p02MR5002CSV0p7 +
                                SUSYHLTRazorHbbRsq0p02MR5502CSV0p7 +
                                SUSYHLTRazorMainRsqMR300 + 
                                SUSYHLTRazorQuadJetRsqMR300 +
                                SUSYHLTRazorDMRsq0p36 + 
                                SUSYHLTRazorMainRsqMR270 + 
                                SUSYHLTRazorQuadJetRsqMR270 +
                                SUSYHLTRazorDMRsq0p30 + 
                                SUSYHLTRazorMainRsqMR260 + 
                                SUSYHLTRazorQuadJetRsqMR260 +
                                SUSYHLTRazorMainRsqMR240 + 
                                SUSYHLTRazorQuadJetRsqMR240 +
                                SUSYHLTRazorDMRsq0p25 + 
                                SUSYHLTRazorMainCaloRsqMR240 +
                                SUSYHLTRazorQuadJetCaloRsqMR240 +
                                SUSYHLTRazorDMCaloRsq0p25 +
                                SUSYHLTCaloHT200 +
                                SUSYHLTCaloHT250 +
                                SUSYHLTCaloHT300 +
                                SUSYHLTCaloHT350 +
                                SUSYHLTCaloHT400 +
                                SUSYHLTPhotonHT +
                                SUSYHLTPhotonMETpt36 +
                                SUSYHLTPhotonMETpt50 +
                                SUSYHLTPhotonMETpt75 +
                                SUSYHLTHTDoubleMuon +
                                SUSYHLTHTDoubleEle +
                                SUSYHLTHTMuEle +
								SUSYHLTHT250DoubleMuon +
                                SUSYHLTHT250DoubleEle +
                                SUSYHLTHT250MuEle +
                                SUSYHLTMuonBJet +
                                SUSYHLTElectronBJet +
                                SUSYHLTHT200alphaT0p51 +
                                SUSYHLTHT200alphaT0p57 +
                                SUSYHLTHT250alphaT0p55 +
                                SUSYHLTHT300alphaT0p53 +
                                SUSYHLTHT350alphaT0p52 +
                                SUSYHLTHT400alphaT0p51 +
                                SUSYHLTHT200alphaT0p63 +
                                SUSYHLTHT250alphaT0p58 +
                                SUSYHLTHT300alphaT0p54 +
                                SUSYHLTHT350alphaT0p53 +
                                SUSYHLTHT400alphaT0p52 +
                                SUSY_HLT_ElecFakes +
                                SUSY_HLT_MuonFakes
                                )
