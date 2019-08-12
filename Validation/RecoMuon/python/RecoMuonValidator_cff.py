import FWCore.ParameterSet.Config as cms

#####################################################################################
# Configurations for RecoMuonValidator
#

from RecoMuon.TrackingTools.MuonServiceProxy_cff import *
from Validation.RecoMuon.RecoMuonValidator_cfi import *
#
##import SimGeneral.MixingModule.mixNoPU_cfi
from SimMuon.MCTruth.muonAssociatorByHitsNoSimHitsHelper_cfi import *
from SimMuon.MCTruth.MuonAssociatorByHits_cfi import muonAssociatorByHitsCommonParameters

#tracker
muonAssociatorByHitsNoSimHitsHelperTrk = muonAssociatorByHitsNoSimHitsHelper.clone()
muonAssociatorByHitsNoSimHitsHelperTrk.UseTracker = True
muonAssociatorByHitsNoSimHitsHelperTrk.UseMuon  = False
recoMuonVMuAssoctrk = recoMuonValidator.clone()
recoMuonVMuAssoctrk.subDir = 'Muons/RecoMuonV/RecoMuon_MuonAssoc_Trk'
recoMuonVMuAssoctrk.muAssocLabel = 'muonAssociatorByHitsNoSimHitsHelperTrk'
recoMuonVMuAssoctrk.trackType = 'inner'
recoMuonVMuAssoctrk.selection = "isTrackerMuon"

#tracker and PF
muonAssociatorByHitsNoSimHitsHelperTrkPF = muonAssociatorByHitsNoSimHitsHelper.clone()
muonAssociatorByHitsNoSimHitsHelperTrkPF.UseTracker = True
muonAssociatorByHitsNoSimHitsHelperTrkPF.UseMuon  = False
recoMuonVMuAssoctrkPF = recoMuonValidator.clone()
recoMuonVMuAssoctrkPF.subDir = 'Muons/RecoMuonV/RecoMuon_MuonAssoc_TrkPF'
recoMuonVMuAssoctrkPF.usePFMuon = True
recoMuonVMuAssoctrkPF.muAssocLabel = 'muonAssociatorByHitsNoSimHitsHelperTrkPF'
recoMuonVMuAssoctrkPF.trackType = 'inner'
recoMuonVMuAssoctrkPF.selection = "isTrackerMuon & isPFMuon"

#standalone
muonAssociatorByHitsNoSimHitsHelperStandalone = muonAssociatorByHitsNoSimHitsHelper.clone()
muonAssociatorByHitsNoSimHitsHelperStandalone.UseTracker = False
muonAssociatorByHitsNoSimHitsHelperStandalone.UseMuon  = True
recoMuonVMuAssocsta = recoMuonValidator.clone()
recoMuonVMuAssocsta.subDir = 'Muons/RecoMuonV/RecoMuon_MuonAssoc_Sta'
recoMuonVMuAssocsta.muAssocLabel = 'muonAssociatorByHitsNoSimHitsHelperStandalone'
recoMuonVMuAssocsta.trackType = 'outer'
recoMuonVMuAssocsta.selection = "isStandAloneMuon"

#seed of StandAlone
muonAssociatorByHitsNoSimHitsHelperSeedStandalone = muonAssociatorByHitsNoSimHitsHelper.clone()
muonAssociatorByHitsNoSimHitsHelperSeedStandalone.UseTracker = False
muonAssociatorByHitsNoSimHitsHelperSeedStandalone.UseMuon  = True
recoMuonVMuAssocseedSta = recoMuonValidator.clone()
recoMuonVMuAssocseedSta.subDir = 'Muons/RecoMuonV/RecoMuon_MuonAssoc_SeedSta'
recoMuonVMuAssocseedSta.muAssocLabel = 'muonAssociatorByHitsNoSimHitsHelperStandalone'
recoMuonVMuAssocseedSta.trackType = 'outer'
recoMuonVMuAssocseedSta.selection = ""

#standalone and PF
muonAssociatorByHitsNoSimHitsHelperStandalonePF = muonAssociatorByHitsNoSimHitsHelper.clone()
muonAssociatorByHitsNoSimHitsHelperStandalonePF.UseTracker = False
muonAssociatorByHitsNoSimHitsHelperStandalonePF.UseMuon  = True
recoMuonVMuAssocstaPF = recoMuonValidator.clone()
recoMuonVMuAssocstaPF.subDir = 'Muons/RecoMuonV/RecoMuon_MuonAssoc_StaPF'
recoMuonVMuAssocstaPF.usePFMuon = True
recoMuonVMuAssocstaPF.muAssocLabel = 'muonAssociatorByHitsNoSimHitsHelperStandalonePF'
recoMuonVMuAssocstaPF.trackType = 'outer'
recoMuonVMuAssocstaPF.selection = "isStandAloneMuon & isPFMuon"

#global
muonAssociatorByHitsNoSimHitsHelperGlobal = muonAssociatorByHitsNoSimHitsHelper.clone()
muonAssociatorByHitsNoSimHitsHelperGlobal.UseTracker = True
muonAssociatorByHitsNoSimHitsHelperGlobal.UseMuon  = True
recoMuonVMuAssocglb = recoMuonValidator.clone()
recoMuonVMuAssocglb.subDir = 'Muons/RecoMuonV/RecoMuon_MuonAssoc_Glb'
recoMuonVMuAssocglb.muAssocLabel = 'muonAssociatorByHitsNoSimHitsHelperGlobal'
recoMuonVMuAssocglb.trackType = 'global'
recoMuonVMuAssocglb.selection = "isGlobalMuon"

#global and PF
muonAssociatorByHitsNoSimHitsHelperGlobalPF = muonAssociatorByHitsNoSimHitsHelper.clone()
muonAssociatorByHitsNoSimHitsHelperGlobalPF.UseTracker = True
muonAssociatorByHitsNoSimHitsHelperGlobalPF.UseMuon  = True
recoMuonVMuAssocglbPF = recoMuonValidator.clone()
recoMuonVMuAssocglbPF.subDir = 'Muons/RecoMuonV/RecoMuon_MuonAssoc_GlbPF'
recoMuonVMuAssocglbPF.usePFMuon = True
recoMuonVMuAssocglbPF.muAssocLabel = 'muonAssociatorByHitsNoSimHitsHelperGlobalPF'
recoMuonVMuAssocglbPF.trackType = 'global'
recoMuonVMuAssocglbPF.selection = "isGlobalMuon & isPFMuon"

#tight
muonAssociatorByHitsNoSimHitsHelperTight = muonAssociatorByHitsNoSimHitsHelper.clone()
muonAssociatorByHitsNoSimHitsHelperTight.UseTracker = True
muonAssociatorByHitsNoSimHitsHelperTight.UseMuon  = True
recoMuonVMuAssoctgt = recoMuonValidator.clone()
recoMuonVMuAssoctgt.subDir = 'Muons/RecoMuonV/RecoMuon_MuonAssoc_Tgt'
recoMuonVMuAssoctgt.muAssocLabel = 'muonAssociatorByHitsNoSimHitsHelperTight'
recoMuonVMuAssoctgt.trackType = 'global'
recoMuonVMuAssoctgt.selection = 'isGlobalMuon'
recoMuonVMuAssoctgt.wantTightMuon = True
recoMuonVMuAssoctgt.beamSpot = 'offlineBeamSpot'
recoMuonVMuAssoctgt.primaryVertex = 'offlinePrimaryVertices'

##########################################################################
# Muon validation sequence using RecoMuonValidator
#

muonValidationRMV_seq = cms.Sequence(
    muonAssociatorByHitsNoSimHitsHelperTrk +recoMuonVMuAssoctrk
    +muonAssociatorByHitsNoSimHitsHelperStandalone +recoMuonVMuAssocsta
    +muonAssociatorByHitsNoSimHitsHelperGlobal +recoMuonVMuAssocglb
    +muonAssociatorByHitsNoSimHitsHelperTight +recoMuonVMuAssoctgt
    # 
    #    +muonAssociatorByHitsNoSimHitsHelperTrkPF +recoMuonVMuAssoctrkPF
    #    +muonAssociatorByHitsNoSimHitsHelperStandalonePF +recoMuonVMuAssocstaPF
    #    +muonAssociatorByHitsNoSimHitsHelperGlobalPF +recoMuonVMuAssocglbPF
    )

