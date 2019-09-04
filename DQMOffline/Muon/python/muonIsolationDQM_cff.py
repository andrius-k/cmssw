import FWCore.ParameterSet.Config as cms

from RecoMuon.MuonIsolationProducers.muIsoDeposits_cff import *
from TrackPropagation.SteppingHelixPropagator.SteppingHelixPropagatorAny_cfi import *

from DQMServices.Core.DQMEDAnalyzer import DQMEDAnalyzer
MuIsoDQMtrk = DQMEDAnalyzer('MuonIsolationDQM',
                              Global_Muon_Label = cms.untracked.InputTag("muons"),
                              requireTRKMuon = cms.untracked.bool(True),
                              requireSTAMuon = cms.untracked.bool(False),
                              requireGLBMuon = cms.untracked.bool(False),                        
                              ecalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ecal"),
                              #    rootfilename = cms.untracked.string('ttbar-DQMidation.root'),
                              hcalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","hcal"),
                              tkIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositTk"),
                              hoIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ho"),
                              vertexLabel = cms.untracked.InputTag("offlinePrimaryVertices"),
                              directory = cms.string("Muons/Isolation/tracker")                             
                              )

MuIsoDQMsta = DQMEDAnalyzer('MuonIsolationDQM',
                              Global_Muon_Label = cms.untracked.InputTag("muons"),
                              requireTRKMuon = cms.untracked.bool(False),
                              requireSTAMuon = cms.untracked.bool(True),
                              requireGLBMuon = cms.untracked.bool(False),
                              ecalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ecal"),
                              #    rootfilename = cms.untracked.string('ttbar-DQMidation.root'),
                              hcalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","hcal"),
                              tkIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositTk"),
                              hoIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ho"),
                              vertexLabel = cms.untracked.InputTag("offlinePrimaryVertices"),
                              directory = cms.string("Muons/Isolation/standalone")
                              )

MuIsoDQMglb = DQMEDAnalyzer('MuonIsolationDQM',
                              Global_Muon_Label = cms.untracked.InputTag("muons"),
                              requireTRKMuon = cms.untracked.bool(False),
                              requireSTAMuon = cms.untracked.bool(False),
                              requireGLBMuon = cms.untracked.bool(True),
                              ecalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ecal"),
                              #    rootfilename = cms.untracked.string('ttbar-DQMidation.root'),
                              hcalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","hcal"),
                              tkIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositTk"),
                              hoIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ho"),
                              vertexLabel = cms.untracked.InputTag("offlinePrimaryVertices"),
                              directory = cms.string("Muons/Isolation/global")
                              )
muIsoDQM_seq = cms.Sequence(MuIsoDQMtrk+MuIsoDQMsta+MuIsoDQMglb)



MuIsoDQMtrkminiAOD = DQMEDAnalyzer('MuonIsolationDQM',
                                      Global_Muon_Label = cms.untracked.InputTag("slimmedMuons"),
                                      requireTRKMuon = cms.untracked.bool(True),
                                      requireSTAMuon = cms.untracked.bool(False),
                                      requireGLBMuon = cms.untracked.bool(False),                        
                                      ecalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ecal"),
                                      #    rootfilename = cms.untracked.string('ttbar-DQMidation.root'),
                                      hcalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","hcal"),
                                      tkIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositTk"),
                                      hoIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ho"),
                                      vertexLabel = cms.untracked.InputTag("offlinePrimaryVertices"),
                                      directory = cms.string("Muons_miniAOD/Isolation/tracker")                             
                                      )

MuIsoDQMstaminiAOD = DQMEDAnalyzer('MuonIsolationDQM',
                                      Global_Muon_Label = cms.untracked.InputTag("slimmedMuons"),
                                      requireTRKMuon = cms.untracked.bool(False),
                                      requireSTAMuon = cms.untracked.bool(True),
                                      requireGLBMuon = cms.untracked.bool(False),
                                      ecalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ecal"),
                                      #    rootfilename = cms.untracked.string('ttbar-DQMidation.root'),
                                      hcalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","hcal"),
                                      tkIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositTk"),
                                      hoIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ho"),
                                      vertexLabel = cms.untracked.InputTag("offlinePrimaryVertices"),
                                      directory = cms.string("Muons_miniAOD/Isolation/standalone")
                                      )

MuIsoDQMglbminiAOD = DQMEDAnalyzer('MuonIsolationDQM',
                                      Global_Muon_Label = cms.untracked.InputTag("slimmedMuons"),
                                      requireTRKMuon = cms.untracked.bool(False),
                                      requireSTAMuon = cms.untracked.bool(False),
                                      requireGLBMuon = cms.untracked.bool(True),
                                      ecalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ecal"),
                                      #    rootfilename = cms.untracked.string('ttbar-DQMidation.root'),
                                      hcalIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","hcal"),
                                      tkIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositTk"),
                                      hoIsoDeposit_Label = cms.untracked.InputTag("muIsoDepositCalByAssociatorTowers","ho"),
                                      vertexLabel = cms.untracked.InputTag("offlinePrimaryVertices"),
                                      directory = cms.string("Muons_miniAOD/Isolation/global")
                                      )
muIsoDQM_seq_miniAOD = cms.Sequence(MuIsoDQMtrkminiAOD+MuIsoDQMstaminiAOD+MuIsoDQMglbminiAOD)
