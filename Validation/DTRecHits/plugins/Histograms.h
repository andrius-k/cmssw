#ifndef Validation_DTRecHits_Histograms_h
#define Validation_DTRecHits_Histograms_h

/** \class Histograms
 *  Collection of histograms for DT RecHit and Segment test.
 *
 *  \author S. Bolognesi and G. Cerminara - INFN Torino
 */

#include <cmath>
#include <iostream>
#include <string>

#include <TH1F.h>

#include "DQMServices/Core/interface/DQMStore.h"

//---------------------------------------------------------------------------------------
/// Function to fill an efficiency histograms with binomial errors
inline void divide(dqm::legacy::MonitorElement* eff,
                   const dqm::legacy::MonitorElement* numerator,
                   const dqm::legacy::MonitorElement* denominator) {
  TH1* effH = eff->getTH1();
  TH1* numH = numerator->getTH1();
  TH1* denH = denominator->getTH1();
  effH->Divide(numH, denH);

  // Set the error accordingly to binomial statistics
  int bins = effH->GetNbinsX();
  for (int bin = 1; bin <= bins; ++bin) {
    float den = denH->GetBinContent(bin);
    float eff = effH->GetBinContent(bin);
    float err = 0;
    if (den != 0) {
      err = sqrt(eff * (1 - eff) / den);
    }
    effH->SetBinError(bin, err);
  }
  return;
}

//---------------------------------------------------------------------------------------
/// A set of histograms of residuals and pulls for 1D RecHits
class HRes1DHit {
public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  HRes1DHit(const std::string& name, DQMStore::IBooker& booker, bool doall = true, bool local = true) {
    std::string pre = "1D_";
    pre += name;
    doall_ = doall;
    booker.setCurrentFolder("DT/1DRecHits/Res/");
    if (doall) {
      hDist = booker.book1D(pre + "_hDist", "1D RHit distance from wire", 100, 0, 2.5);
      hResVsAngle =
          booker.book2D(pre + "_hResVsAngle", "1D RHit residual vs impact angle", 100, -1.2, 1.2, 100, -0.2, 0.2);
      hResVsDistFE =
          booker.book2D(pre + "_hResVsDistFE", "1D RHit residual vs FE distance", 100, 0., 400., 150, -0.5, 0.5);
      booker.setCurrentFolder("DT/1DRecHits/Pull/");
      hPullVsPos = booker.book2D(pre + "_hPullVsPos", "1D RHit pull vs position", 100, 0, 2.5, 100, -5, 5);
      hPullVsAngle = booker.book2D(pre + "_hPullVsAngle", "1D RHit pull vs impact angle", 100, -1.2, 1.2, 100, -5, 5);
      hPullVsDistFE = booker.book2D(pre + "_hPullVsDistFE", "1D RHit pull vs FE distance", 100, 0., 400., 100, -5, 5);
    }
    booker.setCurrentFolder("DT/1DRecHits/Res/");
    hRes = booker.book1D(pre + "_hRes", "1D RHit residual", 300, -0.5, 0.5);
    hResSt[0] = booker.book1D(pre + "_hResMB1", "1D RHit residual", 300, -0.5, 0.5);
    hResSt[1] = booker.book1D(pre + "_hResMB2", "1D RHit residual", 300, -0.5, 0.5);
    hResSt[2] = booker.book1D(pre + "_hResMB3", "1D RHit residual", 300, -0.5, 0.5);
    hResSt[3] = booker.book1D(pre + "_hResMB4", "1D RHit residual", 300, -0.5, 0.5);
    hResVsEta = booker.book2D(pre + "_hResVsEta", "1D RHit residual vs eta", 50, -1.25, 1.25, 150, -0.5, 0.5);
    hResVsPhi = booker.book2D(pre + "_hResVsPhi", "1D RHit residual vs phi", 100, -3.2, 3.2, 150, -0.5, 0.5);
    hResVsPos = booker.book2D(pre + "_hResVsPos", "1D RHit residual vs position", 100, 0, 2.5, 150, -0.5, 0.5);

    booker.setCurrentFolder("DT/1DRecHits/Pull/");
    hPull = booker.book1D(pre + "_hPull", "1D RHit pull", 100, -5, 5);
    hPullSt[0] = booker.book1D(pre + "_hPullMB1", "1D RHit residual", 100, -5, 5);
    hPullSt[1] = booker.book1D(pre + "_hPullMB2", "1D RHit residual", 100, -5, 5);
    hPullSt[2] = booker.book1D(pre + "_hPullMB3", "1D RHit residual", 100, -5, 5);
    hPullSt[3] = booker.book1D(pre + "_hPullMB4", "1D RHit residual", 100, -5, 5);
  }

  void fill(float distSimHit,
            float thetaSimHit,
            float distFESimHit,
            float distRecHit,
            float etaSimHit,
            float phiSimHit,
            float errRecHit,
            int station) {
    // Reso, pull
    float res = distRecHit - distSimHit;
    if (doall_) {
      hDist->Fill(distRecHit);
      hResVsAngle->Fill(thetaSimHit, res);
      hResVsDistFE->Fill(distFESimHit, res);
    }
    hRes->Fill(res);
    hResSt[station - 1]->Fill(res);
    hResVsEta->Fill(etaSimHit, res);
    hResVsPhi->Fill(phiSimHit, res);
    hResVsPos->Fill(distSimHit, res);
    if (errRecHit != 0) {
      float pull = res / errRecHit;
      hPull->Fill(pull);
      hPullSt[station - 1]->Fill(pull);
      if (doall_) {
        hPullVsPos->Fill(distSimHit, pull);
        hPullVsAngle->Fill(thetaSimHit, pull);
        hPullVsDistFE->Fill(distFESimHit, pull);
      }
    } else {
      std::cout << "Error: RecHit error = 0" << std::endl;
    }
  }

private:
  dqm::reco::MonitorElement const* hDist;
  dqm::reco::MonitorElement const* hRes;
  dqm::reco::MonitorElement const* hResSt[4];
  dqm::reco::MonitorElement const* hResVsEta;
  dqm::reco::MonitorElement const* hResVsPhi;
  dqm::reco::MonitorElement const* hResVsPos;
  dqm::reco::MonitorElement const* hResVsAngle;
  dqm::reco::MonitorElement const* hResVsDistFE;

  dqm::reco::MonitorElement const* hPull;
  dqm::reco::MonitorElement const* hPullSt[4];
  dqm::reco::MonitorElement const* hPullVsPos;
  dqm::reco::MonitorElement const* hPullVsAngle;
  dqm::reco::MonitorElement const* hPullVsDistFE;
  bool doall_;
  std::string name_;
};

//---------------------------------------------------------------------------------------
/// A set of histograms fo efficiency computation for 1D RecHits (producer)
class HEff1DHit {
public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  HEff1DHit(const std::string& name, DQMStore::IBooker& booker) {
    std::string pre = "1D_";
    pre += name;
    name_ = pre;
    booker.setCurrentFolder("DT/1DRecHits/");
    hEtaMuSimHit = booker.book1D(pre + "_hEtaMuSimHit", "SimHit Eta distribution", 100, -1.5, 1.5);
    hEtaRecHit = booker.book1D(pre + "_hEtaRecHit", "SimHit Eta distribution with 1D RecHit", 100, -1.5, 1.5);
    hPhiMuSimHit = booker.book1D(pre + "_hPhiMuSimHit", "SimHit Phi distribution", 100, -M_PI, M_PI);
    hPhiRecHit = booker.book1D(pre + "_hPhiRecHit", "SimHit Phi distribution with 1D RecHit", 100, -M_PI, M_PI);
    hDistMuSimHit = booker.book1D(pre + "_hDistMuSimHit", "SimHit Distance from wire distribution", 100, 0, 2.5);
    hDistRecHit =
        booker.book1D(pre + "_hDistRecHit", "SimHit Distance from wire distribution with 1D RecHit", 100, 0, 2.5);
  }

  void fill(float distSimHit, float etaSimHit, float phiSimHit, bool fillRecHit) {
    hEtaMuSimHit->Fill(etaSimHit);
    hPhiMuSimHit->Fill(phiSimHit);
    hDistMuSimHit->Fill(distSimHit);
    if (fillRecHit) {
      hEtaRecHit->Fill(etaSimHit);
      hPhiRecHit->Fill(phiSimHit);
      hDistRecHit->Fill(distSimHit);
    }
  }

private:
  dqm::reco::MonitorElement const* hEtaMuSimHit;
  dqm::reco::MonitorElement const* hEtaRecHit;

  dqm::reco::MonitorElement const* hPhiMuSimHit;
  dqm::reco::MonitorElement const* hPhiRecHit;

  dqm::reco::MonitorElement const* hDistMuSimHit;
  dqm::reco::MonitorElement const* hDistRecHit;

  std::string name_;
};

//---------------------------------------------------------------------------------------
/// A set of histograms fo efficiency computation for 1D RecHits (harvesting)
class HEff1DHitHarvest {
public:
  typedef dqm::legacy::DQMStore DQMStore;
  typedef dqm::legacy::MonitorElement MonitorElement;

  HEff1DHitHarvest(const std::string& name, DQMStore::IBooker& booker, DQMStore::IGetter& getter) {
    std::string pre = "1D_";
    pre += name;
    name_ = pre;
    booker.setCurrentFolder("DT/1DRecHits/");
    hEffVsEta = booker.book1D(pre + "_hEffVsEta", "1D RecHit Efficiency as a function of Eta", 100, -1.5, 1.5);
    hEffVsPhi = booker.book1D(pre + "_hEffVsPhi", "1D RecHit Efficiency as a function of Phi", 100, -M_PI, M_PI);
    hEffVsDist = booker.book1D(pre + "_hEffVsDist", "1D RecHit Efficiency as a function of Dist", 100, 0, 2.5);

    computeEfficiency(getter);
  }

  void computeEfficiency(DQMStore::IGetter& getter) {
    std::string pre = "DT/1DRecHits/" + name_;
    divide(hEffVsEta, getter.get(pre + "_hEtaMuRecHit"), getter.get(pre + "_hEtaMuSimHit"));
    divide(hEffVsPhi, getter.get(pre + "_hPhiMuRecHit"), getter.get(pre + "_hPhiMuSimHit"));
    divide(hEffVsDist, getter.get(pre + "_hDistMuRecHit"), getter.get(pre + "_hDistMuSimHit"));
  }

private:
  MonitorElement* hEffVsEta;
  MonitorElement* hEffVsPhi;
  MonitorElement* hEffVsDist;

  std::string name_;
};

//---------------------------------------------------------------------------------------
// Histos of residuals for 2D rechits
class HRes2DHit {
public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  HRes2DHit(const std::string& name, DQMStore::IBooker& booker, bool doall = true, bool local = true) {
    doall_ = doall;
    std::string pre = "2D_";
    pre += name;
    booker.setCurrentFolder("DT/2DSegments/Res/");
    if (doall) {
      hRecAngle = booker.book1D(pre + "_hRecAngle", "Distribution of Rec segment angles;angle (rad)", 100, -1.5, 1.5);
      hSimAngle =
          booker.book1D(pre + "_hSimAngle", "Distribution of segment angles from SimHits;angle (rad)", 100, -1.5, 1.5);
      hRecVsSimAngle =
          booker.book2D(pre + "_hRecVsSimAngle", "Rec angle vs sim angle;angle (rad)", 100, -1.5, 1.5, 100, -1.5, 1.5);
      hResAngleVsEta = booker.book2D(pre + "_hResAngleVsEta",
                                     "Residual on 2D segment angle vs Eta; #eta; res (rad)",
                                     100,
                                     -2.5,
                                     2.5,
                                     200,
                                     -0.2,
                                     0.2);
      hResAngleVsPhi = booker.book2D(pre + "_hResAngleVsPhi",
                                     "Residual on 2D segment angle vs Phi; #phi (rad);res (rad)",
                                     100,
                                     -3.2,
                                     3.2,
                                     150,
                                     -0.2,
                                     0.2);
      hResPosVsEta = booker.book2D(
          pre + "_hResPosVsEta", "Residual on 2D segment position vs Eta;#eta;res (cm)", 100, -2.5, 2.5, 150, -0.2, 0.2);
      hResPosVsPhi = booker.book2D(pre + "_hResPosVsPhi",
                                   "Residual on 2D segment position vs Phi;#phi (rad);res (cm)",
                                   100,
                                   -3.2,
                                   3.2,
                                   150,
                                   -0.2,
                                   0.2);
      hResPosVsResAngle = booker.book2D(pre + "_hResPosVsResAngle",
                                        "Residual on 2D segment position vs Residual on 2D "
                                        "segment angle;angle (rad);res (cm)",
                                        100,
                                        -0.3,
                                        0.3,
                                        150,
                                        -0.2,
                                        0.2);
    }
    hResAngle = booker.book1D(
        pre + "_hResAngle", "Residual on 2D segment angle;angle_{rec}-angle_{sim} (rad)", 50, -0.01, 0.01);
    hResPos = booker.book1D(
        pre + "_hResPos", "Residual on 2D segment position (x at SL center);x_{rec}-x_{sim} (cm)", 150, -0.2, 0.2);

    booker.setCurrentFolder("DT/2DSegments/Pull/");
    hPullAngle = booker.book1D(
        pre + "_hPullAngle", "Pull on 2D segment angle;(angle_{rec}-angle_{sim})/#sigma (rad)", 150, -5, 5);
    hPullPos = booker.book1D(pre + "_hPullPos",
                             "Pull on 2D segment position (x at SL "
                             "center);(x_{rec}-x_{sim} (cm))/#sigma",
                             150,
                             -5,
                             5);
  }

  void fill(float angleSimSegment,
            float angleRecSegment,
            float posSimSegment,
            float posRecSegment,
            float etaSimSegment,
            float phiSimSegment,
            float sigmaPos,
            float sigmaAngle) {
    float resAngle = angleRecSegment - angleSimSegment;
    hResAngle->Fill(resAngle);
    float resPos = posRecSegment - posSimSegment;
    hResPos->Fill(resPos);
    hPullAngle->Fill(resAngle / sigmaAngle);
    hPullPos->Fill(resPos / sigmaPos);
    if (doall_) {
      hRecAngle->Fill(angleRecSegment);
      hSimAngle->Fill(angleSimSegment);
      hRecVsSimAngle->Fill(angleSimSegment, angleRecSegment);
      hResAngleVsEta->Fill(etaSimSegment, resAngle);
      hResAngleVsPhi->Fill(phiSimSegment, resAngle);
      hResPosVsEta->Fill(etaSimSegment, resPos);
      hResPosVsPhi->Fill(phiSimSegment, resPos);
      hResPosVsResAngle->Fill(resAngle, resPos);
    }
  }

private:
  dqm::reco::MonitorElement const* hRecAngle;
  dqm::reco::MonitorElement const* hSimAngle;
  dqm::reco::MonitorElement const* hRecVsSimAngle;
  dqm::reco::MonitorElement const* hResAngle;
  dqm::reco::MonitorElement const* hResAngleVsEta;
  dqm::reco::MonitorElement const* hResAngleVsPhi;
  dqm::reco::MonitorElement const* hResPos;
  dqm::reco::MonitorElement const* hResPosVsEta;
  dqm::reco::MonitorElement const* hResPosVsPhi;
  dqm::reco::MonitorElement const* hResPosVsResAngle;
  dqm::reco::MonitorElement const* hPullAngle;
  dqm::reco::MonitorElement const* hPullPos;

  std::string name_;
  bool doall_;
};

//---------------------------------------------------------------------------------------
// Histos for 2D RecHit efficiency (producer)
class HEff2DHit {
public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  HEff2DHit(const std::string& name, DQMStore::IBooker& booker) {
    std::string pre = "2D_";
    pre += name;
    name_ = pre;
    booker.setCurrentFolder("DT/2DSegments/");
    hEtaSimSegm = booker.book1D(pre + "_hEtaSimSegm", "Eta of SimHit segment", 100, -1.5, 1.5);
    hEtaRecHit =
        booker.book1D(pre + "_hEtaRecHit", "Eta distribution of SimHit segment with 2D RecHit", 100, -1.5, 1.5);
    hPhiSimSegm = booker.book1D(pre + "_hPhiSimSegm", "Phi of SimHit segment", 100, -M_PI, M_PI);
    hPhiRecHit =
        booker.book1D(pre + "_hPhiRecHit", "Phi distribution of SimHit segment with 2D RecHit", 100, -M_PI, M_PI);
    hPosSimSegm = booker.book1D(pre + "_hPosSimSegm", "Position in SL of SimHit segment (cm)", 100, -250, 250);
    hPosRecHit =
        booker.book1D(pre + "_hPosRecHit", "Position in SL of SimHit segment with 2D RecHit (cm)", 100, -250, 250);
    hAngleSimSegm = booker.book1D(pre + "_hAngleSimSegm", "Angle of SimHit segment (rad)", 100, -2, 2);
    hAngleRecHit = booker.book1D(pre + "_hAngleRecHit", "Angle of SimHit segment with 2D RecHit (rad)", 100, -2, 2);
  }

  void fill(float etaSimSegm, float phiSimSegm, float posSimSegm, float angleSimSegm, bool fillRecHit) {
    hEtaSimSegm->Fill(etaSimSegm);
    hPhiSimSegm->Fill(phiSimSegm);
    hPosSimSegm->Fill(posSimSegm);
    hAngleSimSegm->Fill(angleSimSegm);

    if (fillRecHit) {
      hEtaRecHit->Fill(etaSimSegm);
      hPhiRecHit->Fill(phiSimSegm);
      hPosRecHit->Fill(posSimSegm);
      hAngleRecHit->Fill(angleSimSegm);
    }
  }

private:
  dqm::reco::MonitorElement const* hEtaSimSegm;
  dqm::reco::MonitorElement const* hEtaRecHit;
  dqm::reco::MonitorElement const* hPhiSimSegm;
  dqm::reco::MonitorElement const* hPhiRecHit;
  dqm::reco::MonitorElement const* hPosSimSegm;
  dqm::reco::MonitorElement const* hPosRecHit;
  dqm::reco::MonitorElement const* hAngleSimSegm;
  dqm::reco::MonitorElement const* hAngleRecHit;

  std::string name_;
};

//---------------------------------------------------------------------------------------
// Histos for 2D RecHit efficiency (harvesting)
class HEff2DHitHarvest {
public:
  typedef dqm::legacy::DQMStore DQMStore;
  typedef dqm::legacy::MonitorElement MonitorElement;

  HEff2DHitHarvest(const std::string& name, DQMStore::IBooker& booker, DQMStore::IGetter& getter) {
    std::string pre = "2D_";
    pre += name;
    name_ = pre;
    booker.setCurrentFolder("DT/2DSegments/");
    hEffVsEta = booker.book1D(pre + "_hEffVsEta", "2D RecHit Efficiency as a function of Eta", 100, -1.5, 1.5);
    hEffVsPhi = booker.book1D(pre + "_hEffVsPhi", "2D RecHit Efficiency as a function of Phi", 100, -M_PI, M_PI);
    hEffVsPos =
        booker.book1D(pre + "_hEffVsPos", "2D RecHit Efficiency as a function of position in SL", 100, -250, 250);
    hEffVsAngle = booker.book1D(pre + "_hEffVsAngle", "2D RecHit Efficiency as a function of angle", 100, -2, 2);

    computeEfficiency(getter);
  }

  void computeEfficiency(DQMStore::IGetter& getter) {
    std::string pre = "DT/2DSegments/" + name_;
    divide(hEffVsEta, getter.get(pre + "_hEtaRecHit"), getter.get(pre + "_hEtaSimSegm"));
    divide(hEffVsPhi, getter.get(pre + "_hPhiRecHit"), getter.get(pre + "_hPhiSimSegm"));
    divide(hEffVsPos, getter.get(pre + "_hPosRecHit"), getter.get(pre + "_hPosSimSegm"));
    divide(hEffVsAngle, getter.get(pre + "_hAngleRecHit"), getter.get(pre + "_hAngleSimSegm"));
  }

private:
  MonitorElement* hEffVsEta;
  MonitorElement* hEffVsPhi;
  MonitorElement* hEffVsPos;
  MonitorElement* hEffVsAngle;

  std::string name_;
};

//---------------------------------------------------------------------------------------
// Histos of residuals for 4D rechits
class HRes4DHit {
public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  HRes4DHit(const std::string& name, DQMStore::IBooker& booker, bool doall = true, bool local = true) : local_(local) {
    std::string pre = "4D_";
    pre += name;
    doall_ = doall;

    booker.setCurrentFolder("DT/4DSegments/Res/");
    if (doall) {
      hRecAlpha =
          booker.book1D(pre + "_hRecAlpha", "4D RecHit alpha (RPhi) distribution;#alpha^{x} (rad)", 100, -1.5, 1.5);
      hRecBeta = booker.book1D(pre + "_hRecBeta", "4D RecHit beta distribution:#alpha^{y} (rad)", 100, -1.5, 1.5);

      hSimAlpha = booker.book1D(
          pre + "_hSimAlpha", "4D segment from SimHit alpha (RPhi) distribution;i#alpha^{x} (rad)", 100, -1.5, 1.5);
      hSimBeta =
          booker.book1D(pre + "_hSimBeta", "4D segment from SimHit beta distribution;#alpha^{y} (rad)", 100, -1.5, 1.5);
      hRecVsSimAlpha = booker.book2D(pre + "_hRecVsSimAlpha",
                                     "4D segment rec alpha {v}s sim alpha (RPhi);#alpha^{x} (rad)",
                                     100,
                                     -1.5,
                                     1.5,
                                     100,
                                     -1.5,
                                     1.5);
      hRecVsSimBeta = booker.book2D(pre + "_hRecVsSimBeta",
                                    "4D segment rec beta vs sim beta (RZ);#alpha^{y} (rad)",
                                    100,
                                    -1.5,
                                    1.5,
                                    100,
                                    -1.5,
                                    1.5);

      hResAlphaVsEta = booker.book2D(pre + "_hResAlphaVsEta",
                                     "4D RecHit residual on #alpha_x direction vs "
                                     "eta;#eta;#alpha^{x}_{rec}-#alpha^{x}_{sim} (rad)",
                                     100,
                                     -2.5,
                                     2.5,
                                     100,
                                     -0.025,
                                     0.025);
      hResAlphaVsPhi = booker.book2D(pre + "_hResAlphaVsPhi",
                                     "4D RecHit residual on #alpha_x direction vs phi (rad);#phi "
                                     "(rad);#alpha^{x}_{rec}-#alpha^{x}_{sim} (rad)",
                                     100,
                                     -3.2,
                                     3.2,
                                     100,
                                     -0.025,
                                     0.025);
      hResBetaVsEta = booker.book2D(pre + "_hResBetaVsEta",
                                    "4D RecHit residual on beta direction vs "
                                    "eta;#eta;#alpha^{y}_{rec}-#alpha^{y}_{sim} (rad)",
                                    100,
                                    -2.5,
                                    2.5,
                                    200,
                                    -0.2,
                                    0.2);
      hResBetaVsPhi = booker.book2D(pre + "_hResBetaVsPhi",
                                    "4D RecHit residual on beta direction vs phi;#phi "
                                    "(rad);#alpha^{y}_{rec}-#alpha^{y}_{sim} (rad)",
                                    100,
                                    -3.2,
                                    3.2,
                                    200,
                                    -0.2,
                                    0.2);

      hResXVsEta = booker.book2D(pre + "_hResXVsEta",
                                 "4D RecHit residual on position (x) in "
                                 "chamber vs eta;#eta;x_{rec}-x_{sim} (cm)",
                                 100,
                                 -2.5,
                                 2.5,
                                 150,
                                 -0.3,
                                 0.3);
      hResXVsPhi = booker.book2D(pre + "_hResXVsPhi",
                                 "4D RecHit residual on position (x) in chamber vs "
                                 "phi;#phi (rad);x_{rec}-x_{sim} (cm)",
                                 100,
                                 -3.2,
                                 3.2,
                                 150,
                                 -0.3,
                                 0.3);

      hResYVsEta = booker.book2D(pre + "_hResYVsEta",
                                 "4D RecHit residual on position (y) in "
                                 "chamber vs eta;#eta;y_{rec}-y_{sim} (cm)",
                                 100,
                                 -2.5,
                                 2.5,
                                 150,
                                 -0.6,
                                 0.6);
      hResYVsPhi = booker.book2D(pre + "_hResYVsPhi",
                                 "4D RecHit residual on position (y) in chamber vs "
                                 "phi;#phi (rad);y_{rec}-y_{sim} (cm)",
                                 100,
                                 -3.2,
                                 3.2,
                                 150,
                                 -0.6,
                                 0.6);

      hResAlphaVsResBeta = booker.book2D(pre + "_hResAlphaVsResBeta",
                                         "4D RecHit residual on alpha vs residual on beta",
                                         200,
                                         -0.3,
                                         0.3,
                                         500,
                                         -0.15,
                                         0.15);
      hResXVsResY = booker.book2D(
          pre + "_hResXVsResY", "4D RecHit residual on X vs residual on Y", 150, -0.6, 0.6, 50, -0.3, 0.3);
      hResAlphaVsResX = booker.book2D(
          pre + "_hResAlphaVsResX", "4D RecHit residual on alpha vs residual on x", 150, -0.3, 0.3, 500, -0.15, 0.15);

      hResAlphaVsResY = booker.book2D(
          pre + "_hResAlphaVsResY", "4D RecHit residual on alpha vs residual on y", 150, -0.6, 0.6, 500, -0.15, 0.15);

      hRecBetaRZ = booker.book1D(pre + "_hRecBetaRZ", "4D RecHit beta distribution:#alpha^{y} (rad)", 100, -1.5, 1.5);

      hSimBetaRZ = booker.book1D(
          pre + "_hSimBetaRZ", "4D segment from SimHit beta distribution in RZ SL;#alpha^{y} (rad)", 100, -1.5, 1.5);
      hRecVsSimBetaRZ = booker.book2D(pre + "_hRecVsSimBetaRZ",
                                      "4D segment rec beta vs sim beta (RZ) in RZ SL;#alpha^{y} (rad)",
                                      100,
                                      -1.5,
                                      1.5,
                                      100,
                                      -1.5,
                                      1.5);

      hResBetaVsEtaRZ = booker.book2D(pre + "_hResBetaVsEtaRZ",
                                      "4D RecHit residual on beta direction vs eta;#eta in "
                                      "RZ SL;#alpha^{y}_{rec}-#alpha^{y}_{sim} (rad)",
                                      100,
                                      -2.5,
                                      2.5,
                                      200,
                                      -0.2,
                                      0.2);
      hResBetaVsPhiRZ = booker.book2D(pre + "_hResBetaVsPhiRZ",
                                      "4D RecHit residual on beta direction vs phi in RZ "
                                      "SL;#phi (rad);#alpha^{y}_{rec}-#alpha^{y}_{sim} (rad)",
                                      100,
                                      -3.2,
                                      3.2,
                                      200,
                                      -0.2,
                                      0.2);
      hResYVsEtaRZ = booker.book2D(pre + "_hResYVsEtaRZ",
                                   "4D RecHit residual on position (y) in chamber vs eta "
                                   "in RZ SL;#eta;y_{rec}-y_{sim} (cm)",
                                   100,
                                   -2.5,
                                   2.5,
                                   150,
                                   -0.6,
                                   0.6);
      hResYVsPhiRZ = booker.book2D(pre + "_hResYVsPhiRZ",
                                   "4D RecHit residual on position (y) in chamber vs phi "
                                   "in RZ SL;#phi (rad);y_{rec}-y_{sim} (cm)",
                                   100,
                                   -3.2,
                                   3.2,
                                   150,
                                   -0.6,
                                   0.6);

      booker.setCurrentFolder("DT/4DSegments/Pull/");
      hPullAlphaVsEta = booker.book2D(pre + "_hPullAlphaVsEta",
                                      "4D RecHit pull on #alpha_x direction vs "
                                      "eta;#eta;(#alpha^{x}_{rec}-#alpha^{x}_{sim})/#sigma",
                                      100,
                                      -2.5,
                                      2.5,
                                      100,
                                      -5,
                                      5);
      hPullAlphaVsPhi = booker.book2D(pre + "_hPullAlphaVsPhi",
                                      "4D RecHit pull on #alpha_x direction vs phi (rad);#phi "
                                      "(rad);(#alpha^{x}_{rec}-#alpha^{x}_{sim})/#sigma",
                                      100,
                                      -3.2,
                                      3.2,
                                      100,
                                      -5,
                                      5);
      hPullBetaVsEta = booker.book2D(pre + "_hPullBetaVsEta",
                                     "4D RecHit pull on beta direction vs "
                                     "eta;#eta;(#alpha^{y}_{rec}-#alpha^{y}_{sim})/#sigma",
                                     100,
                                     -2.5,
                                     2.5,
                                     200,
                                     -5,
                                     5);
      hPullBetaVsPhi = booker.book2D(pre + "_hPullBetaVsPhi",
                                     "4D RecHit pull on beta direction vs phi;#phi "
                                     "(rad);(#alpha^{y}_{rec}-#alpha^{y}_{sim})/#sigma",
                                     100,
                                     -3.2,
                                     3.2,
                                     200,
                                     -5,
                                     5);
      hPullXVsEta = booker.book2D(pre + "_hPullXVsEta",
                                  "4D RecHit pull on position (x) in chamber "
                                  "vs eta;#eta;(x_{rec}-x_{sim})#sigma",
                                  100,
                                  -2.5,
                                  2.5,
                                  150,
                                  -5,
                                  5);
      hPullXVsPhi = booker.book2D(pre + "_hPullXVsPhi",
                                  "4D RecHit pull on position (x) in chamber "
                                  "vs phi;#phi (rad);(x_{rec}-x_{sim})/#sigma",
                                  100,
                                  -3.2,
                                  3.2,
                                  150,
                                  -5,
                                  5);
      hPullYVsEta = booker.book2D(pre + "_hPullYVsEta",
                                  "4D RecHit pull on position (y) in chamber "
                                  "vs eta;#eta;(y_{rec}-y_{sim})/#sigma",
                                  100,
                                  -2.5,
                                  2.5,
                                  150,
                                  -5,
                                  5);
      hPullYVsPhi = booker.book2D(pre + "_hPullYVsPhi",
                                  "4D RecHit pull on position (y) in chamber "
                                  "vs phi;#phi (rad);(y_{rec}-y_{sim})/#sigma",
                                  100,
                                  -3.2,
                                  3.2,
                                  150,
                                  -5,
                                  5);
      hPullBetaVsEtaRZ = booker.book2D(pre + "_hPullBetaVsEtaRZ",
                                       "4D RecHit pull on beta direction vs eta;#eta in RZ "
                                       "SL;(#alpha^{y}_{rec}-#alpha^{y}_{sim})/#sigma",
                                       100,
                                       -2.5,
                                       2.5,
                                       200,
                                       -5,
                                       5);
      hPullBetaVsPhiRZ = booker.book2D(pre + "_hPullBetaVsPhiRZ",
                                       "4D RecHit pull on beta direction vs phi in RZ SL;#phi "
                                       "(rad);(#alpha^{y}_{rec}-#alpha^{y}_{sim})/#sigma",
                                       100,
                                       -3.2,
                                       3.2,
                                       200,
                                       -5,
                                       5);
      hPullYVsEtaRZ = booker.book2D(pre + "_hPullYVsEtaRZ",
                                    "4D RecHit pull on position (y) in chamber vs eta in "
                                    "RZ SL;#eta;(y_{rec}-y_{sim})/#sigma",
                                    100,
                                    -2.5,
                                    2.5,
                                    150,
                                    -5,
                                    5);
      hPullYVsPhiRZ = booker.book2D(pre + "_hPullYVsPhiRZ",
                                    "4D RecHit pull on position (y) in chamber vs phi in "
                                    "RZ SL;#phi (rad);(y_{rec}-y_{sim})/#sigma",
                                    100,
                                    -3.2,
                                    3.2,
                                    150,
                                    -5,
                                    5);
    }
    booker.setCurrentFolder("DT/4DSegments/Res/");
    hResAlpha = booker.book1D(pre + "_hResAlpha",
                              "4D RecHit residual on #alpha_x "
                              "direction;#alpha^{x}_{rec}-#alpha^{x}_{sim} (rad)",
                              200,
                              -0.015,
                              0.015);

    hResBeta = booker.book1D(pre + "_hResBeta",
                             "4D RecHit residual on beta "
                             "direction;#alpha^{y}_{rec}-#alpha^{y}_{sim} (rad)",
                             200,
                             -0.1,
                             0.1);
    hResX = booker.book1D(
        pre + "_hResX", "4D RecHit residual on position (x) in chamber;x_{rec}-x_{sim} (cm)", 150, -0.15, 0.15);
    hResY = booker.book1D(
        pre + "_hResY", "4D RecHit residual on position (y) in chamber;y_{rec}-y_{sim} (cm)", 150, -0.6, 0.6);

    // histo in rz SL reference frame.
    hResBetaRZ = booker.book1D(pre + "_hResBetaRZ",
                               "4D RecHit residual on beta direction in RZ "
                               "SL;#alpha^{y}_{rec}-#alpha^{y}_{sim} (rad)",
                               200,
                               -0.1,
                               0.1);

    hResYRZ = booker.book1D(pre + "_hResYRZ",
                            "4D RecHit residual on position (y) in chamber in "
                            "RZ SL;y_{rec}-y_{sim} (cm)",
                            150,
                            -0.15,
                            0.15);

    // Pulls
    booker.setCurrentFolder("DT/4DSegments/Pull/");

    hPullAlpha = booker.book1D(pre + "_hPullAlpha",
                               "4D RecHit pull on #alpha_x "
                               "direction;(#alpha^{x}_{rec}-#alpha^{x}_{sim})/#sigma",
                               200,
                               -5,
                               5);
    hPullBeta = booker.book1D(pre + "_hPullBeta",
                              "4D RecHit pull on beta "
                              "direction;(#alpha^{y}_{rec}-#alpha^{y}_{sim})/#sigma",
                              200,
                              -5,
                              5);

    hPullX =
        booker.book1D(pre + "_hPullX", "4D RecHit pull on position (x) in chamber;(x_{rec}-x_{sim})#sigma", 150, -5, 5);

    hPullY = booker.book1D(
        pre + "_hPullY", "4D RecHit pull on position (y) in chamber;(y_{rec}-y_{sim})/#sigma", 150, -5, 5);

    hPullBetaRZ = booker.book1D(pre + "_hPullBetaRZ",
                                "4D RecHit pull on beta direction in RZ "
                                "SL;(#alpha^{y}_{rec}-#alpha^{y}_{sim})/#sigma",
                                200,
                                -5,
                                5);

    hPullYRZ = booker.book1D(pre + "_hPullYRZ",
                             "4D RecHit pull on position (y) in chamber in RZ "
                             "SL;(y_{rec}-y_{sim})/#sigma",
                             150,
                             -5,
                             5);

    // NHits, t0
    if (local_) {
      booker.setCurrentFolder("DT/4DSegments/");
      hHitMult = booker.book2D(pre + "_hNHits", "NHits", 12, 0, 12, 6, 0, 6);
      ht0 = booker.book2D(pre + "_ht0", "t0", 200, -25, 25, 200, -25, 25);
    }
  }

  void fill(float simDirectionAlpha,
            float recDirectionAlpha,
            float simDirectionBeta,
            float recDirectionBeta,
            float simX,
            float recX,
            float simY,
            float recY,
            float simEta,
            float simPhi,
            float recYRZ,
            float simYRZ,
            float recBetaRZ,
            float simBetaRZ,
            float sigmaAlpha,
            float sigmaBeta,
            float sigmaX,
            float sigmaY,
            float sigmaBetaRZ,
            float sigmaYRZ,
            int nHitsPhi,
            int nHitsTheta,
            float t0Phi,
            float t0Theta) {
    float resAlpha = recDirectionAlpha - simDirectionAlpha;
    hResAlpha->Fill(resAlpha);
    hPullAlpha->Fill(resAlpha / sigmaAlpha);
    float resBeta = recDirectionBeta - simDirectionBeta;
    hResBeta->Fill(resBeta);
    hPullBeta->Fill(resBeta / sigmaBeta);
    float resX = recX - simX;
    hResX->Fill(resX);
    hPullX->Fill(resX / sigmaX);
    float resY = recY - simY;
    hResY->Fill(resY);
    hPullY->Fill(resY / sigmaY);

    float resBetaRZ = recBetaRZ - simBetaRZ;
    hResBetaRZ->Fill(resBetaRZ);
    hPullBetaRZ->Fill(resBetaRZ / sigmaBetaRZ);
    float resYRZ = recYRZ - simYRZ;
    hResYRZ->Fill(resYRZ);
    hPullYRZ->Fill(resYRZ / sigmaYRZ);
    if (doall_) {
      hRecAlpha->Fill(recDirectionAlpha);
      hRecBeta->Fill(recDirectionBeta);
      hSimAlpha->Fill(simDirectionAlpha);
      hSimBeta->Fill(simDirectionBeta);
      hRecVsSimAlpha->Fill(simDirectionAlpha, recDirectionAlpha);
      hRecVsSimBeta->Fill(simDirectionBeta, recDirectionBeta);
      hResAlphaVsEta->Fill(simEta, resAlpha);
      hResAlphaVsPhi->Fill(simPhi, resAlpha);
      hPullAlphaVsEta->Fill(simEta, resAlpha / sigmaAlpha);
      hPullAlphaVsPhi->Fill(simPhi, resAlpha / sigmaAlpha);
      hResBetaVsEta->Fill(simEta, resBeta);
      hResBetaVsPhi->Fill(simPhi, resBeta);
      hPullBetaVsEta->Fill(simEta, resBeta / sigmaBeta);
      hPullBetaVsPhi->Fill(simPhi, resBeta / sigmaBeta);
      hResXVsEta->Fill(simEta, resX);
      hResXVsPhi->Fill(simPhi, resX);
      hPullXVsEta->Fill(simEta, resX / sigmaX);
      hPullXVsPhi->Fill(simPhi, resX / sigmaX);
      hResYVsEta->Fill(simEta, resY);
      hResYVsPhi->Fill(simPhi, resY);
      hPullYVsEta->Fill(simEta, resY / sigmaY);
      hPullYVsPhi->Fill(simPhi, resY / sigmaY);
      hResAlphaVsResBeta->Fill(resBeta, resAlpha);
      hResXVsResY->Fill(resY, resX);
      hResAlphaVsResX->Fill(resX, resAlpha);
      hResAlphaVsResY->Fill(resY, resAlpha);

      // RZ SuperLayer
      hRecBetaRZ->Fill(recBetaRZ);
      hSimBetaRZ->Fill(simBetaRZ);
      hRecVsSimBetaRZ->Fill(simBetaRZ, recBetaRZ);
      hResBetaVsEtaRZ->Fill(simEta, resBetaRZ);
      hResBetaVsPhiRZ->Fill(simPhi, resBetaRZ);
      hPullBetaVsEtaRZ->Fill(simEta, resBetaRZ / sigmaBetaRZ);
      hPullBetaVsPhiRZ->Fill(simPhi, resBetaRZ / sigmaBetaRZ);
      hResYVsEtaRZ->Fill(simEta, resYRZ);
      hResYVsPhiRZ->Fill(simPhi, resYRZ);
      hPullYVsEtaRZ->Fill(simEta, resYRZ / sigmaYRZ);
      hPullYVsPhiRZ->Fill(simPhi, resYRZ / sigmaYRZ);
    }
    if (local_) {
      hHitMult->Fill(nHitsPhi, nHitsTheta);
      ht0->Fill(t0Phi, t0Theta);
    }
  }

private:
  dqm::reco::MonitorElement const* hRecAlpha;
  dqm::reco::MonitorElement const* hRecBeta;
  dqm::reco::MonitorElement const* hSimAlpha;
  dqm::reco::MonitorElement const* hSimBeta;
  dqm::reco::MonitorElement const* hRecVsSimAlpha;
  dqm::reco::MonitorElement const* hRecVsSimBeta;
  dqm::reco::MonitorElement const* hResAlpha;
  dqm::reco::MonitorElement const* hResAlphaVsEta;
  dqm::reco::MonitorElement const* hResAlphaVsPhi;
  dqm::reco::MonitorElement const* hResBeta;
  dqm::reco::MonitorElement const* hResBetaVsEta;
  dqm::reco::MonitorElement const* hResBetaVsPhi;
  dqm::reco::MonitorElement const* hResX;
  dqm::reco::MonitorElement const* hResXVsEta;
  dqm::reco::MonitorElement const* hResXVsPhi;
  dqm::reco::MonitorElement const* hResY;
  dqm::reco::MonitorElement const* hResYVsEta;
  dqm::reco::MonitorElement const* hResYVsPhi;
  dqm::reco::MonitorElement const* hResAlphaVsResBeta;
  dqm::reco::MonitorElement const* hResXVsResY;
  dqm::reco::MonitorElement const* hResAlphaVsResX;
  dqm::reco::MonitorElement const* hResAlphaVsResY;
  dqm::reco::MonitorElement const* hPullAlpha;
  dqm::reco::MonitorElement const* hPullAlphaVsEta;
  dqm::reco::MonitorElement const* hPullAlphaVsPhi;
  dqm::reco::MonitorElement const* hPullBeta;
  dqm::reco::MonitorElement const* hPullBetaVsEta;
  dqm::reco::MonitorElement const* hPullBetaVsPhi;
  dqm::reco::MonitorElement const* hPullX;
  dqm::reco::MonitorElement const* hPullXVsEta;
  dqm::reco::MonitorElement const* hPullXVsPhi;
  dqm::reco::MonitorElement const* hPullY;
  dqm::reco::MonitorElement const* hPullYVsEta;
  dqm::reco::MonitorElement const* hPullYVsPhi;

  // RZ SL
  dqm::reco::MonitorElement const* hRecBetaRZ;
  dqm::reco::MonitorElement const* hSimBetaRZ;
  dqm::reco::MonitorElement const* hRecVsSimBetaRZ;
  dqm::reco::MonitorElement const* hResBetaRZ;
  dqm::reco::MonitorElement const* hResBetaVsEtaRZ;
  dqm::reco::MonitorElement const* hResBetaVsPhiRZ;
  dqm::reco::MonitorElement const* hResYRZ;
  dqm::reco::MonitorElement const* hResYVsEtaRZ;
  dqm::reco::MonitorElement const* hResYVsPhiRZ;
  dqm::reco::MonitorElement const* hPullBetaRZ;
  dqm::reco::MonitorElement const* hPullBetaVsEtaRZ;
  dqm::reco::MonitorElement const* hPullBetaVsPhiRZ;
  dqm::reco::MonitorElement const* hPullYRZ;
  dqm::reco::MonitorElement const* hPullYVsEtaRZ;
  dqm::reco::MonitorElement const* hPullYVsPhiRZ;

  dqm::reco::MonitorElement const* hHitMult;
  dqm::reco::MonitorElement const* ht0;

  bool doall_;
  bool local_;
  std::string name_;
};

//---------------------------------------------------------------------------------------
/// A set of histograms for efficiency 4D RecHits (producer)
class HEff4DHit {
public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  HEff4DHit(const std::string& name, DQMStore::IBooker& booker) {
    std::string pre = "4D_";
    pre += name;
    name_ = pre;
    booker.setCurrentFolder("DT/4DSegments/");
    hEtaSimSegm = booker.book1D(pre + "_hEtaSimSegm", "Eta of SimHit segment", 100, -1.5, 1.5);
    hEtaRecHit =
        booker.book1D(pre + "_hEtaRecHit", "Eta distribution of SimHit segment with 4D RecHit", 100, -1.5, 1.5);

    hPhiSimSegm = booker.book1D(pre + "_hPhiSimSegm", "Phi of SimHit segment", 100, -M_PI, M_PI);
    hPhiRecHit =
        booker.book1D(pre + "_hPhiRecHit", "Phi distribution of SimHit segment with 4D RecHit", 100, -M_PI, M_PI);

    hXSimSegm = booker.book1D(pre + "_hXSimSegm", "X position in Chamber of SimHit segment (cm)", 100, -200, 200);
    hXRecHit =
        booker.book1D(pre + "_hXRecHit", "X position in Chamber of SimHit segment with 4D RecHit (cm)", 100, -200, 200);

    hYSimSegm = booker.book1D(pre + "_hYSimSegm", "Y position in Chamber of SimHit segment (cm)", 100, -200, 200);
    hYRecHit =
        booker.book1D(pre + "_hYRecHit", "Y position in Chamber of SimHit segment with 4D RecHit (cm)", 100, -200, 200);

    hAlphaSimSegm = booker.book1D(pre + "_hAlphaSimSegm", "Alpha of SimHit segment (rad)", 100, -1.5, 1.5);
    hAlphaRecHit = booker.book1D(pre + "_hAlphaRecHit", "Alpha of SimHit segment with 4D RecHit (rad)", 100, -1.5, 1.5);

    hBetaSimSegm = booker.book1D(pre + "_hBetaSimSegm", "Beta of SimHit segment (rad)", 100, -2, 2);
    hBetaRecHit = booker.book1D(pre + "_hBetaRecHit", "Beta of SimHit segment with 4D RecHit (rad)", 100, -2, 2);

    hNSeg = booker.book1D(pre + "_hNSeg", "Number of rec segment per sim seg", 20, 0, 20);
  }

  void fill(float etaSimSegm,
            float phiSimSegm,
            float xSimSegm,
            float ySimSegm,
            float alphaSimSegm,
            float betaSimSegm,
            bool fillRecHit,
            int nSeg) {
    hEtaSimSegm->Fill(etaSimSegm);
    hPhiSimSegm->Fill(phiSimSegm);
    hXSimSegm->Fill(xSimSegm);
    hYSimSegm->Fill(ySimSegm);
    hAlphaSimSegm->Fill(alphaSimSegm);
    hBetaSimSegm->Fill(betaSimSegm);
    hNSeg->Fill(nSeg);

    if (fillRecHit) {
      hEtaRecHit->Fill(etaSimSegm);
      hPhiRecHit->Fill(phiSimSegm);
      hXRecHit->Fill(xSimSegm);
      hYRecHit->Fill(ySimSegm);
      hAlphaRecHit->Fill(alphaSimSegm);
      hBetaRecHit->Fill(betaSimSegm);
    }
  }

private:
  dqm::reco::MonitorElement const* hEtaSimSegm;
  dqm::reco::MonitorElement const* hEtaRecHit;
  dqm::reco::MonitorElement const* hPhiSimSegm;
  dqm::reco::MonitorElement const* hPhiRecHit;
  dqm::reco::MonitorElement const* hXSimSegm;
  dqm::reco::MonitorElement const* hXRecHit;
  dqm::reco::MonitorElement const* hYSimSegm;
  dqm::reco::MonitorElement const* hYRecHit;
  dqm::reco::MonitorElement const* hAlphaSimSegm;
  dqm::reco::MonitorElement const* hAlphaRecHit;
  dqm::reco::MonitorElement const* hBetaSimSegm;
  dqm::reco::MonitorElement const* hBetaRecHit;

  dqm::reco::MonitorElement const* hNSeg;

  std::string name_;
};

//---------------------------------------------------------------------------------------
/// A set of histograms for efficiency 4D RecHits (harvesting)
class HEff4DHitHarvest {
public:
  typedef dqm::legacy::DQMStore DQMStore;
  typedef dqm::legacy::MonitorElement MonitorElement;

  HEff4DHitHarvest(const std::string& name, DQMStore::IBooker& booker, DQMStore::IGetter& getter) {
    std::string pre = "4D_";
    pre += name;
    name_ = pre;
    booker.setCurrentFolder("DT/4DSegments/");
    hEffVsEta = booker.book1D(pre + "_hEffVsEta", "4D RecHit Efficiency as a function of Eta", 100, -1.5, 1.5);
    hEffVsPhi = booker.book1D(pre + "_hEffVsPhi", "4D RecHit Efficiency as a function of Phi", 100, -M_PI, M_PI);
    hEffVsX =
        booker.book1D(pre + "_hEffVsX", "4D RecHit Efficiency as a function of x position in Chamber", 100, -200, 200);
    hEffVsY =
        booker.book1D(pre + "_hEffVsY", "4D RecHit Efficiency as a function of y position in Chamber", 100, -200, 200);
    hEffVsAlpha = booker.book1D(pre + "_hEffVsAlpha", "4D RecHit Efficiency as a function of alpha", 100, -1.5, 1.5);
    hEffVsBeta = booker.book1D(pre + "_hEffVsBeta", "4D RecHit Efficiency as a function of beta", 100, -2, 2);

    computeEfficiency(getter);
  }

  void computeEfficiency(DQMStore::IGetter& getter) {
    std::string pre = "DT/4DSegments/" + name_;
    divide(hEffVsEta, getter.get(pre + "_hEtaRecHit"), getter.get(pre + "_hEtaSimSegm"));
    divide(hEffVsPhi, getter.get(pre + "_hPhiRecHit"), getter.get(pre + "_hPhiSimSegm"));
    divide(hEffVsX, getter.get(pre + "_hXRecHit"), getter.get(pre + "_hXSimSegm"));
    divide(hEffVsY, getter.get(pre + "_hYRecHit"), getter.get(pre + "_hYSimSegm"));
    divide(hEffVsAlpha, getter.get(pre + "_hAlphaRecHit"), getter.get(pre + "_hAlphaSimSegm"));
    divide(hEffVsBeta, getter.get(pre + "_hBetaRecHit"), getter.get(pre + "_hBetaSimSegm"));
  }

private:
  MonitorElement* hEffVsEta;
  MonitorElement* hEffVsPhi;

  MonitorElement* hEffVsX;
  MonitorElement* hEffVsY;

  MonitorElement* hEffVsAlpha;
  MonitorElement* hEffVsBeta;

  std::string name_;
};

#endif  // Validation_DTRecHits_Histograms_h
