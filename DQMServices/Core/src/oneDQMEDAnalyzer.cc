#include "DQMServices/Core/interface/oneDQMEDAnalyzer.h"

using namespace one::dqmimplementation;

DQMLumisEDProducer::DQMLumisEDProducer()
    : lumiToken_{produces<DQMToken, edm::Transition::EndLuminosityBlock>("endLumi")} {}

void DQMLumisEDProducer::beginLuminosityBlock(edm::LuminosityBlock const& lumi, edm::EventSetup const& setup) {
  dqmBeginLuminosityBlock(lumi, setup);
}

void DQMLumisEDProducer::dqmBeginLuminosityBlock(edm::LuminosityBlock const& lumi, edm::EventSetup const& setup) {}

void DQMLumisEDProducer::endLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&) {}

void DQMLumisEDProducer::endLuminosityBlockProduce(edm::LuminosityBlock& lumi, edm::EventSetup const& setup) {
  lumi.emplace<DQMToken>(lumiToken_);
}
