#ifndef CORE_DQMED_HARVESTER_H
#define CORE_DQMED_HARVESTER_H

#include "DQMServices/Core/interface/DQMStore.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/LuminosityBlock.h"
#include "FWCore/Framework/interface/Run.h"
#include "FWCore/Framework/interface/one/EDProducer.h"

#include "FWCore/Utilities/interface/EDPutToken.h"
#include "FWCore/Framework/interface/InputTagMatch.h"
#include "FWCore/Framework/interface/GetterOfProducts.h"



namespace edm {
 class VInputTagMatch {
  public:
    VInputTagMatch(std::vector<edm::InputTag> const& inputTags) {
      for (auto& tag : inputTags) {
        matchers_.emplace_back(InputTagMatch(tag));
      }
    }

    bool operator()(edm::BranchDescription const& branchDescription) {
      for (auto& m : matchers_) {
        if (m(branchDescription)) {
          return true;
        }
      }
      return false;
    }

  private:
    std::vector<InputTagMatch> matchers_;
  };
}  // namespace edm


class DQMEDHarvester : public edm::one::EDProducer<
                                                   edm::EndLuminosityBlockProducer,
                                                   edm::EndRunProducer,
                                                   edm::one::WatchLuminosityBlocks,
                                                   edm::one::WatchRuns,
                                                   edm::Accumulator
                                                   > {

public:
  typedef dqm::harvesting::DQMStore DQMStore;
  typedef dqm::harvesting::MonitorElement MonitorElement;
protected:
  std::unique_ptr<DQMStore> dqmstore_;
  edm::GetterOfProducts<MonitorElementCollection> runmegetter_;
  edm::GetterOfProducts<MonitorElementCollection> lumimegetter_;
public:
  DQMEDHarvester (edm::ParameterSet const& iConfig) {
    dqmstore_ = std::make_unique<DQMStore>();
    // TODO: Run/Lumi suffix should not be needed, complain to CMSSW core in case.
    produces<MonitorElementCollection,edm::Transition::EndLuminosityBlock>("DQMGenerationHarvestingLumi");
    produces<MonitorElementCollection,edm::Transition::EndRun>("DQMGenerationHarvestingRun");

    // Use explicitly specified inputs, but if there are none...
    auto inputtags = iConfig.getUntrackedParameter<std::vector<edm::InputTag>>("inputMEs", std::vector<edm::InputTag>());
    if (inputtags.empty()) {
      // ... use all RECO MEs.
      inputtags.push_back(edm::InputTag("", "DQMGenerationRecoRun"));
      inputtags.push_back(edm::InputTag("", "DQMGenerationRecoLumi"));
    }
    runmegetter_ = edm::GetterOfProducts<MonitorElementCollection>(edm::VInputTagMatch(inputtags), this, edm::InRun);
    lumimegetter_ = edm::GetterOfProducts<MonitorElementCollection>(edm::VInputTagMatch(inputtags), this, edm::InLumi);
    callWhenNewProductsRegistered( [this](edm::BranchDescription const& bd ) {
      runmegetter_(bd);
      lumimegetter_(bd);
    });
  };

  DQMEDHarvester () : DQMEDHarvester(edm::ParameterSet()) {};

  void beginJob() override {};

  void beginRun(edm::Run const & run, edm::EventSetup const &) override{
    // According to edm experts, it is never save to look at run products
    // in beginRun, since they might be merged as new input files how up.
  }

  void beginLuminosityBlock(edm::LuminosityBlock const & lumi, edm::EventSetup const &) final{
    // According to edm experts, it is never save to look at run products
    // in beginRun, since they might be merged as new input files how up.
  }

  void endLuminosityBlockProduce(edm::LuminosityBlock & lumi, edm::EventSetup const & es) final {
    auto refs = std::vector<edm::Handle<MonitorElementCollection>>();
    lumimegetter_.fillHandles(lumi, refs);
    for (auto h : refs) {
      dqmstore_->registerProduct(h);
    }

    dqmEndLuminosityBlock(*dqmstore_, *dqmstore_, lumi, es);

    auto out = dqmstore_->toProduct(edm::Transition::EndLuminosityBlock, lumi.run(), lumi.luminosityBlock());
    auto prod = std::make_unique<MonitorElementCollection>();
    prod->swap(out);
    lumi.put(std::move(prod), "DQMGenerationHarvestingLumi");

  }

  void endLuminosityBlock(edm::LuminosityBlock const &, edm::EventSetup const &) final {};

  void accumulate(edm::Event const &ev, edm::EventSetup const &es) override {};

  void endRunProduce(edm::Run &run, edm::EventSetup const &setup) final {
    // TODO: if we could somehow know that this is the last run to be processed,
    // we could keep the lod semantics and put job products 
    auto refs = std::vector<edm::Handle<MonitorElementCollection>>();
    runmegetter_.fillHandles(run, refs);
    for (auto h : refs) {
      dqmstore_->registerProduct(h);
    }

    // TODO: we should, and probably can, rename this.
    dqmEndJob(*dqmstore_, *dqmstore_);

    auto out = dqmstore_->toProduct(edm::Transition::EndRun, run.run(), 0);
    auto prod = std::make_unique<MonitorElementCollection>();
    prod->swap(out);
    run.put(std::move(prod), "DQMGenerationHarvestingRun");
  }

  void endRun(edm::Run const &, edm::EventSetup const &) override {};

  void endJob() final {};


  ~DQMEDHarvester() override = default;


  virtual void dqmEndLuminosityBlock(DQMStore::IBooker &,
                                     DQMStore::IGetter &,
                                     edm::LuminosityBlock const &,
                                     edm::EventSetup const &){};
  virtual void dqmEndJob(DQMStore::IBooker &, DQMStore::IGetter &) = 0;

};

#endif  // CORE_DQMED_HARVESTER_H
