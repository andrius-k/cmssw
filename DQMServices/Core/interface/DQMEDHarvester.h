#ifndef CORE_DQMED_HARVESTER_H
#define CORE_DQMED_HARVESTER_H

#include "DQMServices/Core/interface/DQMStore.h"

#include "FWCore/Framework/interface/one/EDProducer.h"

#include "FWCore/Framework/interface/Run.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/LuminosityBlock.h"
#include "FWCore/Framework/interface/InputTagMatch.h"
#include "FWCore/Framework/interface/GetterOfProducts.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Utilities/interface/EDPutToken.h"

namespace edm {
  class VInputTagMatch {
  public:
    VInputTagMatch(std::vector<edm::InputTag> const &inputTags) {
      for (auto &tag : inputTags) {
        matchers_.emplace_back(InputTagMatch(tag));
      }
    }

    bool operator()(edm::BranchDescription const &branchDescription) {
      for (auto &m : matchers_) {
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

class DQMEDHarvester : public edm::one::EDProducer<edm::EndLuminosityBlockProducer,
                                                   edm::EndRunProducer,
                                                   edm::one::WatchLuminosityBlocks,
                                                   edm::one::WatchRuns,
                                                   edm::Accumulator> {
public:
  typedef dqm::harvesting::DQMStore DQMStore;
  typedef dqm::harvesting::MonitorElement MonitorElement;

protected:
  std::unique_ptr<DQMStore> dqmstore_;
  edm::GetterOfProducts<MonitorElementCollection> runmegetter_;
  edm::GetterOfProducts<MonitorElementCollection> lumimegetter_;
  edm::EDPutTokenT<MonitorElementCollection> lumiToken_;
  edm::EDPutTokenT<MonitorElementCollection> runToken_;

public:
  DQMEDHarvester(edm::ParameterSet const &iConfig) {
    dqmstore_ = std::make_unique<DQMStore>();
    // TODO: Run/Lumi suffix should not be needed, complain to CMSSW core in case.
    lumiToken_ = produces<MonitorElementCollection, edm::Transition::EndLuminosityBlock>("DQMGenerationHarvestingLumi");
    runToken_ = produces<MonitorElementCollection, edm::Transition::EndRun>("DQMGenerationHarvestingRun");

    // Use explicitly specified inputs, but if there are none...
    auto inputtags =
        iConfig.getUntrackedParameter<std::vector<edm::InputTag>>("inputMEs", std::vector<edm::InputTag>());
    if (inputtags.empty()) {
      // ... use all RECO MEs.
      inputtags.push_back(edm::InputTag("", "DQMGenerationRecoRun"));
      inputtags.push_back(edm::InputTag("", "DQMGenerationRecoLumi"));
    }
    runmegetter_ = edm::GetterOfProducts<MonitorElementCollection>(edm::VInputTagMatch(inputtags), this, edm::InRun);
    lumimegetter_ = edm::GetterOfProducts<MonitorElementCollection>(edm::VInputTagMatch(inputtags), this, edm::InLumi);
    callWhenNewProductsRegistered([this](edm::BranchDescription const &bd) {
      runmegetter_(bd);
      lumimegetter_(bd);
    });
  };

  DQMEDHarvester() : DQMEDHarvester(edm::ParameterSet()){};

  void beginJob() override{};

  void beginRun(edm::Run const &run, edm::EventSetup const &) override {
    // According to edm experts, it is never save to look at run products
    // in beginRun, since they might be merged as new input files how up.
  }

  void beginLuminosityBlock(edm::LuminosityBlock const &lumi, edm::EventSetup const &) final {
    // According to edm experts, it is never save to look at run products
    // in beginRun, since they might be merged as new input files how up.
  }

  void endLuminosityBlockProduce(edm::LuminosityBlock &lumi, edm::EventSetup const &es) final {
    auto refs = std::vector<edm::Handle<MonitorElementCollection>>();
    lumimegetter_.fillHandles(lumi, refs);
    for (auto h : refs) {
      dqmstore_->registerProduct(h);
    }

    // update MEs that we have in the DQMStore, but that might have a new
    // version in the products.
    dqmstore_->loadFromProduct();

    dqmstore_->setScope(MonitorElementData::Scope::LUMI);
    dqmEndLuminosityBlock(*dqmstore_, *dqmstore_, lumi, es);

    // subsystem code might have triggered more imports from the products (by
    // calling get() et. al.). We remove the proxy MEs now if they where not
    // modified, no need to save them for later.
    // This is important also since the MEData's from the products might be
    // deallocated at the end of this method.
    dqmstore_->cleanupFromProduct();

    // call enterLumi in the end since MEs might have been booked in user code.
    // This will add the required metadata to them.
    dqmstore_->enterLumi(lumi.run(), lumi.luminosityBlock());

    // finally call toProduct(). We will end up with prototypes for the MEs
    // that where modified by the subsystem code, which will be replaced/reused
    // in the next transition.
    lumi.emplace(lumiToken_,
                 dqmstore_->toProduct(edm::Transition::EndLuminosityBlock, lumi.run(), lumi.luminosityBlock()));

    // The run/lumi products *will* silently go bad at some point, so we better
    // don't leak any handles out of this method.
    dqmstore_->clearProducts();
  }

  void endLuminosityBlock(edm::LuminosityBlock const &, edm::EventSetup const &) final{};

  void accumulate(edm::Event const &ev, edm::EventSetup const &es) override{};

  void endRunProduce(edm::Run &run, edm::EventSetup const &setup) final {
    // TODO: if we could somehow know that this is the last run to be processed,
    // we could keep the old semantics and put job products
    auto refs = std::vector<edm::Handle<MonitorElementCollection>>();
    runmegetter_.fillHandles(run, refs);
    for (auto h : refs) {
      dqmstore_->registerProduct(h);
    }

    // update MEs that we have in the DQMStore, but that might have a new
    // version in the products.
    dqmstore_->loadFromProduct();

    // TODO: we should, and probably can, rename this.
    dqmstore_->setScope(MonitorElementData::Scope::RUN);
    dqmEndJob(*dqmstore_, *dqmstore_);

    // subsystem code might have triggered more imports from the products (by
    // calling get() et. al.). We remove the proxy MEs now if they where not
    // modified, no need to save them for later.
    // This is important also since the MEData's from the products might be
    // deallocated at the end of this method.
    dqmstore_->cleanupFromProduct();

    // call enterLumi in the end since MEs might have been booked in user code.
    // This will add the required metadata to them.
    dqmstore_->enterLumi(run.run(), edm::invalidLuminosityBlockNumber);

    // finally call toProduct(). We will end up with prototypes for the MEs
    // that where modified by the subsystem code, which will be replaced/reused
    // in the next transition.
    run.emplace(runToken_, dqmstore_->toProduct(edm::Transition::EndRun, run.run(), edm::invalidLuminosityBlockNumber));

    // The run/lumi products *will* silently go bad at some point, so we better
    // don't leak any handles out of this method.
    dqmstore_->clearProducts();
  }

  void endRun(edm::Run const &, edm::EventSetup const &) override{};

  void endJob() final{};

  ~DQMEDHarvester() override = default;

  virtual void dqmEndLuminosityBlock(DQMStore::IBooker &,
                                     DQMStore::IGetter &,
                                     edm::LuminosityBlock const &,
                                     edm::EventSetup const &){};
  virtual void dqmEndJob(DQMStore::IBooker &, DQMStore::IGetter &) = 0;
};

#endif  // CORE_DQMED_HARVESTER_H
