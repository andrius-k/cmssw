#ifndef DQMServices_Core_DQMEDAnalyzer_h
#define DQMServices_Core_DQMEDAnalyzer_h

#include "FWCore/Framework/interface/stream/makeGlobal.h"

namespace dqm {
  namespace reco {
    struct DQMEDAnalyzerGlobalCache;
  };  // namespace reco
}  // namespace dqm

namespace edm::stream::impl {
  template <typename T>
  T* makeStreamModule(edm::ParameterSet const& iPSet, dqm::reco::DQMEDAnalyzerGlobalCache const* global) {
    return new T(iPSet);
  }
}  // namespace edm::stream::impl

#include "DQMServices/Core/interface/DQMStore.h"

#include "FWCore/Framework/interface/stream/EDProducer.h"
#include "FWCore/Utilities/interface/EDPutToken.h"

namespace dqm {
  namespace reco {
    struct DQMEDAnalyzerGlobalCache {
      // this could be a value instead, but then the DQMStore should take "borrowed" refs.
      std::shared_ptr<DQMStore::DQMStoreMaster> master_ = std::make_shared<DQMStore::DQMStoreMaster>();
      mutable edm::EDPutTokenT<MonitorElementCollection> lumiToken_;
      mutable edm::EDPutTokenT<MonitorElementCollection> runToken_;
    };
  };  // namespace reco
}  // namespace dqm

class DQMEDAnalyzer : public edm::stream::EDProducer<
                          // We use a lot of edm features. We need all the caches to get the global
                          // transitions, where we can pull the (shared) MonitorElements out of the
                          // DQMStores.
                          edm::GlobalCache<dqm::reco::DQMEDAnalyzerGlobalCache>,
                          edm::EndLuminosityBlockProducer,
                          edm::EndRunProducer,
                          // This feature is essentially made for DQM and required to get per-event calls.
                          edm::Accumulator> {
protected:
  std::shared_ptr<dqm::reco::DQMStore> dqmstore_;
  edm::EDPutTokenT<MonitorElementCollection> lumiToken_;
  edm::EDPutTokenT<MonitorElementCollection> runToken_;

public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  // The following EDM methods are listed (as much as possible) in the order
  // that edm calls them in.

  static std::unique_ptr<dqm::reco::DQMEDAnalyzerGlobalCache> initializeGlobalCache(edm::ParameterSet const&) {
    return std::make_unique<dqm::reco::DQMEDAnalyzerGlobalCache>();
  }

  DQMEDAnalyzer() {
    lumiToken_ = produces<MonitorElementCollection, edm::Transition::EndLuminosityBlock>("DQMGenerationRecoLumi");
    runToken_ = produces<MonitorElementCollection, edm::Transition::EndRun>("DQMGenerationRecoRun");
  }

  // TODO: this is overridden in subsystem code, make sure that is safe.
  virtual void beginJob(){};

  void beginStream(edm::StreamID id) {
    dqmstore_ = std::make_unique<DQMStore>();
    dqmstore_->setMaster(globalCache()->master_);

    auto lock = std::scoped_lock(globalCache()->master_->lock_);
    if (globalCache()->runToken_.isUninitialized()) {
      globalCache()->lumiToken_ = lumiToken_;
      globalCache()->runToken_ = runToken_;
    }
  }

  void beginRun(edm::Run const& run, edm::EventSetup const& setup) {
    dqmBeginRun(run, setup);
    // For multi-run harvesting, we should have a job-level granularity which
    // can also be used by default. Maybe we can make that a per-plugin option?
    dqmstore_->setScope(MonitorElementData::Scope::LUMI);
    this->bookHistograms(*dqmstore_, run, setup);
  }

  void beginLuminosityBlock(edm::LuminosityBlock const& lumi, edm::EventSetup const& setup) {
    dqmstore_->enterLumi(lumi.run(), lumi.luminosityBlock());
    dqmBeginLuminosityBlock(lumi, setup);
  }

  void accumulate(edm::Event const& ev, edm::EventSetup const& es) { analyze(ev, es); }

  void endLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&){};

  static void globalEndLuminosityBlockProduce(edm::LuminosityBlock& lumi,
                                              edm::EventSetup const& setup,
                                              LuminosityBlockContext const* context) {
    auto lock = std::scoped_lock(context->global()->master_->lock_);
    auto& master = context->global()->master_->master_;
    lumi.emplace(context->global()->lumiToken_,
                 master.toProduct(edm::Transition::EndLuminosityBlock, lumi.run(), lumi.luminosityBlock()));
  }

  void endRun(edm::Run const& run, edm::EventSetup const& setup){};

  static void globalEndRunProduce(edm::Run& run, edm::EventSetup const& setup, RunContext const* context) {
    auto lock = std::scoped_lock(context->global()->master_->lock_);
    auto& master = context->global()->master_->master_;
    run.emplace(context->global()->runToken_,
                master.toProduct(edm::Transition::EndRun, run.run(), edm::invalidLuminosityBlockNumber));
  }

  static void globalEndJob(dqm::reco::DQMEDAnalyzerGlobalCache*){};

  // Methods to be implemented by the users
  virtual void bookHistograms(DQMStore::IBooker& i, edm::Run const&, edm::EventSetup const&) = 0;
  virtual void dqmBeginRun(edm::Run const& run, edm::EventSetup const& setup){};
  virtual void dqmBeginLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&){};
  virtual void analyze(edm::Event const&, edm::EventSetup const&){};
  // void endJob() final; // TODO: implemented in a few palces, not sure if ever called.
};

#endif  // DQMServices_Core_DQMEDAnalyzer_h
