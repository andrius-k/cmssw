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
    auto t = new T(iPSet);
    t->initTokens(global);
    return t;
  }
}  // namespace edm::stream::impl

#include "DQMServices/Core/interface/DQMStore.h"

#include "FWCore/Framework/interface/stream/EDProducer.h"
#include "FWCore/Utilities/interface/EDPutToken.h"
#include "DataFormats/Histograms/interface/DQMToken.h"

namespace dqm {
  namespace reco {
    class MonitorElementCollectionHolder {
      // nothing to hold here, we just keep stuff in the DQMStore until produce().
    };
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
                          edm::LuminosityBlockSummaryCache<dqm::reco::MonitorElementCollectionHolder>,
                          edm::EndRunProducer,
                          edm::RunSummaryCache<dqm::reco::MonitorElementCollectionHolder>,
                          // This feature is essentially made for DQM and required to get per-event calls.
                          edm::Accumulator> {
protected:
  std::shared_ptr<dqm::reco::DQMStore> dqmstore_;

public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  // The following EDM methods are listed (as much as possible) in the order
  // that edm calls them in.

  static std::unique_ptr<dqm::reco::DQMEDAnalyzerGlobalCache> initializeGlobalCache(edm::ParameterSet const&) {
    return std::make_unique<dqm::reco::DQMEDAnalyzerGlobalCache>();
  }

  DQMEDAnalyzer() {
  }

  void initTokens(dqm::reco::DQMEDAnalyzerGlobalCache const* global) {
    auto lock = std::scoped_lock(global->master_->lock_);
    global->lumiToken_ = produces<MonitorElementCollection, edm::Transition::EndLuminosityBlock>("DQMGenerationRecoLumi");
    global->runToken_ = produces<MonitorElementCollection, edm::Transition::EndRun>("DQMGenerationRecoRun");
  }

  void beginStream(edm::StreamID id) {
    dqmstore_ = std::make_unique<DQMStore>();
    dqmstore_->setMaster(globalCache()->master_);
  }

  static std::shared_ptr<dqm::reco::MonitorElementCollectionHolder> globalBeginRunSummary(edm::Run const&,
                                                                                          edm::EventSetup const&,
                                                                                          RunContext const*) {
    return std::make_shared<dqm::reco::MonitorElementCollectionHolder>();
  }

  void beginRun(edm::Run const& run, edm::EventSetup const& setup) {
    dqmBeginRun(run, setup);
    // For multi-run harvesting, we should have a job-level granularity which
    // can also be used by default. Maybe we can make that a per-plugin option?
    this->bookHistograms(*dqmstore_, run, setup);
  }

  static std::shared_ptr<dqm::reco::MonitorElementCollectionHolder> globalBeginLuminosityBlockSummary(
      edm::LuminosityBlock const& lumi, edm::EventSetup const& setup, LuminosityBlockContext const* constex) {
    // this is just a placeholder, we swap in the real thing in endLuminosityBlockSummary.
    return std::make_shared<dqm::reco::MonitorElementCollectionHolder>();
  }

  void beginLuminosityBlock(edm::LuminosityBlock const& lumi, edm::EventSetup const& setup) {
    dqmstore_->enterLumi(lumi.luminosityBlock(), lumi.run());
    dqmBeginLuminosityBlock(lumi, setup);
  }

  void accumulate(edm::Event const& ev, edm::EventSetup const& es) { analyze(ev, es); }

  void endLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&){};

  void endLuminosityBlockSummary(edm::LuminosityBlock const& lumi,
                                 edm::EventSetup const& setup,
                                 dqm::reco::MonitorElementCollectionHolder* data) const {};

  static void globalEndLuminosityBlockSummary(edm::LuminosityBlock const& lumi,
                                              edm::EventSetup const& setup,
                                              LuminosityBlockContext const* context,
                                              dqm::reco::MonitorElementCollectionHolder* data) {}

  static void globalEndLuminosityBlockProduce(edm::LuminosityBlock& lumi,
                                              edm::EventSetup const& setup,
                                              LuminosityBlockContext const* context,
                                              dqm::reco::MonitorElementCollectionHolder const* data) {
    auto lock = std::scoped_lock(context->global()->master_->lock_);
    auto& master = context->global()->master_->master_;
    lumi.emplace(context->global()->lumiToken_,  master.toProduct(edm::Transition::EndLuminosityBlock, lumi.run(), lumi.luminosityBlock()));
  }

  void endRun(edm::Run const& run, edm::EventSetup const& setup){};

  void endRunSummary(edm::Run const& run,
                     edm::EventSetup const& setup,
                     dqm::reco::MonitorElementCollectionHolder* data) const {};

  static void globalEndRunSummary(edm::Run const& run,
                                  edm::EventSetup const& setup,
                                  RunContext const* context,
                                  dqm::reco::MonitorElementCollectionHolder const* data) {}

  static void globalEndRunProduce(edm::Run& run,
                                  edm::EventSetup const& setup,
                                  RunContext const* context,
                                  dqm::reco::MonitorElementCollectionHolder const* data) {
    auto lock = std::scoped_lock(context->global()->master_->lock_);
    auto& master = context->global()->master_->master_;
    run.emplace(context->global()->runToken_, master.toProduct(edm::Transition::EndRun, run.run(), edm::invalidLuminosityBlockNumber));
  }

  static void globalEndJob(dqm::reco::DQMEDAnalyzerGlobalCache*){};

  // Methods to be implemented by the users
  virtual void bookHistograms(DQMStore::IBooker& i, edm::Run const&, edm::EventSetup const&) = 0;
  virtual void dqmBeginRun(edm::Run const& run, edm::EventSetup const& setup){};
  virtual void dqmBeginLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&){};
  virtual void analyze(edm::Event const&, edm::EventSetup const&){};
};

#endif  // DQMServices_Core_DQMEDAnalyzer_h
