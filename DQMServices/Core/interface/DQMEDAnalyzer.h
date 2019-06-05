#ifndef DQMServices_Core_DQMEDAnalyzer_h
#define DQMServices_Core_DQMEDAnalyzer_h

#include "DQMServices/Core/interface/oneDQMEDAnalyzer.h"
#include "DQMServices/Core/interface/DQMStore.h"

namespace dqm {
  namespace reco {
    class DQMStoreGroup {
      mutable std::vector<std::shared_ptr<DQMStore>> dqmstores_;
      mutable std::mutex lock_; 
      public:
      auto forId(int id) const {
        auto lock = std::scoped_lock(lock_);
        if (dqmstores_.size()-1 < id) {
          dqmstores_.resize(id+1);
        }
        if (!dqmstores_[id]) {
          dqmstores[id] = std::make_shared<DQMStore>();
        }
        // we thread-unsafely leak a reference here. This should only be used
        // after intialisation once this structure is no longer modified.
        dqmstores[id]->setSiblings(&dqmstores_);
        return dqmstores[id];
      }
    };

class DQMEDAnalyzer : edm::stream::EDProducer<
  // We use a lot of edm features. We need all the caches to get the global 
  // transitions, where we can pull the (shared) MonitorElements out of the 
  // DQMStores.
  edm::GlobalCache<DQMStoreGroup>,
  edm::EndLuminosityBlockProducer,
  edm::LuminosityBlockSummaryCache<MonitorElementCollection>,
  edm::EndRunProducer,
  edm::RunSummaryCache<MonitorElementCollection>,
  // This feature is essentially made for DQM and required to get per-event calls.
  edm::Accumulator
  > {
public:
  // The following EDM methods are listed (as much as possible) in the order 
  // that edm calls them in.

  static std::unique_ptr<DQMStoreGroup> initializeGlobalCache(edm::ParameterSet const&) {
    return std::make_unique<DQMStoreGroup>();
  }

  // TODO: will the second argument break every single derived module?
  DQMEDAnalyzer(edm::ParameterSet const& /*, DQMStoreGroup const* */) {
  }

  void beginStream(edm::StreamID id) {
    dqmstore_ = globalCache()->forId(id);
  }

  static std::shared_ptr<MonitorElementCollection> globalBeginRunSumary(edm::Run const&, edm::EventSetup const&, RunContxt const*) {
    return std::make_shared<MonitorElementCollection>();
  }

  void beginRun(edm::Run const& run, edm::EventSetup const& setup) final {
    dqmBeginRun(run, setup);
    // TODO: make sure booking does set the run number for each ME.
    // So if we see two runs, we can re-book without conflicts and can choose
    // to keep or drop the MEs from the previous run.
    // For multi-run harvesting, we should have a job-level granularity which
    // can also be used by default. Maybe we can make that a per-plugin option?
    this->bookHistograms(*dqmstore_, run, setup);
  }

  static std::shared_ptr<MonitorElementCollection> globalBeginLuminosityBlockSummary(edm::LuminosityBlock const& lumi, edm::EventSetup const& setup, LuminosityBlockContext const* constex) {
    // this is just a placeholder, we swap in the real thing in endLuminosityBlockSummary.
    return std::make_shared<MonitorElementCollection>();
  }

  void beginLuminosityBlock(edm::LuminosityBlock const& lumi, edm::EventSetup const& setup) override {
    dqmBeginLuminosityBlock(lumi, setup);
  }

  void accumulate(edm::Event const& ev, edm::EventSetup const& es) final { 
    analyze(ev, es); 
  }

  void endLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&) override {};

  void endLuminosityBlockSummary(edm::LuminosityBlock const& lumi, edm::EventSetup const& setup, LuminosityBlockContext const* context) {};

  static void globalEndLuminosityBlockSummary(edm::LuminosityBlock const& lumi, edm::EventSetup const& setup, LuminosityBlockContext const* context) {
    // TODO: all streams are done with the lumi, we can pull the MEs from the
    // DQMStores. We have a const* here, I hope it is fine to modify that.
  }

  static void endLuminosityBlockProduce(edm::LuminosityBlock& lumi, edm::EventSetup const& setup, LuminosityBlockContext const* context, MonitorElementCollection const* data) final {
    // TODO: this should be no more than moving the `data` into the `lumi`.
  }

  void endRun(edm::Run const& run, edm::EventSetup const& setup) override {};

  void endRunSummary(edm::Run const& run, edm::EventSetup const& setup, MonitorElementCollection* data) override {};

  static void globalEndRunSummary(edm::Run const& run, edm::EventSetup const& setup, RunContxt const* context) override {
    // TODO: all streams are done with the run, we can pull the MEs from the
    // DQMStores. We have a const* here, I hope it is fine to modify that.
  }

  static void globalEndRunProduce(edm::Run& run, edm::EventSetup const& setup, MonitorElementCollection const* data) override { 
    // TODO: this should be no more than moving the `data` into the `run`.
    run.emplace<DQMToken>(runToken_);
  }

  static void globalEndJob(DQMStoreGroup*) {};

  // Methods to be implemented by the users
  virtual void bookHistograms(DQMStore::IBooker& i, edm::Run const&, edm::EventSetup const&) = 0;
  virtual void dqmBeginRun(edm::Run const& run, edm::EventSetup const& setup) {};
  virtual void dqmBeginLuminosityBlock(edm::LuminosityBlock const&, edm::EventSetup const&) {};
  virtual void analyze(edm::Event const&, edm::EventSetup const&) {};
}

  }
}

using dqm::reco::DQMEDAnalyzer;

#endif // DQMServices_Core_DQMEDAnalyzer_h
