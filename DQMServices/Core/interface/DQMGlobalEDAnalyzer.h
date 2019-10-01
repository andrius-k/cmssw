#ifndef DQMServices_Core_DQMGlobalEDAnalyzer_h
#define DQMServices_Core_DQMGlobalEDAnalyzer_h

#include "DQMServices/Core/interface/DQMStore.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/Run.h"
#include "FWCore/Framework/interface/global/EDProducer.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"

template <typename H, typename... Args>
class DQMGlobalEDAnalyzer
    : public edm::global::EDProducer<edm::RunCache<H>,
                                     // DQMGlobalEDAnalyzer are fundamentally unable to produce histograms for any
                                     // other scope than MonitorElement::Scope::RUN.
                                     edm::EndRunProducer,
                                     edm::Accumulator,
                                     Args...> {
public:
  typedef dqm::reco::DQMStore DQMStore;
  typedef dqm::reco::MonitorElement MonitorElement;

  // framework calls in the order of invocation
  DQMGlobalEDAnalyzer() {
    // for whatever reason we need the explicit `template` keyword here.
    runToken_ = this->template produces<MonitorElementCollection, edm::Transition::EndRun>("DQMGenerationRecoRun");
  }

  std::shared_ptr<H> globalBeginRun(edm::Run const& run, edm::EventSetup const& setup) const final {
    auto h = std::make_shared<H>();
    auto lock = std::scoped_lock(dqmstore_lock_);

    dqmBeginRun(run, setup, *h);

    // in case of concurrent runs, this will create clones of the already
    // booked MEs.
    dqmstore_->enterLumi(run.run(), edm::invalidLuminosityBlockNumber);
    // TODO: for concurrent runs, we'd have to make sure to return the correct
    // run/lumi version of the (existing) MEs. This requires keeping track of
    // the current run and lumi in DQMStore or IBooker.
    dqmstore_->setScope(MonitorElementData::Scope::RUN);
    bookHistograms(*dqmstore_, run, setup, *h);
    // Populate run numbers, in case booking only books prototypes.
    // We will not call enterLumi per-lumi, since this is strictly run-based.
    dqmstore_->enterLumi(run.run(), edm::invalidLuminosityBlockNumber);
    return h;
  }

  void accumulate(edm::StreamID id, edm::Event const& event, edm::EventSetup const& setup) const final {
    auto const& h = *this->runCache(event.getRun().index());
    dqmAnalyze(event, setup, h);
  }

  void globalEndRunProduce(edm::Run& run, edm::EventSetup const& setup) const final {
    auto const& h = *this->runCache(run.index());
    dqmEndRun(run, setup, h);

    auto lock = std::scoped_lock(dqmstore_lock_);
    run.emplace(runToken_, dqmstore_->toProduct(edm::Transition::EndRun, run.run(), edm::invalidLuminosityBlockNumber));
  }

  // Subsystems could safely override this, but any changes to MEs would not be
  // noticeable since the product was made already.
  void globalEndRun(edm::Run const&, edm::EventSetup const&) const final{};

  // methods to be implemented by the user, in order of invocation
  virtual void dqmBeginRun(edm::Run const&, edm::EventSetup const&, H&) const {}
  virtual void bookHistograms(DQMStore::IBooker&, edm::Run const&, edm::EventSetup const&, H&) const = 0;
  // TODO: rename this analyze() for consistency.
  virtual void dqmAnalyze(edm::Event const&, edm::EventSetup const&, H const&) const = 0;
  virtual void dqmEndRun(edm::Run const&, edm::EventSetup const&, H const&) const {}

private:
  // This should be held whenever interacting with our DQMStore.
  // The individual MEs however have their own locks.
  mutable std::mutex dqmstore_lock_;
  std::unique_ptr<DQMStore> dqmstore_ = std::make_unique<DQMStore>();
  edm::EDPutTokenT<MonitorElementCollection> runToken_;
};

#endif  // DQMServices_Core_DQMGlobalEDAnalyzer_h
