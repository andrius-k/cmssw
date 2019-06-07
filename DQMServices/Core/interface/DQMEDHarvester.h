#ifndef CORE_DQMED_HARVESTER_H
#define CORE_DQMED_HARVESTER_H

#include "DQMServices/Core/interface/DQMStore.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/EventSetup.h"
#include "FWCore/Framework/interface/LuminosityBlock.h"
#include "FWCore/Framework/interface/Run.h"
#include "FWCore/Framework/interface/one/EDProducer.h"

#include "FWCore/Utilities/interface/EDPutToken.h"

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
public:
  DQMEDHarvester () {
    produces<MonitorElementCollection,edm::Transition::EndLuminosityBlock>("DQMGenerationHarvestingLumi");
    produces<MonitorElementCollection,edm::Transition::EndRun>("DQMGenerationHarvestingRun");
    // should specify DQMGenerationReco, but how?
    consumesMany<MonitorElementCollection, edm::InLumi>();
    consumesMany<MonitorElementCollection, edm::InRun>();
  };

  void beginJob() override {};

  void beginRun(edm::Run const & run, edm::EventSetup const &) override{
    auto refs = std::vector<edm::Handle<MonitorElementCollection>>();
    run.getManyByType(refs);
    for (auto h : refs) {
      dqmstore_->registerProduct(h);
    }
  }

  void beginLuminosityBlock(edm::LuminosityBlock const & lumi, edm::EventSetup const &) final{
    auto refs = std::vector<edm::Handle<MonitorElementCollection>>();
    lumi.getManyByType(refs);
    for (auto h : refs) {
      dqmstore_->registerProduct(h);
    }
  }

  void endLuminosityBlockProduce(edm::LuminosityBlock & lumi, edm::EventSetup const & es) final {
    auto refs = std::vector<edm::Handle<MonitorElementCollection>>();
    lumi.getManyByType(refs);
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
    auto refs = std::vector<edm::Handle<MonitorElementCollection>>();
    run.getManyByType(refs);
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
