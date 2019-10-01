// -*- C++ -*-
//
// Package:     FwkIO
// Class  :     DQMRootSource
//
// Implementation:
//     [Notes on implementation]
//
// Original Author:  Chris Jones
//         Created:  Tue May  3 11:13:47 CDT 2011
//

// system include files
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <list>
#include <set>
#include "TFile.h"
#include "TTree.h"
#include "TString.h"
#include "TH1.h"
#include "TH2.h"
#include "TProfile.h"

// user include files
#include "FWCore/Framework/interface/InputSource.h"
#include "FWCore/Sources/interface/PuttableSourceBase.h"
#include "FWCore/Catalog/interface/InputFileCatalog.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "DQMServices/Core/interface/DQMStore.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/MessageLogger/interface/JobReport.h"
//#include "FWCore/Utilities/interface/GlobalIdentifier.h"
#include "FWCore/Utilities/interface/EDMException.h"
#include "FWCore/Utilities/interface/ExceptionPropagate.h"

#include "FWCore/Framework/interface/RunPrincipal.h"
#include "FWCore/Framework/interface/LuminosityBlockPrincipal.h"

#include "FWCore/Framework/interface/Run.h"
#include "FWCore/Framework/interface/LuminosityBlock.h"
#include "DataFormats/Provenance/interface/LuminosityBlockID.h"
#include "DataFormats/Provenance/interface/LuminosityBlockRange.h"

#include "FWCore/ParameterSet/interface/ConfigurationDescriptions.h"
#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"

#include "FWCore/Framework/interface/InputSourceMacros.h"
#include "FWCore/Framework/interface/FileBlock.h"
#include "DataFormats/Provenance/interface/EventRange.h"
#include "DataFormats/Provenance/interface/EventID.h"

#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/Utilities/interface/TimeOfDay.h"

#include "DataFormats/Provenance/interface/ProcessHistory.h"
#include "DataFormats/Provenance/interface/ProcessHistoryID.h"
#include "DataFormats/Provenance/interface/ProcessHistoryRegistry.h"
#include "FWCore/ParameterSet/interface/Registry.h"

#include "FWCore/Utilities/interface/Digest.h"
#include "FWCore/Utilities/interface/InputType.h"

#include "format.h"

namespace {
  typedef dqm::harvesting::MonitorElement MonitorElement;
  typedef dqm::harvesting::DQMStore DQMStore;

  //adapter functions
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TH1F* iHist) {
    //std::cout <<"create: hist size "<<iName <<" "<<iHist->GetEffectiveEntries()<<std::endl;
    return iStore.book1D(iName, iHist);
  }
  //NOTE: the merge logic comes from DataFormats/Histograms/interface/MEtoEDMFormat.h
  void mergeTogether(TH1* iOriginal, TH1* iToAdd) {
    if (iOriginal->CanExtendAllAxes() && iToAdd->CanExtendAllAxes()) {
      TList list;
      list.Add(iToAdd);
      if (-1 == iOriginal->Merge(&list)) {
        edm::LogError("MergeFailure") << "Failed to merge DQM element " << iOriginal->GetName();
      }
    } else {
      /*
      // TODO: Redo.
      if (iOriginal->GetNbinsX() == iToAdd->GetNbinsX() &&
          iOriginal->GetXaxis()->GetXmin() == iToAdd->GetXaxis()->GetXmin() &&
          iOriginal->GetXaxis()->GetXmax() == iToAdd->GetXaxis()->GetXmax() &&
          iOriginal->GetNbinsY() == iToAdd->GetNbinsY() &&
          iOriginal->GetYaxis()->GetXmin() == iToAdd->GetYaxis()->GetXmin() &&
          iOriginal->GetYaxis()->GetXmax() == iToAdd->GetYaxis()->GetXmax() &&
          iOriginal->GetNbinsZ() == iToAdd->GetNbinsZ() &&
          iOriginal->GetZaxis()->GetXmin() == iToAdd->GetZaxis()->GetXmin() &&
          iOriginal->GetZaxis()->GetXmax() == iToAdd->GetZaxis()->GetXmax() &&
          MonitorElement::CheckBinLabels(iOriginal->GetXaxis(), iToAdd->GetXaxis()) &&
          MonitorElement::CheckBinLabels(iOriginal->GetYaxis(), iToAdd->GetYaxis()) &&
          MonitorElement::CheckBinLabels(iOriginal->GetZaxis(), iToAdd->GetZaxis())) {
        iOriginal->Add(iToAdd);
      } else {
        edm::LogError("MergeFailure") << "Found histograms with different axis limits or different labels'"
                                      << iOriginal->GetName() << "' not merged.";
      }
      */
    }
  }

  void mergeWithElement(MonitorElement* iElement, TH1F* iHist) {
    //std::cout <<"merge: hist size "<<iElement->getName() <<" "<<iHist->GetEffectiveEntries()<<std::endl;
    mergeTogether(iElement->getTH1F(), iHist);
  }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TH1S* iHist) {
    return iStore.book1S(iName, iHist);
  }
  void mergeWithElement(MonitorElement* iElement, TH1S* iHist) { mergeTogether(iElement->getTH1S(), iHist); }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TH1D* iHist) {
    return iStore.book1DD(iName, iHist);
  }
  void mergeWithElement(MonitorElement* iElement, TH1D* iHist) { mergeTogether(iElement->getTH1D(), iHist); }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TH2F* iHist) {
    return iStore.book2D(iName, iHist);
  }
  void mergeWithElement(MonitorElement* iElement, TH2F* iHist) { mergeTogether(iElement->getTH2F(), iHist); }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TH2S* iHist) {
    return iStore.book2S(iName, iHist);
  }
  void mergeWithElement(MonitorElement* iElement, TH2S* iHist) { mergeTogether(iElement->getTH2S(), iHist); }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TH2D* iHist) {
    return iStore.book2DD(iName, iHist);
  }
  void mergeWithElement(MonitorElement* iElement, TH2D* iHist) { mergeTogether(iElement->getTH2D(), iHist); }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TH3F* iHist) {
    return iStore.book3D(iName, iHist);
  }
  void mergeWithElement(MonitorElement* iElement, TH3F* iHist) { mergeTogether(iElement->getTH3F(), iHist); }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TProfile* iHist) {
    return iStore.bookProfile(iName, iHist);
  }
  void mergeWithElement(MonitorElement* iElement, TProfile* iHist) { mergeTogether(iElement->getTProfile(), iHist); }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, TProfile2D* iHist) {
    return iStore.bookProfile2D(iName, iHist);
  }
  void mergeWithElement(MonitorElement* iElement, TProfile2D* iHist) {
    mergeTogether(iElement->getTProfile2D(), iHist);
  }

  MonitorElement* createElement(DQMStore& iStore, const char* iName, Long64_t& iValue) {
    MonitorElement* e = iStore.bookInt(iName);
    e->Fill(iValue);
    return e;
  }

  //NOTE: the merge logic comes from DataFormats/Histograms/interface/MEtoEDMFormat.h
  void mergeWithElement(MonitorElement* iElement, Long64_t& iValue) {
    const std::string& name = iElement->getFullname();
    if (name.find("EventInfo/processedEvents") != std::string::npos) {
      iElement->Fill(iValue + iElement->getIntValue());
    } else if (name.find("EventInfo/iEvent") != std::string::npos ||
               name.find("EventInfo/iLumiSection") != std::string::npos) {
      if (iValue > iElement->getIntValue()) {
        iElement->Fill(iValue);
      }
    } else {
      iElement->Fill(iValue);
    }
  }

  MonitorElement* createElement(DQMStore& iStore, const char* iName, double& iValue) {
    MonitorElement* e = iStore.bookFloat(iName);
    e->Fill(iValue);
    return e;
  }
  void mergeWithElement(MonitorElement* iElement, double& iValue) {
    //no merging, take the last one
    iElement->Fill(iValue);
  }
  MonitorElement* createElement(DQMStore& iStore, const char* iName, std::string* iValue) {
    return iStore.bookString(iName, *iValue);
  }
  void mergeWithElement(MonitorElement* iElement, std::string* iValue) {
    //no merging, take the last one
    iElement->Fill(*iValue);
  }

  struct RunLumiToRange {
    unsigned int m_run, m_lumi, m_historyIDIndex;
    ULong64_t m_beginTime;
    ULong64_t m_endTime;
    ULong64_t m_firstIndex, m_lastIndex;  //last is inclusive
    unsigned int m_type;                  //A value in TypeIndex
  };

  using MonitorElementsFromFile = std::map<std::tuple<int, int>, std::vector<MonitorElementData*>>;

  class TreeReaderBase {
  public:
    TreeReaderBase() {}
    virtual ~TreeReaderBase() {}

    void read(ULong64_t iIndex, MonitorElementsFromFile& mesFromFile, bool iIsLumi, int run, int lumi) { doRead(iIndex, mesFromFile, iIsLumi, run, lumi); }
    virtual void setTree(TTree* iTree) = 0;

  protected:
    TTree* m_tree;

  private:
    virtual void doRead(ULong64_t iIndex, MonitorElementsFromFile& mesFromFile, bool iIsLumi, int run, int lumi) = 0;
  };

  template <class T>
  class TreeObjectReader : public TreeReaderBase {
  public:
    TreeObjectReader() : m_tree(nullptr), m_fullName(nullptr), m_buffer(nullptr), m_tag(0) {}
    
    void doRead(ULong64_t iIndex, MonitorElementsFromFile& mesFromFile, bool iIsLumi, int run, int lumi) override {
      m_tree->GetEntry(iIndex);

      // auto me = std::make_unique<dqm::harvesting::MonitorElement>(&meData, /* is_owned */ false, /* is_readonly */ true);

      MonitorElementData* meData = new MonitorElementData();
      MonitorElementData::Key key;
      // key.kind_ = kind; // TODO: make this work
      key.path_.set(*m_fullName, MonitorElementData::Path::Type::DIR_AND_NAME);
      key.scope_ = iIsLumi ? MonitorElementData::Scope::LUMI : MonitorElementData::Scope::RUN;
      meData->key_ = key;
      {
        MonitorElementData::Value::Access value(meData->value_);
        value.object = std::unique_ptr<T>((T*)(m_buffer->Clone()));
      }

      // std::unique_ptr<MonitorElementFromFile> meFromFile = std::unique_ptr<MonitorElementFromFile>(new MonitorElementFromFile());
      // MonitorElementFromFile meFromFile;
      // meFromFile.m_run = run;
      // meFromFile.m_lumi = lumi;
      // meFromFile.m_meData = meData;
      
      // mesFromFile.push_back(std::move(meFromFile));
      // mesFromFile.push_back(meFromFile);
      // std::map<tuple<int, int>, MonitorElementCollection>
      //mesFromFile[std::make_tuple(run, lumi)].push_back(meData);
      mesFromFile[std::make_tuple(run, lumi)].push_back(meData);

      
      // TRACE(*m_fullName)
    }
    void setTree(TTree* iTree) override {
      m_tree = iTree;
      m_tree->SetBranchAddress(kFullNameBranch, &m_fullName);
      m_tree->SetBranchAddress(kFlagBranch, &m_tag);
      m_tree->SetBranchAddress(kValueBranch, &m_buffer);
    }

  private:
    TTree* m_tree;
    std::string* m_fullName;
    T* m_buffer;
    uint32_t m_tag;
  };

  class TreeStringReader : public TreeReaderBase {
  public:
    TreeStringReader() : m_tree(nullptr), m_fullName(nullptr), m_value(nullptr), m_tag(0) {}
    
    void doRead(ULong64_t iIndex, MonitorElementsFromFile& mesFromFile, bool iIsLumi, int run, int lumi) override {
      return;
      m_tree->GetEntry(iIndex);

      // auto me = std::make_unique<dqm::harvesting::MonitorElement>(&meData, /* is_owned */ false, /* is_readonly */ true);

      // TODO: Fill all metadata to meData
      MonitorElementData* meData = new MonitorElementData();
      MonitorElementData::Key key;
      key.kind_ = MonitorElementData::Kind::STRING;
      key.path_.set(*m_fullName, MonitorElementData::Path::Type::DIR_AND_NAME);
      key.scope_ = iIsLumi ? MonitorElementData::Scope::LUMI : MonitorElementData::Scope::RUN;
      meData->key_ = key;
      {
        MonitorElementData::Value::Access value(meData->value_);
        value.scalar.str = *m_value;
      }

      // std::unique_ptr<MonitorElementFromFile> meFromFile = std::unique_ptr<MonitorElementFromFile>(new MonitorElementFromFile());
      // MonitorElementFromFile meFromFile;
      // meFromFile.m_run = run;
      // meFromFile.m_lumi = lumi;
      // meFromFile.m_meData = meData;

      // // mesFromFile.push_back(std::move(meFromFile));
      // mesFromFile.push_back(meFromFile);

      mesFromFile[std::make_tuple(run, lumi)].push_back(std::move(meData));
    }
    void setTree(TTree* iTree) override {
      m_tree = iTree;
      m_tree->SetBranchAddress(kFullNameBranch, &m_fullName);
      m_tree->SetBranchAddress(kFlagBranch, &m_tag);
      m_tree->SetBranchAddress(kValueBranch, &m_value);
    }

  private:
    TTree* m_tree;
    std::string* m_fullName;
    std::string* m_value;
    uint32_t m_tag;
  };

  template <class T>
  class TreeSimpleReader : public TreeReaderBase {
  public:
    TreeSimpleReader() : m_tree(nullptr), m_fullName(nullptr), m_buffer(0), m_tag(0) {}
    
    void doRead(ULong64_t iIndex, MonitorElementsFromFile& mesFromFile, bool iIsLumi, int run, int lumi) override {
      m_tree->GetEntry(iIndex);

      // TODO: Make this work

      // // auto me = std::make_unique<dqm::harvesting::MonitorElement>(&meData, /* is_owned */ false, /* is_readonly */ true);

      // MonitorElementData* data = new MonitorElementData();
      // MonitorElementData::Key key;
      // // key.kind_ = kind; // TODO: make this work
      // key.path_.set(*m_fullName, MonitorElementData::Path::Type::DIR_AND_NAME);
      // key.scope_ = iIsLumi ? MonitorElementData::Scope::LUMI : MonitorElementData::Scope::RUN;
      // data->key_ = key;
      // {
      //   MonitorElementData::Value::Access value(data->value_);
      //   value.object = std::unique_ptr<TH1>(m_buffer);
      // }

      // std::unique_ptr<MonitorElement> me = std::make_unique<MonitorElement>(data, /* is_owned */ true, /* is_readonly */ false);

      // std::unique_ptr<MonitorElementFromFile> meFromFile = std::make_unique<MonitorElementFromFile>();
      // meFromFile->m_run = run;
      // meFromFile->m_lumi = lumi;
      // meFromFile->m_element = me;
      
      // mesFromFile.push_back(meFromFile);
    }
    void setTree(TTree* iTree) override {
      m_tree = iTree;
      m_tree->SetBranchAddress(kFullNameBranch, &m_fullName);
      m_tree->SetBranchAddress(kFlagBranch, &m_tag);
      m_tree->SetBranchAddress(kValueBranch, &m_buffer);
    }

  private:
    TTree* m_tree;
    std::string* m_fullName;
    T m_buffer;
    uint32_t m_tag;
  };

}  // namespace

class DQMRootSource : public edm::PuttableSourceBase {
public:
  DQMRootSource(edm::ParameterSet const&, const edm::InputSourceDescription&);
  ~DQMRootSource() override;

  // ---------- const member functions ---------------------

  // ---------- static member functions --------------------

  // ---------- member functions ---------------------------
  static void fillDescriptions(edm::ConfigurationDescriptions& descriptions);

private:
  DQMRootSource(const DQMRootSource&) = delete;  // stop default

  class RunPHIDKey {
  public:
    RunPHIDKey(edm::ProcessHistoryID const& phid, unsigned int run) : processHistoryID_(phid), run_(run) {}
    edm::ProcessHistoryID const& processHistoryID() const { return processHistoryID_; }
    unsigned int run() const { return run_; }
    bool operator<(RunPHIDKey const& right) const {
      if (processHistoryID_ == right.processHistoryID()) {
        return run_ < right.run();
      }
      return processHistoryID_ < right.processHistoryID();
    }

  private:
    edm::ProcessHistoryID processHistoryID_;
    unsigned int run_;
  };

  class RunLumiPHIDKey {
  public:
    RunLumiPHIDKey(edm::ProcessHistoryID const& phid, unsigned int run, unsigned int lumi)
        : processHistoryID_(phid), run_(run), lumi_(lumi) {}
    edm::ProcessHistoryID const& processHistoryID() const { return processHistoryID_; }
    unsigned int run() const { return run_; }
    unsigned int lumi() const { return lumi_; }
    bool operator<(RunLumiPHIDKey const& right) const {
      if (processHistoryID_ == right.processHistoryID()) {
        if (run_ == right.run()) {
          return lumi_ < right.lumi();
        }
        return run_ < right.run();
      }
      return processHistoryID_ < right.processHistoryID();
    }

  private:
    edm::ProcessHistoryID processHistoryID_;
    unsigned int run_;
    unsigned int lumi_;
  };

  void beginRun(edm::Run& run) {
    TRACE(run.run())

    std::unique_ptr<MonitorElementCollection> product = std::make_unique<MonitorElementCollection>();

    auto mes = m_MEsFromFile[std::make_tuple(run.run(), 0)];

    TRACE(mes.size()) // run: 302472 count: 93228

    for(MonitorElementData* meData_ptr : mes) {
      // assert(meData_ptr != nullptr);
      // TRACE_
      // TRACE(meData_ptr)
      product->push_back(meData_ptr);
      // TRACE(meData_ptr)
    }
    // TRACE_

    // run.put(std::move(product), "DQMGenerationRecoRun");
    
    m_MEsFromFile[std::make_tuple(run.run(), 0)] = std::vector<MonitorElementData*>();




    // m_justOpenedFileSoNeedToGenerateRunTransition = false;

    // assert(m_presentIndexItr != m_orderedIndices.end());
    // RunLumiToRange runLumiRange = m_runlumiToRange[*m_presentIndexItr];
    
    // readNextItemType();

    // //NOTE: it is possible to have a Run when all we have stored is lumis
    // if (runLumiRange.m_lumi == 0) {
    //   readElements();
    // }


  }
  void beginLuminosityBlock(edm::LuminosityBlock& lumi) {
    TRACE(lumi.luminosityBlock())

    std::unique_ptr<MonitorElementCollection> product = std::make_unique<MonitorElementCollection>();

    auto mes = m_MEsFromFile[std::make_tuple(lumi.run(), lumi.luminosityBlock())];

    for(MonitorElementData* meData_ptr : mes) {
      assert(meData_ptr != nullptr);
      product->push_back(meData_ptr);
    }
    
    lumi.put(std::move(product), "DQMGenerationRecoLumi");






    // RunLumiToRange runLumiRange = m_runlumiToRange[*m_presentIndexItr];

    // // //NOTE: need to reset all lumi block elements at this point
    // if ((m_lastSeenLumi2 != runLumiRange.m_lumi || m_lastSeenRun2 != runLumiRange.m_run ||
    //     m_lastSeenReducedPHID2 != m_reducedHistoryIDs.at(runLumiRange.m_historyIDIndex)) &&
    //     m_shouldReadMEs) {
    //   m_lastSeenReducedPHID2 = m_reducedHistoryIDs.at(runLumiRange.m_historyIDIndex);
    //   m_lastSeenRun2 = runLumiRange.m_run;
    //   m_lastSeenLumi2 = runLumiRange.m_lumi;
    // }

    // assert(m_presentIndexItr != m_orderedIndices.end());

    // readNextItemType();
    // readElements();


  }

  void readElements(int run, int lumi) {
    RunLumiToRange runLumiRange = m_runlumiToRange[*m_presentIndexItr];
    bool shouldContinue = false;
    do {
      shouldContinue = false;
      ++m_presentIndexItr;
      while (m_presentIndexItr != m_orderedIndices.end() &&
            skipIt(m_runlumiToRange[*m_presentIndexItr].m_run, m_runlumiToRange[*m_presentIndexItr].m_lumi))
        ++m_presentIndexItr;

      if (runLumiRange.m_type != kNoTypesStored) {
        boost::shared_ptr<TreeReaderBase> reader = m_treeReaders[runLumiRange.m_type];
        ULong64_t index = runLumiRange.m_firstIndex;
        ULong64_t endIndex = runLumiRange.m_lastIndex + 1;
        for (; index != endIndex; ++index) {
          bool isLumi = runLumiRange.m_lumi != 0;
          if (m_shouldReadMEs)
            reader->read(index, m_MEsFromFile, isLumi, run, lumi);

          //std::cout << runLumiRange.m_run << " " << runLumiRange.m_lumi <<" "<<index<< " " << runLumiRange.m_type << std::endl;
        }
      }

      if (m_presentIndexItr != m_orderedIndices.end()) {
        //are there more parts to this same run/lumi?
        const RunLumiToRange nextRunLumiRange = m_runlumiToRange[*m_presentIndexItr];
        //continue to the next item if that item is either
        if ((m_reducedHistoryIDs.at(nextRunLumiRange.m_historyIDIndex) ==
            m_reducedHistoryIDs.at(runLumiRange.m_historyIDIndex)) &&
            (nextRunLumiRange.m_run == runLumiRange.m_run) && (nextRunLumiRange.m_lumi == runLumiRange.m_lumi)) {
          shouldContinue = true;
          runLumiRange = nextRunLumiRange;
        }
      }
    } while (shouldContinue);
  }

  edm::InputSource::ItemType getNextItemType() override;
  //NOTE: the following is really read next run auxiliary
  // What is run/lumi auxiliary? Why do we need them?
  std::shared_ptr<edm::RunAuxiliary> readRunAuxiliary_() override;
  std::shared_ptr<edm::LuminosityBlockAuxiliary> readLuminosityBlockAuxiliary_() override;
  void readRun_(edm::RunPrincipal& rpCache) override;
  void readLuminosityBlock_(edm::LuminosityBlockPrincipal& lbCache) override;
  void readEvent_(edm::EventPrincipal&) override;

  std::unique_ptr<edm::FileBlock> readFile_() override;
  void closeFile_() override;

  void logFileAction(char const* msg, char const* fileName) const;

  void readNextItemType();
  bool setupFile(unsigned int iIndex);
  void readElements();
  bool skipIt(edm::RunNumber_t, edm::LuminosityBlockNumber_t) const;

  const DQMRootSource& operator=(const DQMRootSource&) = delete;  // stop default

  // ---------- member data --------------------------------
  std::unique_ptr<DQMStore> m_store;
  edm::InputFileCatalog m_catalog;
  edm::RunAuxiliary m_runAux;
  edm::LuminosityBlockAuxiliary m_lumiAux;
  edm::InputSource::ItemType m_nextItemType;

  size_t m_fileIndex;
  size_t m_presentlyOpenFileIndex;
  std::list<unsigned int>::iterator m_nextIndexItr;
  std::list<unsigned int>::iterator m_presentIndexItr;
  std::vector<RunLumiToRange> m_runlumiToRange;
  std::unique_ptr<TFile> m_file;
  std::vector<TTree*> m_trees;
  std::vector<boost::shared_ptr<TreeReaderBase> > m_treeReaders;

  std::list<unsigned int> m_orderedIndices;
  edm::ProcessHistoryID m_lastSeenReducedPHID;
  unsigned int m_lastSeenRun;
  edm::ProcessHistoryID m_lastSeenReducedPHID2;
  unsigned int m_lastSeenRun2;
  unsigned int m_lastSeenLumi2;
  unsigned int m_filterOnRun;
  bool m_skipBadFiles;
  std::vector<edm::LuminosityBlockRange> m_lumisToProcess;
  std::vector<edm::RunNumber_t> m_runsToProcess;

  bool m_justOpenedFileSoNeedToGenerateRunTransition;
  bool m_shouldReadMEs;
  std::set<MonitorElement*> m_lumiElements;
  std::set<MonitorElement*> m_runElements;
  std::vector<edm::ProcessHistoryID> m_historyIDs;
  std::vector<edm::ProcessHistoryID> m_reducedHistoryIDs;

  edm::JobReport::Token m_jrToken;

  MonitorElementsFromFile m_MEsFromFile;
};

//
// constants, enums and typedefs
//

//
// static data member definitions
//

void DQMRootSource::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
  edm::ParameterSetDescription desc;
  desc.addUntracked<std::vector<std::string> >("fileNames")->setComment("Names of files to be processed.");
  desc.addUntracked<unsigned int>("filterOnRun", 0)->setComment("Just limit the process to the selected run.");
  desc.addUntracked<bool>("skipBadFiles", false)->setComment("Skip the file if it is not valid");
  desc.addUntracked<std::string>("overrideCatalog", std::string())
      ->setComment("An alternate file catalog to use instead of the standard site one.");
  std::vector<edm::LuminosityBlockRange> defaultLumis;
  desc.addUntracked<std::vector<edm::LuminosityBlockRange> >("lumisToProcess", defaultLumis)
      ->setComment("Skip any lumi inside the specified run:lumi range.");

  descriptions.addDefault(desc);
}
//
// constructors and destructor
//
DQMRootSource::DQMRootSource(edm::ParameterSet const& iPSet, const edm::InputSourceDescription& iDesc)
    : edm::PuttableSourceBase(iPSet, iDesc),
      m_store(std::make_unique<DQMStore>()),
      m_catalog(iPSet.getUntrackedParameter<std::vector<std::string> >("fileNames"),
                iPSet.getUntrackedParameter<std::string>("overrideCatalog")),
      m_nextItemType(edm::InputSource::IsFile),
      m_fileIndex(0),
      m_presentlyOpenFileIndex(0),
      m_trees(kNIndicies, static_cast<TTree*>(nullptr)),
      m_treeReaders(kNIndicies, boost::shared_ptr<TreeReaderBase>()),
      m_lastSeenReducedPHID(),
      m_lastSeenRun(0),
      m_lastSeenReducedPHID2(),
      m_lastSeenRun2(0),
      m_lastSeenLumi2(0),
      m_filterOnRun(iPSet.getUntrackedParameter<unsigned int>("filterOnRun", 0)),
      m_skipBadFiles(iPSet.getUntrackedParameter<bool>("skipBadFiles", false)),
      m_lumisToProcess(iPSet.getUntrackedParameter<std::vector<edm::LuminosityBlockRange> >(
          "lumisToProcess", std::vector<edm::LuminosityBlockRange>())),
      m_justOpenedFileSoNeedToGenerateRunTransition(false),
      m_shouldReadMEs(true),
      m_MEsFromFile(MonitorElementsFromFile()) {
  edm::sortAndRemoveOverlaps(m_lumisToProcess);
  for (std::vector<edm::LuminosityBlockRange>::const_iterator itr = m_lumisToProcess.begin();
       itr != m_lumisToProcess.end();
       ++itr) {
    m_runsToProcess.push_back(itr->startRun());
  }

  if (m_fileIndex == m_catalog.fileNames().size()) {
    m_nextItemType = edm::InputSource::IsStop;
  } else {
    m_treeReaders[kIntIndex].reset(new TreeSimpleReader<Long64_t>());
    m_treeReaders[kFloatIndex].reset(new TreeSimpleReader<double>());
    m_treeReaders[kStringIndex].reset(new TreeStringReader());
    m_treeReaders[kTH1FIndex].reset(new TreeObjectReader<TH1F>());
    m_treeReaders[kTH1SIndex].reset(new TreeObjectReader<TH1S>());
    m_treeReaders[kTH1DIndex].reset(new TreeObjectReader<TH1D>());
    m_treeReaders[kTH2FIndex].reset(new TreeObjectReader<TH2F>());
    m_treeReaders[kTH2SIndex].reset(new TreeObjectReader<TH2S>());
    m_treeReaders[kTH2DIndex].reset(new TreeObjectReader<TH2D>());
    m_treeReaders[kTH3FIndex].reset(new TreeObjectReader<TH3F>());
    m_treeReaders[kTProfileIndex].reset(new TreeObjectReader<TProfile>());
    m_treeReaders[kTProfile2DIndex].reset(new TreeObjectReader<TProfile2D>());
  }
  produces<MonitorElementCollection, edm::Transition::BeginRun>("DQMGenerationRecoRun");
  produces<MonitorElementCollection, edm::Transition::BeginLuminosityBlock>("DQMGenerationRecoLumi");
}

// DQMRootSource::DQMRootSource(const DQMootSource& rhs)
// {
//    // do actual copying here;
// }

DQMRootSource::~DQMRootSource() {
  // Close files but not sure if needed because we have closeFile_()
}

//
// member functions
//

void DQMRootSource::readEvent_(edm::EventPrincipal&) {
  //std::cout << "readEvent_" << std::endl;
}

edm::InputSource::ItemType DQMRootSource::getNextItemType() {
  return m_nextItemType;
}

std::shared_ptr<edm::RunAuxiliary> DQMRootSource::readRunAuxiliary_() {
  //std::cout <<"readRunAuxiliary_"<<std::endl;
  assert(m_nextIndexItr != m_orderedIndices.end());
  RunLumiToRange runLumiRange = m_runlumiToRange[*m_nextIndexItr];

  //NOTE: the setBeginTime and setEndTime functions of RunAuxiliary only work if the previous value was invalid
  // therefore we must copy
  m_runAux = edm::RunAuxiliary(
      runLumiRange.m_run, edm::Timestamp(runLumiRange.m_beginTime), edm::Timestamp(runLumiRange.m_endTime));
  assert(m_historyIDs.size() > runLumiRange.m_historyIDIndex);
  //std::cout <<"readRunAuxiliary_ "<<m_historyIDs[runLumiRange.m_historyIDIndex]<<std::endl;
  m_runAux.setProcessHistoryID(m_historyIDs[runLumiRange.m_historyIDIndex]);
  return std::make_shared<edm::RunAuxiliary>(m_runAux);
}

std::shared_ptr<edm::LuminosityBlockAuxiliary> DQMRootSource::readLuminosityBlockAuxiliary_() {
  //std::cout <<"readLuminosityBlockAuxiliary_"<<std::endl;
  assert(m_nextIndexItr != m_orderedIndices.end());
  const RunLumiToRange runLumiRange = m_runlumiToRange[*m_nextIndexItr];
  m_lumiAux = edm::LuminosityBlockAuxiliary(edm::LuminosityBlockID(runLumiRange.m_run, runLumiRange.m_lumi),
                                            edm::Timestamp(runLumiRange.m_beginTime),
                                            edm::Timestamp(runLumiRange.m_endTime));
  assert(m_historyIDs.size() > runLumiRange.m_historyIDIndex);
  //std::cout <<"lumi "<<m_lumiAux.beginTime().value()<<" "<<runLumiRange.m_beginTime<<std::endl;
  m_lumiAux.setProcessHistoryID(m_historyIDs[runLumiRange.m_historyIDIndex]);

  return std::make_shared<edm::LuminosityBlockAuxiliary>(m_lumiAux);
}

void DQMRootSource::readRun_(edm::RunPrincipal& rpCache) {
  // Read elements of a current run. 
  // There might be MEs of the same run in multiple files. These has to be merged
  // RunLumiToRange runLumiRange = m_runlumiToRange[*m_presentIndexItr];
  // if (runLumiRange.m_lumi == 0) {
    // readElements(runLumiRange.m_run, runLumiRange.m_lumi);
  // }
  // readNextItemType();
}

void DQMRootSource::readLuminosityBlock_(edm::LuminosityBlockPrincipal& lbCache) {
  // Read elements of a current lumi. 
  // There might be MEs of the same run and lumi in multiple files??? These has to be merged.
  // readElements(runLumiRange.m_run, runLumiRange.m_lumi);
  // readNextItemType();
}

std::unique_ptr<edm::FileBlock> DQMRootSource::readFile_() {
  // readNextItemType();
  // return std::unique_ptr<edm::FileBlock>(new edm::FileBlock);
  return nullptr;
}

void DQMRootSource::closeFile_() {
  // Close all opened files
}

void DQMRootSource::readNextItemType() {
  // Figure put what is the next item type
}

bool DQMRootSource::setupFile(unsigned int iIndex) {
  // Open files and make a list: (run, lumi, file pointer, )
  return true;
}

bool DQMRootSource::skipIt(edm::RunNumber_t run, edm::LuminosityBlockNumber_t lumi) const {
  if (!m_runsToProcess.empty() && edm::search_all(m_runsToProcess, run) && lumi == 0) {
    return false;
  }

  edm::LuminosityBlockID lumiID = edm::LuminosityBlockID(run, lumi);
  edm::LuminosityBlockRange lumiRange = edm::LuminosityBlockRange(lumiID, lumiID);
  bool (*lt)(edm::LuminosityBlockRange const&, edm::LuminosityBlockRange const&) = &edm::lessThan;
  if (!m_lumisToProcess.empty() && !binary_search_all(m_lumisToProcess, lumiRange, lt)) {
    return true;
  }
  return false;
}

void DQMRootSource::logFileAction(char const* msg, char const* fileName) const {
  edm::LogAbsolute("fileAction") << std::setprecision(0) << edm::TimeOfDay() << msg << fileName;
  edm::FlushMessageLog();
}

//
// const member functions
//

//
// static member functions
//
DEFINE_FWK_INPUT_SOURCE(DQMRootSource);
