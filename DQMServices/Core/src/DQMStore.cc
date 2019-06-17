#include "DQMServices/Core/interface/DQMStore.h"
#include <string>
#include <regex>

namespace dqm {

  namespace legacy {
    IBooker::IBooker() {}
    IBooker::~IBooker() {}
    IGetter::IGetter() {}
    IGetter::~IGetter() {}

  }  // namespace legacy

  namespace implementation {

    std::string const& NavigatorBase::pwd() { return cwd_; }
    void NavigatorBase::cd() { setCurrentFolder(""); }
    void NavigatorBase::cd(std::string const& dir) { setCurrentFolder(cwd_ + dir); }
    void NavigatorBase::goUp() { cd(".."); }
    void NavigatorBase::setCurrentFolder(std::string const& fullpath) {
      // clean up the path and normalize it to preserve certain invariants.
      // this is the only method that should set cwd_.
      // Instead of reasoning about whatever properties of paths, we just parse
      // the thing and build a normalized instance with no slash in the beginning
      // and a slash in the end.
      std::string in(fullpath);
      std::vector<std::string> buf;
      std::regex dir("^/*([^/]+)");
      std::smatch m;
      while (std::regex_search(in, m, dir)) {
        in = m.suffix().str();
        if (m[0] == "..") {
          if (!buf.empty()) {
            buf.pop_back();
          }
        } else {
          buf.push_back(m[0]);
        }
      }
      std::string normalized;
      for (auto& s : buf) {
        normalized += s;
        normalized += "/";
      }
      cwd_ = normalized;
    }

    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookME(TString const& name, MonitorElementData::Kind kind, TH1* object) {
      MonitorElementData* data = new MonitorElementData();
      MonitorElementData::Key key;
      key.kind_ = kind;
      key.objname_ = std::string(name.View());
      key.dirname_ = pwd();
      key.scope_ = MonitorElementData::Scope::DEFAULT;
      data->key_ = key;
      data->object_ = std::unique_ptr<TH1>(object);

      std::unique_ptr<ME> me = std::make_unique<ME>(data);
      ME* me_ptr = store_->putME(std::move(me));
      return me_ptr;
    }

    template <class ME>
    ME* DQMStore<ME>::putME(std::unique_ptr<ME> && me) {
      if(master_ != nullptr) {
        // We have a master, so we simply forward the ME to there.
        auto lock = std::scoped_lock(*masterlock_);
        ME* me_ptr = master_->putME(std::move(me));

        // Make a copy of ME sharing the underlying MonitorElementData and root TH1 object
        localmes_[me_ptr->internal()->key_] = std::make_unique<ME>(*me_ptr);

        return localmes_[me_ptr->internal()->key_].get();
      }

      auto& existing = localmes_[me->internal()->key_];

      // existing = nullptr;
      // existing.asdasdasd();

      if(existing != nullptr) {
        // TODO: Check monitor element compatibility
        ME::checkCompatibility(me.get(), existing.get());

        // Delete previously created ME because we have to use the existing one
        me = nullptr;

        return existing.get();
      }
      else {
        localmes_[me->internal()->key_] = std::move(me);
        return me.get();
      }
    }

    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookInt(TString const& name) {
      return bookME(name, MonitorElementData::Kind::INT, nullptr);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookFloat(TString const& name) {
      return bookME(name, MonitorElementData::Kind::INT, nullptr);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookString(TString const& name, TString const& value) {
      return bookME(name, MonitorElementData::Kind::INT, nullptr);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1D(
        TString const& name, TString const& title, int const nchX, double const lowX, double const highX) {
      auto th1 = new TH1F(name, title, nchX, lowX, highX);
      return bookME(name, MonitorElementData::Kind::TH1F, th1);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1D(TString const& name, TString const& title, int nchX, float const* xbinsize) {
      auto th1 = new TH1F(name, title, nchX, xbinsize);
      return bookME(name, MonitorElementData::Kind::TH1F, th1);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1D(TString const& name, TH1F* object) {
      auto th1 = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TH1F, th1);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1S(TString const& name, TString const& title, int nchX, double lowX, double highX) {
      auto th1 = new TH1S(name, title, nchX, lowX, highX);
      return bookME(name, MonitorElementData::Kind::TH1S, th1);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1S(TString const& name, TH1S* object) {
      auto th1 = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TH1S, th1);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1DD(TString const& name, TString const& title, int nchX, double lowX, double highX) {
      auto th1 = new TH1D(name, title, nchX, lowX, highX);
      return bookME(name, MonitorElementData::Kind::TH1D, th1);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1DD(TString const& name, TH1D* object) {
      auto th1 = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TH1D, th1);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2D(TString const& name,
                                   TString const& title,
                                   int nchX,
                                   double lowX,
                                   double highX,
                                   int nchY,
                                   double lowY,
                                   double highY) {
      auto th2 = new TH2F(name, title, nchX, lowX, highX, nchY, lowY, highY);
      return bookME(name, MonitorElementData::Kind::TH2F, th2);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2D(
        TString const& name, TString const& title, int nchX, float const* xbinsize, int nchY, float const* ybinsize) {
      auto th2 = new TH2F(name, title, nchX, xbinsize, nchY, ybinsize);
      return bookME(name, MonitorElementData::Kind::TH2F, th2);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2D(TString const& name, TH2F* object) {
      auto th2 = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TH2F, th2);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2S(TString const& name,
                                   TString const& title,
                                   int nchX,
                                   double lowX,
                                   double highX,
                                   int nchY,
                                   double lowY,
                                   double highY) {
      auto th2 = new TH2S(name, title, nchX, lowX, highX, nchY, lowY, highY);
      return bookME(name, MonitorElementData::Kind::TH2S, th2);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2S(
        TString const& name, TString const& title, int nchX, float const* xbinsize, int nchY, float const* ybinsize) {
      auto th2 = new TH2S(name, title, nchX, xbinsize, nchY, ybinsize);
      return bookME(name, MonitorElementData::Kind::TH2S, th2);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2S(TString const& name, TH2S* object) {
      auto th2 = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TH2S, th2);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2DD(TString const& name,
                                    TString const& title,
                                    int nchX,
                                    double lowX,
                                    double highX,
                                    int nchY,
                                    double lowY,
                                    double highY) {
      auto th2 = new TH2D(name, title, nchX, lowX, highX, nchY, lowY, highY);
      return bookME(name, MonitorElementData::Kind::TH2D, th2);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2DD(TString const& name, TH2D* object) {
      auto th2 = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TH2D, th2);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book3D(TString const& name,
                                   TString const& title,
                                   int nchX,
                                   double lowX,
                                   double highX,
                                   int nchY,
                                   double lowY,
                                   double highY,
                                   int nchZ,
                                   double lowZ,
                                   double highZ) {
      auto th3 = new TH3F(name, title, nchX, lowX, highX, nchY, lowY, highY, nchZ, lowZ, highZ);
      return bookME(name, MonitorElementData::Kind::TH3F, th3);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book3D(TString const& name, TH3F* object) {
      auto th3 = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TH3F, th3);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name,
                                        TString const& title,
                                        int nchX,
                                        double lowX,
                                        double highX,
                                        int /* nchY */,
                                        double lowY,
                                        double highY,
                                        char const* option) {
      auto tprofile = new TProfile(name, title, nchX, lowX, highX, lowY, highY, option);
      return bookME(name, MonitorElementData::Kind::TPROFILE, tprofile);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name,
                                        TString const& title,
                                        int nchX,
                                        double lowX,
                                        double highX,
                                        double lowY,
                                        double highY,
                                        char const* option) {
      auto tprofile = new TProfile(name, title, nchX, lowX, highX, lowY, highY, option);
      return bookME(name, MonitorElementData::Kind::TPROFILE, tprofile);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name,
                                        TString const& title,
                                        int nchX,
                                        double const* xbinsize,
                                        int /* nchY */,
                                        double lowY,
                                        double highY,
                                        char const* option) {
      auto tprofile = new TProfile(name, title, nchX, xbinsize, lowY, highY, option);
      return bookME(name, MonitorElementData::Kind::TPROFILE, tprofile);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name,
                                        TString const& title,
                                        int nchX,
                                        double const* xbinsize,
                                        double lowY,
                                        double highY,
                                        char const* option) {
      auto tprofile = new TProfile(name, title, nchX, xbinsize, lowY, highY, option);
      return bookME(name, MonitorElementData::Kind::TPROFILE, tprofile);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name, TProfile* object) {
      auto tprofile = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TPROFILE, tprofile);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile2D(TString const& name,
                                          TString const& title,
                                          int nchX,
                                          double lowX,
                                          double highX,
                                          int nchY,
                                          double lowY,
                                          double highY,
                                          double lowZ,
                                          double highZ,
                                          char const* option) {
      auto tprofile = new TProfile2D(name, title, nchX, lowX, highX, nchY, lowY, highY, lowZ, highZ, option);
      return bookME(name, MonitorElementData::Kind::TPROFILE2D, tprofile);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile2D(TString const& name,
                                          TString const& title,
                                          int nchX,
                                          double lowX,
                                          double highX,
                                          int nchY,
                                          double lowY,
                                          double highY,
                                          int /* nchZ */,
                                          double lowZ,
                                          double highZ,
                                          char const* option) {
      auto tprofile = new TProfile2D(name, title, nchX, lowX, highX, nchY, lowY, highY, lowZ, highZ, option);
      return bookME(name, MonitorElementData::Kind::TPROFILE2D, tprofile);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile2D(TString const& name, TProfile2D* object) {
      auto tprofile = static_cast<TH1*>(object->Clone(name));
      return bookME(name, MonitorElementData::Kind::TPROFILE2D, tprofile);
    }

    template <class ME, class STORE>
    IBooker<ME, STORE>::IBooker(STORE* store) {
      store_ = store;
    }

    template <class ME, class STORE>
    std::vector<dqm::harvesting::MonitorElement*> IGetter<ME, STORE>::getContents(std::string const& path) const {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    std::vector<dqm::harvesting::MonitorElement*> IGetter<ME, STORE>::getContents(std::string const& path,
                                                                                  unsigned int tag) const {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    void IGetter<ME, STORE>::getContents(std::vector<std::string>& into, bool showContents) const {
      assert(!"NIY");
    }

    template <class ME, class STORE>
    void IGetter<ME, STORE>::removeContents() {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    void IGetter<ME, STORE>::removeContents(std::string const& dir) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    void IGetter<ME, STORE>::removeElement(std::string const& name) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    void IGetter<ME, STORE>::removeElement(std::string const& dir, std::string const& name, bool warning) {
      assert(!"NIY");
    }

    template <class ME, class STORE>
    std::vector<dqm::harvesting::MonitorElement*> IGetter<ME, STORE>::getAllContents(std::string const& path) const {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    std::vector<dqm::harvesting::MonitorElement*> IGetter<ME, STORE>::getAllContents(std::string const& path,
                                                                                     uint32_t runNumber,
                                                                                     uint32_t lumi) const {
      assert(!"NIY");
    }

    template <class ME, class STORE>
    ME* IGetter<ME, STORE>::get(std::string const& path) const {
      assert(!"NIY");
    }

    template <class ME, class STORE>
    ME* IGetter<ME, STORE>::getElement(std::string const& path) const {
      assert(!"NIY");
    }

    template <class ME, class STORE>
    std::vector<std::string> IGetter<ME, STORE>::getSubdirs() const {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    std::vector<std::string> IGetter<ME, STORE>::getMEs() const {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    bool IGetter<ME, STORE>::dirExists(std::string const& path) const {
      assert(!"NIY");
    }

    template <class ME, class STORE>
    IGetter<ME, STORE>::IGetter(STORE* store) {
      store_ = store;
    }

    template <class ME>
    DQMStore<ME>::DQMStore() : IGetter<ME, DQMStore<ME>>(this), IBooker<ME, DQMStore<ME>>(this) {}
    template <class ME>
    DQMStore<ME>::~DQMStore() {}

    template <class ME>
    void DQMStore<ME>::setVerbose(unsigned level) {
      assert(!"NIY");
    }

    template <class ME>
    void DQMStore<ME>::softReset(dqm::legacy::MonitorElement* me) {
      assert(!"NIY");
    }
    template <class ME>
    void DQMStore<ME>::disableSoftReset(dqm::legacy::MonitorElement* me) {
      assert(!"NIY");
    }

    template <class ME>
    void DQMStore<ME>::rmdir(std::string const& fullpath) {
      assert(!"NIY");
    }

    template <class ME>
    void DQMStore<ME>::save(std::string const& filename,
                            std::string const& path,
                            std::string const& pattern,
                            std::string const& rewrite,
                            uint32_t run,
                            uint32_t lumi,
                            SaveReferenceTag ref,
                            int minStatus,
                            std::string const& fileupdate) {
      assert(!"NIY");
    }
    template <class ME>
    void DQMStore<ME>::savePB(std::string const& filename, std::string const& path, uint32_t run, uint32_t lumi) {
      assert(!"NIY");
    }
    template <class ME>
    bool DQMStore<ME>::open(std::string const& filename,
                            bool overwrite,
                            std::string const& path,
                            std::string const& prepend,
                            OpenRunDirs stripdirs,
                            bool fileMustExist) {
      assert(!"NIY");
    }
    template <class ME>
    bool DQMStore<ME>::load(std::string const& filename, OpenRunDirs stripdirs, bool fileMustExist) {
      assert(!"NIY");
    }

    template <class ME>
    void DQMStore<ME>::showDirStructure() const {
      assert(!"NIY");
    }

    template <class ME>
    QCriterion* DQMStore<ME>::getQCriterion(std::string const& qtname) const {
      assert(!"NIY");
    }
    template <class ME>
    QCriterion* DQMStore<ME>::createQTest(std::string const& algoname, std::string const& qtname) {
      assert(!"NIY");
    }
    template <class ME>
    void DQMStore<ME>::useQTest(std::string const& dir, std::string const& qtname) {
      assert(!"NIY");
    }
    template <class ME>
    int DQMStore<ME>::useQTestByMatch(std::string const& pattern, std::string const& qtname) {
      assert(!"NIY");
    }
    template <class ME>
    void DQMStore<ME>::runQTests() {
      assert(!"NIY");
    }
    template <class ME>
    int DQMStore<ME>::getStatus(std::string const& path) const {
      assert(!"NIY");
    }
    template <class ME>
    void DQMStore<ME>::scaleElements() {
      assert(!"NIY");
    }

    template <class ME>
    std::vector<ME*> DQMStore<ME>::getMatchingContents(std::string const& pattern) const {
      assert(!"NIY");
    }

    template <class ME>
    MonitorElementCollection DQMStore<ME>::toProduct(edm::Transition t, edm::RunNumber_t run, edm::LuminosityBlockNumber_t lumi) {
      // TODO: removed only to make things compile.
      // The logic will have to be addressed
      // if(t != edm::Transition::EndRun && t != edm::Transition::EndLuminosityBlock) {
      //   assert(!"toProduct called on non end event transition");
      // }

      // auto check = [t, run, lumi](MonitorElementData::Key key) {
      //   if(t == edm::Transition::EndRun) {
      //     // Take only run into consideration
      //     auto endRun = std::get<5>(key);
      //     return run == endRun;
      //   }
      //   else if(t == edm::Transition::EndLuminosityBlock) {
      //     // Take run and lumi into consideration
      //     auto endRun = std::get<5>(key);
      //     auto endLuminosityBlock = std::get<6>(key);
      //     return run == endRun && lumi == endLuminosityBlock;
      //   }

      //   return false;
      // };

      // MonitorElementCollection product;

      // // TODO; use lower_bound here
      // auto it = localmes_.begin();
      // while (it != localmes_.end()) {
      //   if(check(it->first)) {
      //     MonitorElementData const* medata = it->second->internal();
      //     MonitorElementData outdata = *medata;
          
      //     if (t == edm::Transition::EndRun) {
      //       // In case of endRun, we remove the ME. In case there is another 
      //       // run, we will runn the booking again, which will create new MEs.
      //       // All subystem code should be fine with the ME*s changing between
      //       // runs.
      //       outdata.object_ = it->second->release();
      //       it = localmes_.erase(it);
      //     } else {
      //       // For not-per-run MEs, we cannot rely on re-booking. Instead, we
      //       // perform a clone here and reset the original. On the beginning of
      //       // the next lumi, we will check for reusable objects and reuse the
      //       // MEs.
      //       outdata.object_ =  (TH1*) medata->object_->Clone();
      //       auto meptr = it->second;
      //       it = localmes_.erase(it);
      //       // Now recycle the old ME, reset all data, and put it back.
      //       MonitorElementData newdata = *meptr->internal();
      //       newdata.coveredrange_ = edm::LuminosityBlockRange{};
      //       newdata.scalar_ = MonitorElementData::Scalar{};
      //       if (newdata.object_) newdata.object_->Reset();
      //       auto newme = std::make_shared<ME>(newdata);
      //       // TODO: we could have key collisions here, if we have reset two
      //       // (concurrent) sets of lumi MEs.
      //       // TODO: check that this does not invalidate iterators.
      //       localmes_[newdata.key()] = newme;
      //     }

      //     product.push_back(outdata);
      //   }
      // }
      // return product;

      assert(!"toProduct called with run number and/or lumi that are not present in DQMStore");
    }

    template <class ME>
    void DQMStore<ME>::registerProduct(edm::Handle<MonitorElementCollection>  mes) {
      if (!mes.isValid()) return;

      for (auto h : inputs_) {
        if (h.isValid()) {
          if (h.product() == mes.product()) {
            // we already know this product.
            return;
          } else {
            // product not valid, we might drop it. (TODO)
            // Can this happen if e.g. Lumi products go out of scope?
          }
        }
      }

      inputs_.push_back(mes);
    }

  }  // namespace implementation
}  // namespace dqm

template class dqm::implementation::DQMStore<dqm::legacy::MonitorElement>;
template class dqm::implementation::DQMStore<dqm::reco::MonitorElement>;
template class dqm::implementation::DQMStore<dqm::harvesting::MonitorElement>;

template class dqm::implementation::IGetter<dqm::legacy::MonitorElement,
                                            dqm::implementation::DQMStore<dqm::legacy::MonitorElement>>;
template class dqm::implementation::IGetter<dqm::reco::MonitorElement,
                                            dqm::implementation::DQMStore<dqm::reco::MonitorElement>>;
template class dqm::implementation::IGetter<dqm::harvesting::MonitorElement,
                                            dqm::implementation::DQMStore<dqm::harvesting::MonitorElement>>;

template class dqm::implementation::IBooker<dqm::legacy::MonitorElement,
                                            dqm::implementation::DQMStore<dqm::legacy::MonitorElement>>;
template class dqm::implementation::IBooker<dqm::reco::MonitorElement,
                                            dqm::implementation::DQMStore<dqm::reco::MonitorElement>>;
template class dqm::implementation::IBooker<dqm::harvesting::MonitorElement,
                                            dqm::implementation::DQMStore<dqm::harvesting::MonitorElement>>;
