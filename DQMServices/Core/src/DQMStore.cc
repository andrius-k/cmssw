#include "DQMServices/Core/interface/DQMStore.h"
#include <string>
#include <regex>

namespace dqm {

  namespace legacy {
    IBooker::IBooker() {}
    IBooker::~IBooker() {}
    IGetter::IGetter() {}
    IGetter::~IGetter() {}

    bool MonitorElement::checkCompatibility(MonitorElement const& a, MonitorElement const& b) {
      // TODO
      return true;
    }

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
      MonitorElementData data;
      data.kind_ = kind;
      data.object_ = object;
      data.objname_ = std::string(name.View());
      data.dirname_ = pwd();
      data.scope_ = MonitorElementData::DQM_SCOPE_DEFAULT;

      ME* me = store_->putME(data);
      return me;
    }

    template <class ME>
    ME* DQMStore<ME>::putME(MonitorElementData const& data) {
      auto& existing = localmes_[data.key()];
      auto newme = std::make_shared<ME>(data);
      if (existing) {
        bool ok = ME::checkCompatibility(*existing, *newme);
        if (!ok) {
          assert(!"Incompatible re-booking.");
          // in case we want to allow that, we probably want to replace the existing.
          existing = newme;
        } else {
          newme = existing;
        }
      } else {
        existing = newme;
      }
      // existing and newme are always the same now.
      // return a bare pointer, the object is owned by localmes in the end. We use
      // a shared pointer since other DQMStores (in case of edm::stream) might
      // co-own it. Also makes it easier to handle.
      return &*newme;
    }

    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookInt(TString const& name) {
      return bookME(name, MonitorElementData::DQM_KIND_INT, nullptr);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookFloat(TString const& name) {
      return bookME(name, MonitorElementData::DQM_KIND_INT, nullptr);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookString(TString const& name, TString const& value) {
      return bookME(name, MonitorElementData::DQM_KIND_INT, nullptr);
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1D(
        TString const& name, TString const& title, int const nchX, double const lowX, double const highX) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1D(TString const& name, TString const& title, int nchX, float const* xbinsize) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1D(TString const& name, TH1F* object) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1S(TString const& name, TString const& title, int nchX, double lowX, double highX) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1S(TString const& name, TH1S* object) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1DD(TString const& name, TString const& title, int nchX, double lowX, double highX) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book1DD(TString const& name, TH1D* object) {
      assert(!"NIY");
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
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2D(
        TString const& name, TString const& title, int nchX, float const* xbinsize, int nchY, float const* ybinsize) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2D(TString const& name, TH2F* object) {
      assert(!"NIY");
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
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2S(
        TString const& name, TString const& title, int nchX, float const* xbinsize, int nchY, float const* ybinsize) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2S(TString const& name, TH2S* object) {
      assert(!"NIY");
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
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book2DD(TString const& name, TH2D* object) {
      assert(!"NIY");
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
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::book3D(TString const& name, TH3F* object) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name,
                                        TString const& title,
                                        int nchX,
                                        double lowX,
                                        double highX,
                                        int nchY,
                                        double lowY,
                                        double highY,
                                        char const* option) {
      assert(!"NIY");
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
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name,
                                        TString const& title,
                                        int nchX,
                                        double const* xbinsize,
                                        int nchY,
                                        double lowY,
                                        double highY,
                                        char const* option) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name,
                                        TString const& title,
                                        int nchX,
                                        double const* xbinsize,
                                        double lowY,
                                        double highY,
                                        char const* option) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile(TString const& name, TProfile* object) {
      assert(!"NIY");
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
      assert(!"NIY");
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
                                          int nchZ,
                                          double lowZ,
                                          double highZ,
                                          char const* option) {
      assert(!"NIY");
    }
    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookProfile2D(TString const& name, TProfile2D* object) {
      assert(!"NIY");
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
