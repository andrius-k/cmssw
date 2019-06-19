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
      MonitorElementData::Value::Access value(data->value_);
      key.kind_ = kind;
      key.objname_ = std::string(name.View());
      key.dirname_ = pwd();
      key.scope_ = MonitorElementData::Scope::DEFAULT;
      value.object = std::unique_ptr<TH1>(object);
      data->key_ = key;

      std::unique_ptr<ME> me = std::make_unique<ME>(data);
      ME* me_ptr = store_->putME(std::move(me));
      return me_ptr;
    }

    template <class ME>
    ME* DQMStore<ME>::putME(std::unique_ptr<ME>&& me) {
      if (master_ != nullptr) {
        // We have a master, so we simply forward the ME to there.
        auto lock = std::scoped_lock(*masterlock_);
        ME* me_ptr = master_->putME(std::move(me));

        // Make a copy of ME sharing the underlying MonitorElementData and root TH1 object
        localmes_[me_ptr->internal()->key_] = std::make_unique<ME>(*me_ptr);

        return localmes_[me_ptr->internal()->key_].get();
      }

      auto& existing = localmes_[me->internal()->key_];

      if (existing != nullptr) {
        // TODO: Check monitor element compatibility
        ME::checkCompatibility(me.get(), existing.get());

        // Delete previously created ME because we have to use the existing one
        me = nullptr;

        return existing.get();
      } else {
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
    void IBooker<ME, STORE>::tag(dqm::legacy::MonitorElement*, unsigned int) {
      assert(!"No longer supported.");
    }
    template <class ME, class STORE>
    void IBooker<ME, STORE>::tagContents(std::string const&, unsigned int) {
      assert(!"No longer supported.");
    }

    template <class ME>
    void DQMStore<ME>::enterLumi(edm::RunNumber_t run, edm::LuminosityBlockNumber_t lumi) {
      auto updaterange = [run, lumi](MonitorElementData::Key& key, bool& is_full) {
        // Check scope and extend covered range as needed.
        // Set is_full if the ME needs to be cloned.
        // Check run/lumi range is unset, as created by booking or
        // when cloned in toProduct(). Then, is_full = false.
        auto& range = key.coveredrange_;
        auto beginrun = range.startRun();
        auto endrun = range.endRun();
        auto beginlumi = range.startLumi();
        auto endlumi = range.endLumi();

        if (beginrun == edm::invalidRunNumber && endrun == edm::invalidRunNumber &&
            beginlumi == edm::invalidLuminosityBlockNumber && endlumi == edm::invalidLuminosityBlockNumber) {
          // this is a prototype, we can simply use it.
          key.coveredrange_ = edm::LuminosityBlockRange(run, lumi, run, lumi);
          is_full = false;
          return;
        }
        if (key.scope_ == MonitorElementData::Scope::RUN) {
          if (beginrun == run && endrun == run) {
            key.coveredrange_ = edm::LuminosityBlockRange(run, std::min(lumi, beginlumi), run, std::max(lumi, endlumi));
            is_full = false;
            return;
          } else {
            key.coveredrange_ = edm::LuminosityBlockRange(run, lumi, run, lumi);
            is_full = true;
            return;
          }
        } else if (key.scope_ == MonitorElementData::Scope::LUMI) {
          if (beginrun == run && endrun == run) {
            if (beginlumi == lumi && endlumi == lumi) {
              is_full = false;
              return;
            }
          }
          key.coveredrange_ = edm::LuminosityBlockRange(run, lumi, run, lumi);
          is_full = true;
          return;
        } else {
          assert(!"NIY");
        }
      };

      if (master_ != nullptr) {
        // We have a master, so we forward the call there first.
        auto lock = std::scoped_lock(*masterlock_);
        master_->enterLumi(run, lumi);

        // Now, for each local ME we look into the master to get the matching
        // MEData. This should always give us a good ME to use.
        std::map<MonitorElementData::Key, std::unique_ptr<ME>> newmes;
        for (auto& it : localmes_) {
          // take ME out of localmes
          std::unique_ptr<ME> me;
          me.swap(it.second);

          // create updated key
          MonitorElementData::Key key = it.first;
          bool unused_isfull;
          updaterange(key, unused_isfull);

          // find matching data
          auto& masterme = master_->localmes_[key];
          assert(masterme || !"Master does not have the ME we need.");

          // update ME and store it
          me->setInternal(masterme->internal());
          assert(!(key < me->internal()->key_) && !(me->internal()->key_ < key));
          newmes[key].swap(me);
        }
        // use newmes, localmes should be only nullptrs now and will be destroyed.
        localmes_.swap(newmes);
        return;
      }

      // Update and clone MEs that we own.
      // The rules are:
      // - There is at least one ME for each path/name
      // - If no ME for a given path/name is in use, there is one with run/lumi=0
      // - All MEs with run/lumi set are currently being filled.
      std::map<MonitorElementData::Key, std::unique_ptr<ME>> newmes;
      for (auto& it : localmes_) {
        // take ME out of localmes
        std::unique_ptr<ME> me;
        me.swap(it.second);

        // create updated key
        MonitorElementData::Key key = it.first;
        bool is_full;
        updaterange(key, is_full);

        // clone if needed
        if (is_full) {
          MonitorElementData* clone = cloneMonitorElementData(me->internal());
          // put back the old ME, we still need it
          newmes[key].swap(me);
          // ... and use the clone.
          auto cloneme = std::make_unique<ME>(clone);
          cloneme->Reset();
          me.swap(cloneme);
        }

        // update the key. This is not save in general, so const_cast is almost
        // appropriate.
        const_cast<MonitorElementData*>(me->internal())->key_ = key;
        // ... and save into the new map.
        newmes[key].swap(me);
      }

      localmes_.swap(newmes);
    }

    template <class ME>
    MonitorElementCollection DQMStore<ME>::toProduct(edm::Transition t,
                                                     edm::RunNumber_t run,
                                                     edm::LuminosityBlockNumber_t lumi) {
      MonitorElementCollection product;  // A product to return

      for (auto it = localmes_.begin(); it != localmes_.end(); it++) {
        // Sanity checks
        if (t == edm::Transition::EndRun) {
          if (it->first.coveredrange_.startRun() != it->first.coveredrange_.endRun()) {
            assert(!"toProduct(): Found per run ME with non matching startRun and endRun");
          }
        } else if (t == edm::Transition::EndLuminosityBlock) {
          if (it->first.coveredrange_.startRun() != it->first.coveredrange_.endRun()) {
            assert(!"toProduct(): Found per lumi ME with non matching startRun and endRun");
          } else if (it->first.coveredrange_.startLumi() != it->first.coveredrange_.endLumi()) {
            assert(!"toProduct(): Found per lumi ME with non matching startLumi and endLumi");
          }
        } else {
          assert(!"toProduct(): Called on non event/lumi end transition");
        }

        // During Transition::EndRun we only care about Scope::RUN
        // During Transition::EndLumi we only care about Scope::LUMI
        // During Transition::EndRun we ignore lumisection numbers
        bool scopeCorrespondsToTransition = false;
        bool runNumbersMatch = false;
        bool lumiNumbersMatch = false;
        if (t == edm::Transition::EndRun) {
          scopeCorrespondsToTransition = (it->first.scope_ == ME::Scope::RUN);
          runNumbersMatch = (it->first.coveredrange_.endRun() == run);
          lumiNumbersMatch = true;
        } else if (t == edm::Transition::EndLuminosityBlock) {
          scopeCorrespondsToTransition = (it->first.scope_ == ME::Scope::LUMI);
          runNumbersMatch = (it->first.coveredrange_.endRun() == run);
          lumiNumbersMatch = (it->first.coveredrange_.endLumi() == lumi);
        }

        if (scopeCorrespondsToTransition && runNumbersMatch && lumiNumbersMatch) {
          std::unique_ptr<ME> localMe = nullptr;
          // After a swap the map contains a null pointer
          it->second.swap(localMe);

          // Erase null pointer from the map
          it = localmes_.erase(it);

          product.push_back(std::unique_ptr<const MonitorElementData>(localMe->internal()));

          // We have to create a prototype MonitorElementData for potential
          // upcoming lumisection only if there is no other (currently being filled)
          // ME. If there is, it will be turned into a prototype once it's done.
          // Since map is an ordered container and key starts with the path/name of the plot,
          // we only have to look into consecutive elements to determine if there is another
          // ME with the same path/name. Next element is returned by the erase() call so we
          // check that as well as `it - 1`.
          bool existsAbove = it != localmes_.end() && it->second->getFullname() == localMe->getFullname();
          bool existsBelow = it != localmes_.begin() && std::prev(it)->second->getFullname() == localMe->getFullname();
          if (existsAbove || existsBelow) {
            // Clone and reset for potential reuse in the next lumi
            MonitorElementData* clone = cloneMonitorElementData(localMe->internal());
            clone->key_.coveredrange_ = edm::LuminosityBlockRange();

            localMe->setInternal(clone);
            localmes_[localMe->internal()->key_].swap(localMe);
          }
        }
      }

      return product;
    }

    template <class ME>
    void DQMStore<ME>::registerProduct(edm::Handle<MonitorElementCollection> mes) {
      if (!mes.isValid())
        return;

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
