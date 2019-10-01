// silence deprecation warnings for the DQMStore itself.
#define DQM_DEPRECATED
#include "DQMServices/Core/interface/DQMStore.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include <string>
#include <regex>
#include <csignal>

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
      MonitorElementData::Path path;
      path.set(fullpath, MonitorElementData::Path::Type::DIR);
      assert(this);
      cwd_ = path.getDirname();
    }

    template <class ME, class STORE>
    IBooker<ME, STORE>::IBooker(STORE* store) {
      store_ = store;
      scope_ = MonitorElementData::Scope::DEFAULT;
    }

    template <class ME, class STORE>
    MonitorElementData::Scope IBooker<ME, STORE>::setScope(MonitorElementData::Scope newscope) {
      auto oldscope = scope_;
      scope_ = newscope;
      return oldscope;
    }

    template <class ME, class STORE>
    ME* IBooker<ME, STORE>::bookME(TString const& name, MonitorElementData::Kind kind, TH1* object) {
      MonitorElementData* data = new MonitorElementData();
      MonitorElementData::Key key;
      key.kind_ = kind;
      std::string fullpath = pwd() + std::string(name.View());
      key.path_.set(fullpath, MonitorElementData::Path::Type::DIR_AND_NAME);
      key.scope_ = scope_;
      data->key_ = key;
      {
        MonitorElementData::Value::Access value(data->value_);
        value.object = std::unique_ptr<TH1>(object);
      }

      std::unique_ptr<ME> me = std::make_unique<ME>(data, /* is_owned */ true, /* is_readonly */ false);
      assert(me);
      ME* me_ptr = store_->putME(std::move(me));
      assert(me_ptr);
      return me_ptr;
    }

    template <class ME>
    ME* DQMStore<ME>::putME(std::unique_ptr<ME>&& me) {
      if (master_ != nullptr) {
        // We have a master, so we simply forward the ME to there.
        auto lock = std::scoped_lock(master_->lock_);
        ME* me_ptr = master_->master_.putME(std::move(me));

        // Make a copy of ME sharing the underlying MonitorElementData and root TH1 object
        if (!localmes_[me_ptr->internal()->key_]) {
        localmes_[me_ptr->internal()->key_] =
            std::make_unique<ME>(me_ptr->internal(), /* is_owned */ false, /* is_readonly */ false);
        } else {
          // However, if we already have it (double booking), reuse the existing object.
          localmes_[me_ptr->internal()->key_]->setInternal(me_ptr->internal(), /* is_owned */ false, /* is_readonly */ false);
        }

        return localmes_[me_ptr->internal()->key_].get();
      }

      printTrace("booking " + me->getFullname());

      auto& existing = localmes_[me->internal()->key_];

      if (existing != nullptr) {
        // TODO: Check monitor element compatibility
        ME::checkCompatibility(me.get(), existing.get());

        // Delete previously created ME because we have to use the existing one
        me = nullptr;

        return existing.get();
      } else {
        auto result = me.get();
        localmes_[me->internal()->key_] = std::move(me);
        return result;
      }
    }

    template<class ME>
    void DQMStore<ME>::printTrace(std::string const& message) {
      edm::LogWarning("DQMStoreBooking").log([&]( auto& logger) {
        std::regex s_rxtrace{"(.*)\\((.*)\\+0x.*\\).*(\\[.*\\])"};
        std::regex s_rxself{"^[^()]*dqm::implementation::.*|^[^()]*edm::.*|.*edm::convertException::wrap.*"};

        void* array[10];
        size_t size;
        char** strings;
        int demangle_status = 0;
        std::vector<std::string> clean_trace;

        // glibc/libgcc backtrace functionality, declared in execinfo.h.
        size = backtrace(array, 10);
        strings = backtrace_symbols(array, size);

        size_t level = 1;
        char* demangled = nullptr;
        for (; level < size; ++level) {
          std::cmatch match;
          bool ok = std::regex_match(strings[level], match, s_rxtrace);

          if (!ok) {
            edm::LogWarning("DQMStoreBacktrace") << "failed match" << level << strings[level];
            continue;
          }

          if (match[2].length() == 0) {
            // no symbol, ignore.
            continue;
          }

          // demangle name to human readable form
          demangled = abi::__cxa_demangle(std::string(match[2]).c_str(), nullptr, nullptr, &demangle_status);
          if (!demangled || demangle_status != 0) {
            edm::LogWarning("DQMStoreBacktrace") << "failed demangle! status " << demangle_status << " on " << match[2];
            continue;
          }

          if (std::regex_match(demangled, s_rxself)) {
            // ignore framework/internal methods
            free(demangled);
            demangled = nullptr;
            continue;
          } else {
            // keep the demangled name and the address.
            // The address can be resolved to a line number in gdb attached to
            // the process, using `list *0x<addr>`, but it can only be done in
            // the running process and we can"t easily do it in this code.
            clean_trace.push_back(std::string(demangled) + std::string(match[3]));
            free(demangled);
            demangled = nullptr;
          }
        }

        if (clean_trace.size() > 0) {
          logger << message << " at ";
          for (auto const& s : clean_trace) {
            logger << s << "; ";
          }
        } else {
          logger << message << " : failed to collect stack trace.";
        }

        free(strings);
      });
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
        // the default lumi value is LuminosityBlockID::maxLuminosityBlockNumber(),
        // not edm::invalidLuminosityBlockNumber (== 0). LuminosityBlockRange does
        // not even allow lumi values of 0, and replaces them with max.
        auto beginlumi = range.startLumi();
        auto endlumi = range.endLumi();

        if (beginrun == edm::invalidRunNumber && endrun == edm::invalidRunNumber) {
          // this is a prototype, we can simply use it.
          key.coveredrange_ = edm::LuminosityBlockRange(run, lumi, run, lumi);
          is_full = false;
          return;
        }
        if (key.scope_ == MonitorElementData::Scope::RUN) {
          if (beginrun == run && endrun == run) {
            if (endlumi == edm::LuminosityBlockID::maxLuminosityBlockNumber()) {
              // LuminosityBlockRange won't  store a 0 lumi ID, so we need to
              // reconstruct it here.
              endlumi = 0;
            }
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
        auto lock = std::scoped_lock(master_->lock_);
        master_->master_.enterLumi(run, lumi);

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
          auto& masterme = master_->master_.localmes_[key];
          assert(masterme || !"Master does not have the ME we need.");

          // update ME and store it
          me->setInternal(masterme->internal(), /* is_owned */ false, /* is_readonly */ false);
          assert(!masterme->isReadonly());  // else above would need to handle it
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
      // - If no ME for a given path/name is in use, there is one with run=0
      // - All MEs with run/lumi set are currently being filled.
      std::map<MonitorElementData::Key, std::unique_ptr<ME>> newmes;
      for (auto& it : localmes_) {
        // take ME out of localmes
        std::unique_ptr<ME> me;
        me.swap(it.second);

        // create updated key
        MonitorElementData::Key key = it.first;
        bool is_full = false;
        updaterange(key, is_full);

        // clone if needed
        // This should only ever be happen when there are concurrent lumis.
        // In that case, special care must be taken since ME* in subsystem code
        // can point to the wrong per-lumi objects (master logic above handles
        // that for edm::stream based moudules).
        if (is_full) {
          // first, check that the expected ME does not already exist somewhere
          if (localmes_.count(key) == 0 && newmes.count(key) == 0) {
            MonitorElementData* clone = cloneMonitorElementData(me->internal());
            clone->key_ = key;
            auto cloneme = std::make_unique<ME>(clone, /* is_owned */ true, /* is_readonly */ false);
            cloneme->Reset();
            newmes[key].swap(cloneme);
            assert(cloneme == nullptr);
          }
          // else there is nothing to do, we have the needed per lumi instance
          // already somewhere else.

          // put back the old ME, we still need it
          newmes[me->internal()->key_].swap(me);
          assert(me == nullptr);
        } else {
          // update the key. This is not save in general, so const_cast is almost
          // appropriate.
          const_cast<MonitorElementData*>(me->internal())->key_ = key;
          // ... and save into the new map.
          newmes[key].swap(me);
          assert(me == nullptr);
        }
      }

      localmes_.swap(newmes);
    }

    template <class ME>
    MonitorElementData* DQMStore<ME>::cloneMonitorElementData(MonitorElementData const* input) {
      MonitorElementData* clone = new MonitorElementData();
      clone->key_ = input->key_;
      MonitorElementData::Value::Access newvalue(clone->value_);
      MonitorElementData::Value::Access oldvalue(input->value_);
      newvalue.scalar = oldvalue.scalar;
      if (oldvalue.object) {
        newvalue.object = std::unique_ptr<TH1>((TH1*)oldvalue.object->Clone());
      } else {
        newvalue.object = nullptr;
      }
      return clone;
    }

    template <class ME>
    MonitorElementCollection DQMStore<ME>::toProduct(edm::Transition t,
                                                     edm::RunNumber_t run,
                                                     edm::LuminosityBlockNumber_t lumi) {
      // the correct thing would be to forward here, but nobody should need
      // that for now.
      assert(master_ == nullptr);

      MonitorElementCollection product;  // A product to return
      auto it = localmes_.begin();
      while (it != localmes_.end()) {
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
          // Erase null pointer from the map. `it` now points to the next element
          it = localmes_.erase(it);

          // We have to create a prototype MonitorElementData for potential
          // upcoming lumisection only if there is no other (currently being filled)
          // ME. If there is, it will be turned into a prototype once it's done.
          // Since map is an ordered container and key starts with the path/name of the plot,
          // we only have to look into consecutive elements to determine if there is another
          // ME with the same path/name. Next element is returned by the erase() call so we
          // check that as well as `it - 1`.
          bool existsAbove = it != localmes_.end() && it->second->getFullname() == localMe->getFullname();
          bool existsBelow = it != localmes_.begin() && std::prev(it)->second->getFullname() == localMe->getFullname();

          // This only makes sense if there are no other references to localMe->internal()
          auto meData = std::unique_ptr<const MonitorElementData>(localMe->release());

          if (existsAbove || existsBelow) {
            localMe = nullptr;
          } else {
            // Clone and reset for potential reuse in the next lumi
            MonitorElementData* clone = cloneMonitorElementData(meData.get());
            clone->key_.coveredrange_ = edm::LuminosityBlockRange();

            // localMe is reused, so ME* fields in subsystem code remain valid
            localMe->setInternal(clone, /* is_owned */ true, /* is_readonly */ false);
            localmes_[clone->key_].swap(localMe);
            assert(localMe == nullptr);
          }

          product.push_back(std::move(meData));
          //std::raise(SIGINT);
        } else {
          it = std::next(it);
        }
      }

      return product;
    }

    template <class ME>
    void DQMStore<ME>::clearProducts() {
      inputs_.clear();
    }

    template <class ME>
    void DQMStore<ME>::registerProduct(edm::Handle<MonitorElementCollection> mes) {
      inputs_.push_back(mes);
    }

    //
    // Some helpers for the IGetter code.
    //

    // Pair with getters to allow range-based for syntax
    struct Range {
      MonitorElementCollection::const_iterator begin_;
      MonitorElementCollection::const_iterator end_;
      MonitorElementCollection::const_iterator begin() const { return begin_; }
      MonitorElementCollection::const_iterator end() const { return end_; }
    };

    // return Range of all objects in the given directory (name is ignored)
    // Taking Path to enforce normalization of the path.
    Range dirRange(MonitorElementCollection const& mec, MonitorElementData::Path const& dir) {
      assert(dir.getObjectname() == "");
      MonitorElementData proto;
      // all other key fields are default-initialised -- this relies on
      // invalidRun sorting below any valid lumi/run, which it does.
      proto.key_ = MonitorElementData::Key{dir};
      Range r;
      r.begin_ = std::lower_bound(mec.begin(), mec.end(), proto);
      r.end_ = r.begin_;
      while (r.end_ != mec.end() && r.end_->key_.path_.getDirname().rfind(dir.getDirname(), 0) != std::string::npos) {
        r.end_++;
      }
      return r;
    }

    // return Range of all objects at the given path. Might be more than one, if
    // there are instances for different Lumis/Runs etc.
    Range nameRange(MonitorElementCollection const& mec, MonitorElementData::Path const& fullpath) {
      MonitorElementData proto;
      // all other key fields are default-initialised -- this relies on
      // invalidRun sorting below any valid lumi/run, which it does.
      proto.key_ = MonitorElementData::Key{fullpath};
      Range r;
      r.begin_ = std::lower_bound(mec.begin(), mec.end(), proto);
      r.end_ = r.begin_;
      while (r.end_ != mec.end() && r.end_->key_.path_ == fullpath) {
        r.end_++;
      }
      return r;
    }

    template <class ME>
    ME* DQMStore<ME>::importFromProduct(MonitorElementData::Path const& path) {
      auto searchkey = MonitorElementData::Key{path};
      auto candidate = localmes_.lower_bound(searchkey);
      std::unique_ptr<ME> me = nullptr;
      if (candidate != localmes_.end() && candidate->first.path_ == path) {
        // this is the object we want, take it out
        me.swap(candidate->second);
        auto next = localmes_.erase(candidate);
        // make sure we did not have two MEs with same path; not sure how we
        // would handle that in harvesting.
        assert(next == localmes_.end() || !(next->first.path_ == path));
      }
      for (auto& collection : inputs_) {
        assert(collection.isValid());
        for (auto& meData : nameRange(*collection, path)) {
          // Return first element
          if (me == nullptr) {
            // Make a copy of ME sharing the underlying MonitorElementData and root TH1 object
            me = std::make_unique<dqm::harvesting::MonitorElement>(
                &meData, /* is_owned */ false, /* is_readonly */ true);
          } else {
            me->setInternal(&meData, /* is_owned */ false, /* is_readonly */ true);
          }
          // put ME (back) into localmes_.
          ME* retval = me.get();
          localmes_[meData.key_].swap(me);
          return retval;
        }
      }
      // might be nullptr!
      ME* retval = me.get();
      // not in the products, just put back what we had.
      if (me) {
        localmes_[me->internal()->key_].swap(me);
      }
      return retval;
    }

    template <class ME>
    void DQMStore<ME>::loadFromProduct() {
      // collect all names first, since the import will modify the localmes_ map.
      std::set<std::string> names;
      for (auto const& [key, me] : localmes_) {
        (void)key;
        names.insert(me->getFullname());
      }
      for (auto const& name : names) {
        MonitorElementData::Path path;
        path.set(name, MonitorElementData::Path::Type::DIR_AND_NAME);
        ME* meptr = importFromProduct(path);
        if (meptr) {
          // we had an update, touch the thing since we only update here what
          // we want to own in the future.
          meptr->getTH1();
        }
      }
    }

    template <class ME>
    void DQMStore<ME>::cleanupFromProduct() {
      auto it = localmes_.begin();
      while (it != localmes_.end()) {
        if (it->second->isReadonly()) {
          // we don't care about these at all; just destroy it.
          it = localmes_.erase(it);
        } else {
          it++;
        }
      }
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
    ME* IGetter<ME, STORE>::get(std::string const& fullpath) const {
      TRACE(fullpath);

      MonitorElementData::Path path;
      path.set(fullpath, MonitorElementData::Path::Type::DIR_AND_NAME);

      auto searchkey = MonitorElementData::Key{path};
      auto candidate = store_->localmes_.lower_bound(searchkey);
      if (candidate != store_->localmes_.end() && candidate->first.path_ == path) {
        return candidate->second.get();
      }
      // else, no local ME found: try to import
      ME* meptr = store_->importFromProduct(path);

      if (meptr) {
        return meptr;
      }
      // else ...
      TRACE("get(): returning nullptr");
      return nullptr;
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
