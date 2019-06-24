#ifndef DQMServices_Core_DQMStore_h
#define DQMServices_Core_DQMStore_h

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <iosfwd>
#include <sstream>
#include <iomanip>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include <tuple>
#include <memory>
#include <cxxabi.h>
#include <execinfo.h>
#include <sys/time.h>

#include <classlib/utils/Regexp.h>

#include "TF1.h"
#include "TH1F.h"
#include "TH1S.h"
#include "TH1D.h"
#include "TH2F.h"
#include "TH2S.h"
#include "TH2D.h"
#include "TH3F.h"
#include "TProfile.h"
#include "TProfile2D.h"
#include "TObjString.h"
#include "TAxis.h"

#include <tbb/spin_mutex.h>

#include "FWCore/Utilities/interface/Transition.h"
#include "DataFormats/Common/interface/Handle.h"
#include "DataFormats/Provenance/interface/LuminosityBlockRange.h"

#include "DataFormats/Histograms/interface/MonitorElementCollection.h"

#if __GNUC__ && !defined DQM_DEPRECATED
#define DQM_DEPRECATED __attribute__((deprecated))
#endif

// TODO: Remove at some point:
#define TRACE(msg) std::cout << "TRACE: " << __FILE__ << ":" << __LINE__ << "(" << __FUNCTION__ << ") " << msg << std::endl;
#define TRACE_ TRACE("");

class TFile;
class TBufferFile;

class QCriterion;
namespace dqmstorepb {
  class ROOTFilePB;
  class ROOTFilePB_Histo;
}  // namespace dqmstorepb

struct DQMChannel {
  int binx;       //< bin # in x-axis (or bin # for 1D histogram)
  int biny;       //< bin # in y-axis (for 2D or 3D histograms)
  float content;  //< bin content

  int getBin() { return getBinX(); }
  int getBinX() { return binx; }
  int getBinY() { return biny; }
  float getContents() { return content; }

  DQMChannel(int bx, int by, float data, float /* rms */) {
    // rms is not stored for now, but might be useful in the future.
    binx = bx;
    biny = by;
    content = data;
  }

  DQMChannel() {
    binx = 0;
    biny = 0;
    content = 0;
  }
};

namespace dqm {
  namespace me_util {
    using Channel = DQMChannel;
  }
}  // namespace dqm

/** Currently used (only?) for Online. We might decide to drop DQMNet entirely
 * and use files for the online mode, to get rid of a lot of complexity. */
#include "DQMServices/Core/interface/DQMNet.h"

/** Class for reporting results of quality tests for Monitoring Elements */
class QReport {
  friend class QCriterion;

public:
  /// get test status (see Core/interface/QTestStatus.h)
  int getStatus() const { return qvalue_->code; }

  /// get test result i.e. prob value
  float getQTresult() const { return qvalue_->qtresult; }

  /// get message attached to test
  const std::string& getMessage() const { return qvalue_->message; }

  /// get name of quality test
  /* unused */
  const std::string& getQRName() const { return qvalue_->qtname; }

  /// get vector of channels that failed test
  /// (not relevant for all quality tests!)
  const std::vector<DQMChannel>& getBadChannels() const { return badChannels_; }

private:
  QReport(DQMNet::QValue* value, QCriterion* qc) : qvalue_(value), qcriterion_(qc) {}

  DQMNet::QValue* qvalue_;               //< Pointer to the actual data.
  QCriterion* qcriterion_;               //< Pointer to QCriterion algorithm.
  std::vector<DQMChannel> badChannels_;  //< Bad channels from QCriterion.
};

namespace dqm {
  // eventually to be turned into "base", and most functionality removed.
  namespace legacy {

    /** The base class for all MonitorElements (ME) */
    class MonitorElement {
    public:
      typedef MonitorElementData::Kind Kind;
      typedef MonitorElementData::Scope Scope;

    public:
      MonitorElement(MonitorElementData const* data);
      MonitorElement(MonitorElement const& me);
      MonitorElement& operator=(MonitorElement const&) = delete;
      MonitorElement& operator=(MonitorElement&&) = delete;
      virtual ~MonitorElement();

      /// Get the type of the monitor element.
      Kind kind() const { return internal_->key_.kind_; }

      /// get name of ME
      const std::string& getName() const { return internal_->key_.path_.getObjectname(); }

      /// get pathname of parent folder
      const std::string& getPathname() const { return internal_->key_.path_.getDirname(); }

      /// get full name of ME including Pathname
      const std::string getFullname() const { return internal_->key_.path_.getDirname() + internal_->key_.path_.getObjectname(); }

      /// specify whether ME should be reset at end of monitoring cycle (default:false);
      /// (typically called by Sources that control the original ME)
      DQM_DEPRECATED  // used by HLXMonitor and LaserDQM, both obsolete.
          virtual void
          setResetMe(bool /* flag */) {
        assert(!"NIY");
      }

      /// true if ME is meant to be stored for each luminosity section
      DQM_DEPRECATED  // used only internally
          virtual bool
          getLumiFlag() const {
        return internal_->key_.scope_ == MonitorElement::Scope::LUMI;
      }

      /// this ME is meant to be stored for each luminosity section
      // We'll probably need to support this kind of interface (better alternative
      // would be booking call parameters, but there are way too many already), so
      // this needs to set the Scope, but since the scope is part of the sorting
      // key, this will require some gymnastics with the owning DQMStore.
      virtual void setLumiFlag() { assert(!"NIY"); }

      /// this ME is meant to be an efficiency plot that must not be
      /// normalized when drawn in the DQM GUI.
      // We'll need this, though DQMIO can't store it atm.
      virtual void setEfficiencyFlag() { assert(!"NIY"); }

      // A static assert to check that T actually fits in
      // int64_t.
      template <typename T>
      struct fits_in_int64_t {
        int checkArray[sizeof(int64_t) - sizeof(T) + 1];
      };

      void Fill(long long x) const {
        fits_in_int64_t<long long>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(unsigned long long x) const {
        fits_in_int64_t<unsigned long long>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(unsigned long x) const {
        fits_in_int64_t<unsigned long>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(long x) const {
        fits_in_int64_t<long>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(unsigned int x) const {
        fits_in_int64_t<unsigned int>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(int x) const {
        fits_in_int64_t<int>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(short x) const {
        fits_in_int64_t<short>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(unsigned short x) const {
        fits_in_int64_t<unsigned short>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(char x) const {
        fits_in_int64_t<char>();
        doFill(static_cast<int64_t>(x));
      }
      void Fill(unsigned char x) const {
        fits_in_int64_t<unsigned char>();
        doFill(static_cast<int64_t>(x));
      }

      void Fill(float x) const { Fill(static_cast<double>(x)); }
      void Fill(double x) const;
      void Fill(std::string& value) const;

      void Fill(double x, double yw) const;
      void Fill(double x, double y, double zw) const;
      void Fill(double x, double y, double z, double w) const;
      DQM_DEPRECATED
      void ShiftFillLast(double y, double ye = 0., int32_t xscale = 1) const;

      // Not sure what to do with this. Let's make it deprecated for the beginning.
      DQM_DEPRECATED
      virtual void Reset();
      DQM_DEPRECATED
      virtual void softReset();

      // mostly used for IO, should be private.
      DQM_DEPRECATED
      std::string valueString() const;
      DQM_DEPRECATED
      /* almost unused */ std::string tagString() const;
      DQM_DEPRECATED
      /* almost unused */ std::string tagLabelString() const;
      DQM_DEPRECATED
      /* almost unused */ std::string effLabelString() const;
      DQM_DEPRECATED
      /* almost unused */ std::string qualityTagString(const DQMNet::QValue& qv) const;

      // these are fine functionality-wise, but not really used much.
      /// true if at least of one of the quality tests returned an error
      DQM_DEPRECATED
      bool hasError() const { assert(!"NIY"); }

      /// true if at least of one of the quality tests returned a warning
      DQM_DEPRECATED
      bool hasWarning() const { assert(!"NIY"); }

      /// true if at least of one of the tests returned some other (non-ok) status
      DQM_DEPRECATED
      bool hasOtherReport() const { assert(!"NIY"); }

      // We have to figure out how QTests actually work, and how they are supposed to work.
      /// get QReport corresponding to <qtname> (null pointer if QReport does not exist)
      const QReport* getQReport(const std::string& qtname) const;
      /// get map of QReports
      std::vector<QReport*> getQReports() const;
      /// get warnings from last set of quality tests
      std::vector<QReport*> getQWarnings() const;
      /// get errors from last set of quality tests
      std::vector<QReport*> getQErrors() const;
      /// from last set of quality tests
      std::vector<QReport*> getQOthers() const;

      /// run all quality tests
      /* almost unused */ void runQTests();

    protected:
      void doFill(int64_t x) const;

    public:
      // const and data-independent -- safe
      virtual int getNbinsX() const;
      virtual int getNbinsY() const;
      virtual int getNbinsZ() const;
      virtual std::string getAxisTitle(int axis = 1) const;
      virtual std::string getTitle() const;

      // const but data-dependent -- semantically unsafe in RECO
      virtual double getMean(int axis = 1) const;
      virtual double getMeanError(int axis = 1) const;
      virtual double getRMS(int axis = 1) const;
      virtual double getRMSError(int axis = 1) const;
      virtual double getBinContent(int binx) const;
      virtual double getBinContent(int binx, int biny) const;
      virtual double getBinContent(int binx, int biny, int binz) const;
      virtual double getBinError(int binx) const;
      virtual double getBinError(int binx, int biny) const;
      virtual double getBinError(int binx, int biny, int binz) const;
      virtual double getEntries() const;
      virtual double getBinEntries(int bin) const;

      // non-const -- thread safety and semantical issues
      virtual void setBinContent(int binx, double content);
      virtual void setBinContent(int binx, int biny, double content);
      virtual void setBinContent(int binx, int biny, int binz, double content);
      virtual void setBinError(int binx, double error);
      virtual void setBinError(int binx, int biny, double error);
      virtual void setBinError(int binx, int biny, int binz, double error);
      virtual void setBinEntries(int bin, double nentries);
      virtual void setEntries(double nentries);
      virtual void setBinLabel(int bin, const std::string& label, int axis = 1);
      virtual void setAxisRange(double xmin, double xmax, int axis = 1);
      virtual void setAxisTitle(const std::string& title, int axis = 1);
      virtual void setAxisTimeDisplay(int value, int axis = 1);
      virtual void setAxisTimeFormat(const char* format = "", int axis = 1);
      virtual void setTitle(const std::string& title);
      // --- Operations that origianted in ConcurrentME ---
      virtual void setXTitle(std::string const& title);
      virtual void setYTitle(std::string const& title);
      virtual void enableSumw2();
      virtual void disableAlphanumeric();
      virtual void setOption(const char* option);

      // these should be non-const, since they are potentially not thread-safe
      virtual TObject* getRootObject() const;
      virtual TH1* getTH1() const;
      virtual TH1F* getTH1F() const;
      virtual TH1S* getTH1S() const;
      virtual TH1D* getTH1D() const;
      virtual TH2F* getTH2F() const;
      virtual TH2S* getTH2S() const;
      virtual TH2D* getTH2D() const;
      virtual TH3F* getTH3F() const;
      virtual TProfile* getTProfile() const;
      virtual TProfile2D* getTProfile2D() const;

    public:
      virtual int64_t getIntValue() const;
      virtual double getFloatValue() const;
      virtual const std::string& getStringValue() const;

      // probably we will drop references entirely.
      DQM_DEPRECATED
      virtual TObject* getRefRootObject() const;

    public:
      // but internal, maybe figure out how to make them accessible as friends

      // Do a full type/axis/range consistency check.
      // TODO: figure out what to do with inconsistent MonitorElement::Scope.
      static bool checkCompatibility(MonitorElement* a, MonitorElement* b);
      // Make sure we own the ROOT object by clone'ing it if needed. Needs to
      // be called in all non-const and even some const methods (the fill
      // methods). This is only required for dqm::harvesting and causes some
      // overhead in dqm::reco, so maybe move it down in the hierarchy.
      void makeMutable() const;
      // Provide access to the internal fields.
      // Be careful with the root pointers. The const is also serious; changing
      // parts of the key could corrupt the sorted datastructures.
      MonitorElementData const* internal() { return internal_; }
      void setInternal(MonitorElementData const* data) {
        assert(!is_owned_);  // could be handled but not needed for now
        internal_ = data;
      }

    protected:
      // The actual object holding ME state, including a potential ROOT object.
      // We typically don't own this object, it might be shared with a product
      // and therefore immutable or shared with other DQMStore but still
      // Fill()'able.
      // TODO: this should be shared_ptr, but we need to be able to move the
      // last ("master") ref to a unique_ptr in the end.
      // The idea is to make this a shared_ptr with no op deleter.
      // Then we have to check if ref count is indeed 1 inside toProduct()
      // If not assert.
      MonitorElementData const* internal_;
      // set if it is our duty to destroy/move the internal_ data. This can be
      // either if this is the "master" instance or we did a copy-on-write.
      mutable bool is_owned_;
      // this is set if we borrowed the ROOT object. In that case, mutations
      // are still possible, but will automatically create a copy and reset
      // this flag.
      mutable bool is_readonly_;

      std::vector<QReport> qreports_;  //< QReports associated to this object.
    };

  }  // namespace legacy
  namespace reco {

    class MonitorElement : public dqm::legacy::MonitorElement {
    public:
      MonitorElement() = default;
      MonitorElement(MonitorElement const& me) : dqm::legacy::MonitorElement(me){};
      MonitorElement(MonitorElementData const* data) : dqm::legacy::MonitorElement(data){};
      ~MonitorElement() = default;

      // now, we deprecate and assert things that are not allowed on reco MEs.
      // Eventually, they should be dropped from the base, but for now we need to
      // allow implicit conversion to legacy with run-time checks.
#define BAN(thing) \
  DQM_DEPRECATED thing { assert(!"Operation not permitted."); }
      BAN(virtual void setEfficiencyFlag())

    public:
      BAN(virtual double getMean(int axis = 1) const)
      BAN(virtual double getMeanError(int axis = 1) const)
      BAN(virtual double getRMS(int axis = 1) const)
      BAN(virtual double getRMSError(int axis = 1) const)
      BAN(virtual double getBinContent(int binx) const)
      BAN(virtual double getBinContent(int binx, int biny) const)
      BAN(virtual double getBinContent(int binx, int biny, int binz) const)
      BAN(virtual double getBinError(int binx) const)
      BAN(virtual double getBinError(int binx, int biny) const)
      BAN(virtual double getBinError(int binx, int biny, int binz) const)
      BAN(virtual double getEntries() const)
      BAN(virtual double getBinEntries(int bin) const)

      BAN(virtual void setBinContent(int binx, double content))
      BAN(virtual void setBinContent(int binx, int biny, double content))
      BAN(virtual void setBinContent(int binx, int biny, int binz, double content))
      BAN(virtual void setBinError(int binx, double error))
      BAN(virtual void setBinError(int binx, int biny, double error))
      BAN(virtual void setBinError(int binx, int biny, int binz, double error))
      BAN(virtual void setBinEntries(int bin, double nentries))
      BAN(virtual void setEntries(double nentries))
      BAN(virtual void setBinLabel(int bin, const std::string& label, int axis = 1))
      BAN(virtual void setAxisRange(double xmin, double xmax, int axis = 1))
      BAN(virtual void setAxisTitle(const std::string& title, int axis = 1))
      BAN(virtual void setAxisTimeDisplay(int value, int axis = 1))
      BAN(virtual void setAxisTimeFormat(const char* format = "", int axis = 1))
      BAN(virtual void setTitle(const std::string& title))

      BAN(virtual TObject* getRootObject() const)
      BAN(virtual TH1* getTH1() const)
      BAN(virtual TH1F* getTH1F() const)
      BAN(virtual TH1S* getTH1S() const)
      BAN(virtual TH1D* getTH1D() const)
      BAN(virtual TH2F* getTH2F() const)
      BAN(virtual TH2S* getTH2S() const)
      BAN(virtual TH2D* getTH2D() const)
      BAN(virtual TH3F* getTH3F() const)
      BAN(virtual TProfile* getTProfile() const)
      BAN(virtual TProfile2D* getTProfile2D() const)

      BAN(virtual int64_t getIntValue() const)
      BAN(virtual double getFloatValue() const)
      BAN(virtual const std::string& getStringValue() const)
#undef BAN
    };

  }  // namespace reco
  namespace harvesting {

    class MonitorElement : public dqm::reco::MonitorElement {
    public:
      MonitorElement() = default;
      MonitorElement(MonitorElement const& me) : dqm::reco::MonitorElement(me){};
      MonitorElement(MonitorElementData const* data, bool readonly = false) 
        : dqm::reco::MonitorElement(data) {
        this->is_readonly_ = readonly;
        this->is_owned_ = readonly;
      };
      ~MonitorElement() = default;

      // In harvesting, we un-ban some of the operations banned before. Eventually,
      // we should get rid of the legacy base and move the implementations here.
#define UNBAN(name) using dqm::legacy::MonitorElement::name;
      UNBAN(setEfficiencyFlag)
      UNBAN(Reset)

    public:
      UNBAN(getMean)
      UNBAN(getMeanError)
      UNBAN(getRMS)
      UNBAN(getRMSError)
      UNBAN(getBinContent)
      UNBAN(getBinError)
      UNBAN(getEntries)
      UNBAN(getBinEntries)

      UNBAN(setBinContent)
      UNBAN(setBinError)
      UNBAN(setBinEntries)
      UNBAN(setEntries)
      UNBAN(setBinLabel)
      UNBAN(setAxisRange)
      UNBAN(setAxisTitle)
      UNBAN(setAxisTimeDisplay)
      UNBAN(setAxisTimeFormat)
      UNBAN(setTitle)

      UNBAN(getRootObject)
      UNBAN(getTH1)
      UNBAN(getTH1F)
      UNBAN(getTH1S)
      UNBAN(getTH1D)
      UNBAN(getTH2F)
      UNBAN(getTH2S)
      UNBAN(getTH2D)
      UNBAN(getTH3F)
      UNBAN(getTProfile)
      UNBAN(getTProfile2D)

      UNBAN(getIntValue)
      UNBAN(getFloatValue)
      UNBAN(getStringValue)
#undef UNBAN
    };

  }  // namespace harvesting

  namespace legacy {

    // The basic IBooker is a pure virtual interface that returns the common base
    // class of MEs (legacy). That justifies it being in the legacy namespace.
    class IBooker {
    public:
      virtual MonitorElement* bookInt(TString const& name) = 0;
      virtual MonitorElement* bookFloat(TString const& name) = 0;
      virtual MonitorElement* bookString(TString const& name, TString const& value) = 0;
      virtual MonitorElement* book1D(
          TString const& name, TString const& title, int const nchX, double const lowX, double const highX) = 0;
      virtual MonitorElement* book1D(TString const& name, TString const& title, int nchX, float const* xbinsize) = 0;
      virtual MonitorElement* book1D(TString const& name, TH1F* object) = 0;
      virtual MonitorElement* book1S(TString const& name, TString const& title, int nchX, double lowX, double highX) = 0;
      virtual MonitorElement* book1S(TString const& name, TH1S* object) = 0;
      virtual MonitorElement* book1DD(
          TString const& name, TString const& title, int nchX, double lowX, double highX) = 0;
      virtual MonitorElement* book1DD(TString const& name, TH1D* object) = 0;
      virtual MonitorElement* book2D(TString const& name,
                                     TString const& title,
                                     int nchX,
                                     double lowX,
                                     double highX,
                                     int nchY,
                                     double lowY,
                                     double highY) = 0;
      virtual MonitorElement* book2D(TString const& name,
                                     TString const& title,
                                     int nchX,
                                     float const* xbinsize,
                                     int nchY,
                                     float const* ybinsize) = 0;
      virtual MonitorElement* book2D(TString const& name, TH2F* object) = 0;
      virtual MonitorElement* book2S(TString const& name,
                                     TString const& title,
                                     int nchX,
                                     double lowX,
                                     double highX,
                                     int nchY,
                                     double lowY,
                                     double highY) = 0;
      virtual MonitorElement* book2S(TString const& name,
                                     TString const& title,
                                     int nchX,
                                     float const* xbinsize,
                                     int nchY,
                                     float const* ybinsize) = 0;
      virtual MonitorElement* book2S(TString const& name, TH2S* object) = 0;
      virtual MonitorElement* book2DD(TString const& name,
                                      TString const& title,
                                      int nchX,
                                      double lowX,
                                      double highX,
                                      int nchY,
                                      double lowY,
                                      double highY) = 0;
      virtual MonitorElement* book2DD(TString const& name, TH2D* object) = 0;
      virtual MonitorElement* book3D(TString const& name,
                                     TString const& title,
                                     int nchX,
                                     double lowX,
                                     double highX,
                                     int nchY,
                                     double lowY,
                                     double highY,
                                     int nchZ,
                                     double lowZ,
                                     double highZ) = 0;
      virtual MonitorElement* book3D(TString const& name, TH3F* object) = 0;
      virtual MonitorElement* bookProfile(TString const& name,
                                          TString const& title,
                                          int nchX,
                                          double lowX,
                                          double highX,
                                          int nchY,
                                          double lowY,
                                          double highY,
                                          char const* option = "s") = 0;
      virtual MonitorElement* bookProfile(TString const& name,
                                          TString const& title,
                                          int nchX,
                                          double lowX,
                                          double highX,
                                          double lowY,
                                          double highY,
                                          char const* option = "s") = 0;
      virtual MonitorElement* bookProfile(TString const& name,
                                          TString const& title,
                                          int nchX,
                                          double const* xbinsize,
                                          int nchY,
                                          double lowY,
                                          double highY,
                                          char const* option = "s") = 0;
      virtual MonitorElement* bookProfile(TString const& name,
                                          TString const& title,
                                          int nchX,
                                          double const* xbinsize,
                                          double lowY,
                                          double highY,
                                          char const* option = "s") = 0;
      virtual MonitorElement* bookProfile(TString const& name, TProfile* object) = 0;
      virtual MonitorElement* bookProfile2D(TString const& name,
                                            TString const& title,
                                            int nchX,
                                            double lowX,
                                            double highX,
                                            int nchY,
                                            double lowY,
                                            double highY,
                                            double lowZ,
                                            double highZ,
                                            char const* option = "s") = 0;
      virtual MonitorElement* bookProfile2D(TString const& name,
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
                                            char const* option = "s") = 0;
      virtual MonitorElement* bookProfile2D(TString const& name, TProfile2D* object) = 0;

      virtual void cd() = 0;
      virtual void cd(std::string const& dir) = 0;
      virtual void setCurrentFolder(std::string const& fullpath) = 0;
      virtual void goUp() = 0;
      virtual std::string const& pwd() = 0;

      DQM_DEPRECATED
      virtual void tag(MonitorElement*, unsigned int) = 0;
      DQM_DEPRECATED
      virtual void tagContents(std::string const&, unsigned int) = 0;

      virtual ~IBooker();

    protected:
      IBooker();
    };
    class IGetter {
    public:
      // TODO: review and possibly rename the all methods below:
      // get MEs that are direct children of full path `path`
      virtual std::vector<dqm::harvesting::MonitorElement*> getContents(std::string const& path) const = 0;
      DQM_DEPRECATED  // for use of tag
          virtual std::vector<dqm::harvesting::MonitorElement*>
          getContents(std::string const& path, unsigned int tag) const = 0;
      // not clear what this is good for.
      DQM_DEPRECATED
      virtual void getContents(std::vector<std::string>& into, bool showContents = true) const = 0;

      // We should not need to delete much from the DQMStore; it might not be save
      // to really delete objects anyways. There might be good reasons to use these,
      // lets see.
      DQM_DEPRECATED
      virtual void removeContents() = 0;
      DQM_DEPRECATED
      virtual void removeContents(std::string const& dir) = 0;
      DQM_DEPRECATED
      virtual void removeElement(std::string const& name) = 0;
      DQM_DEPRECATED
      virtual void removeElement(std::string const& dir, std::string const& name, bool warning = true) = 0;

      // get all elements below full path `path`
      // we have to discuss semantics here -- are run/lumi ever used?
      virtual std::vector<dqm::harvesting::MonitorElement*> getAllContents(std::string const& path) const = 0;
      DQM_DEPRECATED
      virtual std::vector<dqm::harvesting::MonitorElement*> getAllContents(std::string const& path,
                                                                           uint32_t runNumber = 0,
                                                                           uint32_t lumi = 0) const = 0;
      // TODO: rename to reflect the fact that it requires full path
      // return ME identified by full path `path`, or nullptr
      virtual MonitorElement* get(std::string const& fullpath) const = 0;

      // same as get, throws an exception if histogram not found
      // Deprecated simply because it is barely used.
      DQM_DEPRECATED
      virtual MonitorElement* getElement(std::string const& path) const = 0;

      // return sub-directories of current directory
      virtual std::vector<std::string> getSubdirs() const = 0;
      // return element names of direct children of current directory
      virtual std::vector<std::string> getMEs() const = 0;
      // returns whether there are objects at full path `path`
      virtual bool dirExists(std::string const& path) const = 0;

      virtual void cd() = 0;
      virtual void cd(std::string const& dir) = 0;
      virtual void setCurrentFolder(std::string const& fullpath) = 0;
      virtual void goUp() = 0;
      virtual std::string const& pwd() = 0;

      virtual ~IGetter();

    protected:
      IGetter();
    };

  }  // namespace legacy
  // this namespace is for internal use only.
  namespace implementation {
    // this provides a templated implementation of the DQMStore. The operations it
    // does are rather always the same; the only thing that changes are the return
    // types. We keep IBooker and IGetter separate, just in case. DQMStore simply
    // multi-inherits them for now.
    // We will instantiate this for reco MEs and harvesting MEs, and maybe for
    // legacy as well.

    class NavigatorBase {
    public:
      void cd();
      void cd(std::string const& dir);
      // This is the only method that is allowed to change cwd_ value
      void setCurrentFolder(std::string const& fullpath);
      void goUp();
      std::string const& pwd();

    protected:
      NavigatorBase(){};
      std::string cwd_ = "";
    };

    template <class ME, class STORE>
    class IBooker : public virtual dqm::legacy::IBooker, public NavigatorBase {
    public:
      // NavigatorBase -- C++ multi-inheritance requires this
      using NavigatorBase::cd;
      using NavigatorBase::goUp;
      using NavigatorBase::pwd;
      using NavigatorBase::setCurrentFolder;

      virtual ME* bookInt(TString const& name);
      virtual ME* bookFloat(TString const& name);
      virtual ME* bookString(TString const& name, TString const& value);
      virtual ME* book1D(
          TString const& name, TString const& title, int const nchX, double const lowX, double const highX);
      virtual ME* book1D(TString const& name, TString const& title, int nchX, float const* xbinsize);
      virtual ME* book1D(TString const& name, TH1F* object);
      virtual ME* book1S(TString const& name, TString const& title, int nchX, double lowX, double highX);
      virtual ME* book1S(TString const& name, TH1S* object);
      virtual ME* book1DD(TString const& name, TString const& title, int nchX, double lowX, double highX);
      virtual ME* book1DD(TString const& name, TH1D* object);
      virtual ME* book2D(TString const& name,
                         TString const& title,
                         int nchX,
                         double lowX,
                         double highX,
                         int nchY,
                         double lowY,
                         double highY);
      virtual ME* book2D(
          TString const& name, TString const& title, int nchX, float const* xbinsize, int nchY, float const* ybinsize);
      virtual ME* book2D(TString const& name, TH2F* object);
      virtual ME* book2S(TString const& name,
                         TString const& title,
                         int nchX,
                         double lowX,
                         double highX,
                         int nchY,
                         double lowY,
                         double highY);
      virtual ME* book2S(
          TString const& name, TString const& title, int nchX, float const* xbinsize, int nchY, float const* ybinsize);
      virtual ME* book2S(TString const& name, TH2S* object);
      virtual ME* book2DD(TString const& name,
                          TString const& title,
                          int nchX,
                          double lowX,
                          double highX,
                          int nchY,
                          double lowY,
                          double highY);
      virtual ME* book2DD(TString const& name, TH2D* object);
      virtual ME* book3D(TString const& name,
                         TString const& title,
                         int nchX,
                         double lowX,
                         double highX,
                         int nchY,
                         double lowY,
                         double highY,
                         int nchZ,
                         double lowZ,
                         double highZ);
      virtual ME* book3D(TString const& name, TH3F* object);
      virtual ME* bookProfile(TString const& name,
                              TString const& title,
                              int nchX,
                              double lowX,
                              double highX,
                              int nchY,
                              double lowY,
                              double highY,
                              char const* option = "s");
      virtual ME* bookProfile(TString const& name,
                              TString const& title,
                              int nchX,
                              double lowX,
                              double highX,
                              double lowY,
                              double highY,
                              char const* option = "s");
      virtual ME* bookProfile(TString const& name,
                              TString const& title,
                              int nchX,
                              double const* xbinsize,
                              int nchY,
                              double lowY,
                              double highY,
                              char const* option = "s");
      virtual ME* bookProfile(TString const& name,
                              TString const& title,
                              int nchX,
                              double const* xbinsize,
                              double lowY,
                              double highY,
                              char const* option = "s");
      virtual ME* bookProfile(TString const& name, TProfile* object);
      virtual ME* bookProfile2D(TString const& name,
                                TString const& title,
                                int nchX,
                                double lowX,
                                double highX,
                                int nchY,
                                double lowY,
                                double highY,
                                double lowZ,
                                double highZ,
                                char const* option = "s");
      virtual ME* bookProfile2D(TString const& name,
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
                                char const* option = "s");
      virtual ME* bookProfile2D(TString const& name, TProfile2D* object);

      DQM_DEPRECATED
      virtual void tag(dqm::legacy::MonitorElement*, unsigned int);
      DQM_DEPRECATED
      virtual void tagContents(std::string const&, unsigned int);

      virtual ~IBooker(){};

    protected:
      IBooker(STORE* store);
      ME* bookME(TString const& name, MonitorElementData::Kind kind, TH1* object);

      STORE* store_;
    };

    template <class ME, class STORE>
    class IGetter : public dqm::legacy::IGetter, public NavigatorBase {
    public:
      // NavigatorBase -- C++ multi-inheritance requires this
      using NavigatorBase::cd;
      using NavigatorBase::goUp;
      using NavigatorBase::pwd;
      using NavigatorBase::setCurrentFolder;

      // TODO: while we can have covariant return types for individual ME*, it seems we can't for the vectors.
      virtual std::vector<dqm::harvesting::MonitorElement*> getContents(std::string const& path) const;
      virtual std::vector<dqm::harvesting::MonitorElement*> getContents(std::string const& path,
                                                                        unsigned int tag) const;
      virtual void getContents(std::vector<std::string>& into, bool showContents = true) const;

      DQM_DEPRECATED
      virtual void removeContents();
      DQM_DEPRECATED
      virtual void removeContents(std::string const& dir);
      DQM_DEPRECATED
      virtual void removeElement(std::string const& name);
      DQM_DEPRECATED
      virtual void removeElement(std::string const& dir, std::string const& name, bool warning = true);

      virtual std::vector<dqm::harvesting::MonitorElement*> getAllContents(std::string const& path) const;
      DQM_DEPRECATED
      virtual std::vector<dqm::harvesting::MonitorElement*> getAllContents(std::string const& path,
                                                                           uint32_t runNumber,
                                                                           uint32_t lumi) const;
      virtual ME* get(std::string const& fullpath) const;

      DQM_DEPRECATED
      virtual ME* getElement(std::string const& path) const;

      virtual std::vector<std::string> getSubdirs() const;
      virtual std::vector<std::string> getMEs() const;
      virtual bool dirExists(std::string const& path) const;

      virtual ~IGetter(){};

    protected:
      IGetter(STORE* store);

      STORE* store_;
    };

    template <class ME>
    class DQMStore : public IGetter<ME, DQMStore<ME>>, public IBooker<ME, DQMStore<ME>> {
    public:
      // TODO: we should gt rid of these.
      enum SaveReferenceTag { SaveWithoutReference, SaveWithReference, SaveWithReferenceForQTest };
      enum OpenRunDirs { KeepRunDirs, StripRunDirs };

      //-------------------------------------------------------------------------
      // ---------------------- Constructors ------------------------------------
      DQMStore();
      ~DQMStore();

      //-------------------------------------------------------------------------
      void setVerbose(unsigned level);

      // ---------------------- softReset methods -------------------------------
      DQM_DEPRECATED
      void softReset(dqm::legacy::MonitorElement* me);
      DQM_DEPRECATED
      void disableSoftReset(dqm::legacy::MonitorElement* me);

      // ---------------------- Public deleting ---------------------------------
      // TODO: this should go into the getter, but probably it should just disappear.
      DQM_DEPRECATED
      void rmdir(std::string const& fullpath);

      // ------------------------------------------------------------------------
      // ---------------------- public I/O --------------------------------------
      // TODO: these need some cleanup, though in general we want to keep the
      // functionality. Maybe move it to a different class, and change the (rather
      // few) usages.
      void save(std::string const& filename,
                std::string const& path = "",
                std::string const& pattern = "",
                std::string const& rewrite = "",
                uint32_t run = 0,
                uint32_t lumi = 0,
                SaveReferenceTag ref = SaveWithReference,
                int minStatus = dqm::qstatus::STATUS_OK,
                std::string const& fileupdate = "RECREATE");
      void savePB(std::string const& filename, std::string const& path = "", uint32_t run = 0, uint32_t lumi = 0);
      bool open(std::string const& filename,
                bool overwrite = false,
                std::string const& path = "",
                std::string const& prepend = "",
                OpenRunDirs stripdirs = KeepRunDirs,
                bool fileMustExist = true);
      bool load(std::string const& filename, OpenRunDirs stripdirs = StripRunDirs, bool fileMustExist = true);

      DQM_DEPRECATED
      bool mtEnabled() { assert(!"NIY"); }

    public:
      // -------------------------------------------------------------------------
      // ---------------------- Public print methods -----------------------------
      // This is a sort of nice feature, but we really cannot allow random printing
      DQM_DEPRECATED
      void showDirStructure() const;

      // -------------------------------------------------------------------------
      // ---------------------- Quality Test methods -----------------------------
      // TODO: to be figured out
      QCriterion* getQCriterion(std::string const& qtname) const;
      QCriterion* createQTest(std::string const& algoname, std::string const& qtname);
      /* almost unused */ void useQTest(std::string const& dir, std::string const& qtname);
      int useQTestByMatch(std::string const& pattern, std::string const& qtname);
      void runQTests();
      int getStatus(std::string const& path = "") const;
      void scaleElements();

      DQM_DEPRECATED
      std::vector<ME*> getMatchingContents(std::string const& pattern) const;

      DQMStore(DQMStore const&) = delete;
      DQMStore& operator=(DQMStore const&) = delete;

      // ------------------------------------------------------------------------
      // ------------ IBooker/IGetter overrides to prevent ambiguity ------------
      virtual void cd() {
        this->IBooker<ME, DQMStore<ME>>::cd();
        this->IGetter<ME, DQMStore<ME>>::cd();
      }
      virtual void cd(std::string const& dir) {
        this->IBooker<ME, DQMStore<ME>>::cd(dir);
        this->IGetter<ME, DQMStore<ME>>::cd(dir);
      }
      virtual void setCurrentFolder(std::string const& fullpath) {
        this->IBooker<ME, DQMStore<ME>>::setCurrentFolder(fullpath);
        this->IGetter<ME, DQMStore<ME>>::setCurrentFolder(fullpath);
      }
      virtual void goUp() {
        this->IBooker<ME, DQMStore<ME>>::goUp();
        this->IGetter<ME, DQMStore<ME>>::goUp();
      }
      std::string const& pwd() { return this->IBooker<ME, DQMStore<ME>>::pwd(); }

    public:
      // internal -- figure out better protection.

      // Make a ME owned by this DQMStore. Will return a pointer to a ME owned
      // by this DQMStore: either an existing ME matching the key of `me` or
      // a newly added one.
      // Will take ownership of the ROOT object in `me`, deleting it if not
      // needed.
      ME* putME(std::unique_ptr<ME>&& me);
      // Prepare MEs for the next lumisection. This will create per-lumi copies
      // if ther previous lumi has not yet finished and recycle reusable MEs if
      // booking/toProduct() left any.
      void enterLumi(edm::RunNumber_t run, edm::LuminosityBlockNumber_t lumi);
      // Turn the MEs associated with t, run, lumi into a read-only product and
      // remove it from the DQMStore. If no copy of a ME would remain in the
      // DQMStore, this will create a clone and keep it for recycling with run/
      // lumi 0.
      // TODO: Add a leaveLumi method to null out MonitorElementData pointers for logical sense.
      // void leaveLumi(edm::RunNumber_t run, edm::LuminosityBlockNumber_t lumi);
      MonitorElementCollection toProduct(edm::Transition t, edm::RunNumber_t run, edm::LuminosityBlockNumber_t lumi);
      // Register a set of MEs to inputs_. Everything we do here needs to be
      // lazy, since we will usually register lots of products but read only
      // few MEs from them.
      void registerProduct(edm::Handle<MonitorElementCollection> mes);
      // Find MEs matching this key in the inputs_ and create MEs for them in
      // localmes_. Needs to handle priority (we could have e.g. harvesting and
      // reco versions of the same ME available). Also need to handle updating
      // MEs read from lumi products.
      void importFromProduct(MonitorElementData::Key const& key);
      // to be used only by DQMEDAnalyzer.
      void setMaster(std::shared_ptr<DQMStore<ME>> master, std::mutex* lock) {
        masterlock_ = lock;
        master_ = master;
      }

    private:
      // Clone data including the underlying ROOT object (calls ->Clone()).
      static MonitorElementData* cloneMonitorElementData(MonitorElementData const* input);

    private:
      // MEs owned by us. All book/get interactions will hand out pointers into
      // this stucture. They may or may not own a ROOT object: in
      // harvesting, we also keep read-only versions of foreign MEs here.
      // MonitorElementDatas in this map can potentially be shared across
      // multiple DQMStores.
      // Expect 10-10000 entries.
      std::map<MonitorElementData::Key, std::unique_ptr<ME>> localmes_;
      // in case of reco and edm::stream, we keep areference to the master
      // DQMStore here. All booking calls should be forwarded ther, and no
      // other operations should be required in reco.
      // All accesses have to take the lock, there will be multiple threads
      // accessing this instance!
      // TODO: use some sort of structure that enforces taking the lock before
      // using the pointer.
      std::shared_ptr<DQMStore<ME>> master_;
      std::mutex* masterlock_;
      // edm products that we can read MEs from. On get, we will implicitly
      // create a read-only ME that does not own a ROOT object in our localmes_
      // from the data here, if we found the requested ME. If a non-const
      // method on such a ME is called, it will again implicitly copy the ROOT
      // object to own it.
      // Expect 100-1000 entries.
      std::vector<edm::Handle<MonitorElementCollection>> inputs_;
    };
  }  // namespace implementation
  namespace reco {
    class DQMStore : public dqm::implementation::DQMStore<MonitorElement> {
    public:
      typedef dqm::implementation::IBooker<MonitorElement, dqm::implementation::DQMStore<MonitorElement>> IBooker;
      typedef dqm::implementation::IGetter<MonitorElement, dqm::implementation::DQMStore<MonitorElement>> IGetter;
    };
  }  // namespace reco
  namespace harvesting {
    class DQMStore : public dqm::implementation::DQMStore<MonitorElement> {
    public:
      typedef dqm::implementation::IBooker<MonitorElement, dqm::implementation::DQMStore<MonitorElement>> IBooker;
      typedef dqm::implementation::IGetter<MonitorElement, dqm::implementation::DQMStore<MonitorElement>> IGetter;
    };
  }  // namespace harvesting
  namespace legacy {
    class DQMStore : public dqm::implementation::DQMStore<MonitorElement> {
    public:
      typedef dqm::legacy::IBooker IBooker;
      typedef dqm::legacy::IGetter IGetter;
    };
  }  // namespace legacy
}  // namespace dqm

#endif
