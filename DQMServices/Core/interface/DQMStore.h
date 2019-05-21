#ifndef DQMServices_Core_DQMStore_h
#define DQMServices_Core_DQMStore_h

#if __GNUC__ && !defined DQM_DEPRECATED
#define DQM_DEPRECATED __attribute__((deprecated))
#endif

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

#include "DataFormats/Histograms/interface/MonitorElementCollection.h"
#include <tuple>

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

#ifndef DQM_ROOT_METHODS
#define DQM_ROOT_METHODS 1
#endif

namespace dqm {
  // eventually to be turned into "base", and most functionality removed.
  namespace legacy {

    /** The base class for all MonitorElements (ME) */
    class MonitorElement : private MonitorElementData {
    public:
      // we need to make this visible here since it is private-inherited.
      // TODO: can we get the entire enum at once?
      using MonitorElementData::DQM_KIND_INT;
      using MonitorElementData::DQM_KIND_INVALID;
      using MonitorElementData::DQM_KIND_REAL;
      using MonitorElementData::DQM_KIND_STRING;
      using MonitorElementData::DQM_KIND_TH1D;
      using MonitorElementData::DQM_KIND_TH1F;
      using MonitorElementData::DQM_KIND_TH1S;
      using MonitorElementData::DQM_KIND_TH2D;
      using MonitorElementData::DQM_KIND_TH2F;
      using MonitorElementData::DQM_KIND_TH2S;
      using MonitorElementData::DQM_KIND_TH3F;
      using MonitorElementData::DQM_KIND_TPROFILE;
      using MonitorElementData::DQM_KIND_TPROFILE2D;
      using MonitorElementData::Kind;
      ;
      using MonitorElementData::DQM_SCOPE_DEFAULT;
      using MonitorElementData::DQM_SCOPE_JOB;
      using MonitorElementData::DQM_SCOPE_LUMI;
      using MonitorElementData::DQM_SCOPE_RUN;
      using MonitorElementData::Scope;

    private:
      mutable tbb::spin_mutex lock_;
      DQM_DEPRECATED TH1* reference_;  //< Current ROOT reference object
      std::vector<QReport> qreports_;  //< QReports associated to this object.

    public:
      MonitorElement();
      MonitorElement(MonitorElement&&);  // needed to construct object inside container?
      MonitorElement& operator=(const MonitorElement&) = delete;
      MonitorElement& operator=(MonitorElement&&) = delete;
      virtual ~MonitorElement();

      // TODO: maybe move to dataformat
      auto key() const {
        return std::make_tuple(dirname_,
                               objname_,
                               scope_,
                               coveredrange_.startRun(),
                               coveredrange_.startLumi(),
                               coveredrange_.endRun(),
                               coveredrange_.endLumi());
      }

      /// Compare monitor elements, for ordering in sets.
      bool operator<(const MonitorElement& x) const { return this->key() < x.key(); }

      /// Get the type of the monitor element.
      Kind kind() const { return kind_; }

      /// get name of ME
      const std::string& getName() const { return objname_; }

      /// get pathname of parent folder
      const std::string& getPathname() const { return dirname_; }

      /// get full name of ME including Pathname
      const std::string getFullname() const { return dirname_ + objname_; }

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
        return scope_ == MonitorElement::DQM_SCOPE_LUMI;
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
      void ShiftFillLast(double y, double ye = 0., int32_t xscale = 1) const;

      // Not sure what to do with this. Let's make it depracated for the beginning.
      DQM_DEPRECATED
      virtual void Reset();

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
      TH1* accessRootObject(const char* func, int reqdim) const;

    public:
#if DQM_ROOT_METHODS
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
#endif  // DQM_ROOT_METHODS

      // ------------ Operations for MEs that are normally never reset ---------
      DQM_DEPRECATED
      void softReset();

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

      // probably we will drop references entirely.
      DQM_DEPRECATED
      virtual TObject* getRefRootObject() const;

      virtual int64_t getIntValue() const;
      virtual double getFloatValue() const;
      virtual const std::string& getStringValue() const;

      // --- Operations that origianted in ConcurrentME ---
      virtual void setXTitle(std::string const& title);
      virtual void setYTitle(std::string const& title);
      virtual void enableSumw2();
      virtual void disableAlphanumeric();
      virtual void setOption(const char* option);
    };

  }  // namespace legacy
  namespace reco {

    class MonitorElement : public dqm::legacy::MonitorElement {
    public:
      MonitorElement() = default;
      MonitorElement(MonitorElement&&) = default;
      ~MonitorElement() = default;

      // now, we deprecate and assert things that are not allowed on reco MEs.
      // Eventually, they should be dropped from the base, but for now we need to
      // allow implicit conversion to legacy with run-time checks.
#define BAN(thing) \
  DQM_DEPRECATED thing { assert(!"Operation not permitted."); }
      BAN(virtual void setEfficiencyFlag())
      BAN(virtual void Reset())
    protected:
      BAN(TH1* accessRootObject(const char* func, int reqdim) const)

    public:
#if DQM_ROOT_METHODS
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
#endif  // DQM_ROOT_METHODS

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
    };

  }  // namespace reco
  namespace harvesting {

    class MonitorElement : public dqm::reco::MonitorElement {
    public:
      MonitorElement() = default;
      MonitorElement(MonitorElement&&) = default;
      ~MonitorElement() = default;

      // In harvesting, we un-ban some of the operations banned before. Eventually,
      // we should get rid of the legacy base and move the implementations here.
#define UNBAN(name) using dqm::legacy::MonitorElement::name;
      UNBAN(setEfficiencyFlag)
      UNBAN(Reset)
    protected:
      UNBAN(accessRootObject)

    public:
#if DQM_ROOT_METHODS
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
#endif  // DQM_ROOT_METHODS

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
      virtual std::vector<MonitorElement*> getContents(std::string const& path) const = 0;
      virtual std::vector<MonitorElement*> getContents(std::string const& path, unsigned int tag) const = 0;
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

      // we have to discuss semantics here -- are run/lumi ever used?
      virtual std::vector<MonitorElement*> getAllContents(std::string const& path) = 0;
      DQM_DEPRECATED
      virtual std::vector<MonitorElement*> getAllContents(std::string const& path,
                                                          uint32_t runNumber = 0,
                                                          uint32_t lumi = 0) = 0;
      virtual MonitorElement* get(std::string const& path) = 0;

      // same as get, throws an exception if histogram not found
      // Deprecated simply because it is barely used.
      DQM_DEPRECATED
      virtual MonitorElement* getElement(std::string const& path) = 0;

      virtual std::vector<std::string> getSubdirs() = 0;
      virtual std::vector<std::string> getMEs() = 0;
      virtual bool dirExists(std::string const& path) = 0;
      virtual void cd() = 0;
      virtual void cd(std::string const& dir) = 0;
      virtual void setCurrentFolder(std::string const& fullpath) = 0;

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
    template <class ME, class STORE>
    class IBooker : public virtual dqm::legacy::IBooker {
    public:
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

      virtual void cd();
      virtual void cd(std::string const& dir);
      virtual void setCurrentFolder(std::string const& fullpath);
      virtual void goUp();
      virtual std::string const& pwd();
      DQM_DEPRECATED
      virtual void tag(dqm::legacy::MonitorElement*, unsigned int) { assert(!"No longer supported."); }
      DQM_DEPRECATED
      virtual void tagContents(std::string const&, unsigned int) { assert(!"No longer supported."); }

      virtual ~IBooker(){};

    protected:
      IBooker(STORE* store);

      std::string cwd_;
      STORE* store_;
    };

    template <class ME, class STORE>
    class IGetter : public dqm::legacy::IGetter {
    public:
      // TODO: while we can have covariant return types for individual ME*, it seems we can't for the vectors.
      virtual std::vector<dqm::legacy::MonitorElement*> getContents(std::string const& path) const;
      virtual std::vector<dqm::legacy::MonitorElement*> getContents(std::string const& path, unsigned int tag) const;
      virtual void getContents(std::vector<std::string>& into, bool showContents = true) const;

      DQM_DEPRECATED
      virtual void removeContents();
      DQM_DEPRECATED
      virtual void removeContents(std::string const& dir);
      DQM_DEPRECATED
      virtual void removeElement(std::string const& name);
      DQM_DEPRECATED
      virtual void removeElement(std::string const& dir, std::string const& name, bool warning = true);

      virtual std::vector<dqm::legacy::MonitorElement*> getAllContents(std::string const& path);
      DQM_DEPRECATED
      virtual std::vector<dqm::legacy::MonitorElement*> getAllContents(std::string const& path,
                                                                       uint32_t runNumber,
                                                                       uint32_t lumi);
      virtual ME* get(std::string const& path);

      DQM_DEPRECATED
      virtual ME* getElement(std::string const& path);

      virtual std::vector<std::string> getSubdirs();
      virtual std::vector<std::string> getMEs();
      virtual bool dirExists(std::string const& path);
      virtual void cd();
      virtual void cd(std::string const& dir);
      virtual void setCurrentFolder(std::string const& fullpath);

      virtual ~IGetter(){};

    protected:
      IGetter(STORE* store);

      std::string cwd_;
      STORE* store_;
    };

    // TODO: Maybe we need to inline IBooker/IGetter impl here to get the semantics
    // on combined booking/getting right (only one cwd, cd() affects both).
    // For now, explicit IBooker::/IGetter:: prefixing is required on cd() etc. in
    // legacy booking/getting due to the conflicting multiple inheritance.
    // This can be avoided by using an IBooker/IGetter variable, but then both have
    // independent state.
    // When inlining, we again loose the ability to have multiple bookers/getters
    // for one DQMSotre, but then we should not need that since everybody gets
    // their own DQMStore...
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

    private:
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
      typedef dqm::implementation::IBooker<MonitorElement, dqm::implementation::DQMStore<MonitorElement>> IBooker;
      typedef dqm::implementation::IGetter<MonitorElement, dqm::implementation::DQMStore<MonitorElement>> IGetter;
    };
  }  // namespace legacy
}  // namespace dqm

#endif
