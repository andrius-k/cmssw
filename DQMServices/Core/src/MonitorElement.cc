#include "DQMServices/Core/interface/DQMStore.h"
namespace dqm {
  namespace legacy {

    bool MonitorElement::checkCompatibility(MonitorElement const &a, MonitorElement const &b) {
      // TODO
      return true;
    }

    MonitorElement::~MonitorElement() {
      // TODO: do we own the ROOT object, if one is set?
      // We do, if this ME was initialized by booking and not moved into a
      // product. We'll not delete it here, to prevent unwelcome surprises.
      // This means special care must be taken when MEs are deleted for good
      // to not leak the ROOT object. (Note that ROOT object ownership might
      // also have sth. to say here).
    }

    void MonitorElement::Fill(double x) const {
      std::scoped_lock lock(lock_);
      assert(kind_ == DQM_KIND_INT || kind_ == DQM_KIND_REAL || kind_ == DQM_KIND_TH1F || kind_ == DQM_KIND_TH1S ||
             kind_ == DQM_KIND_TH1D);
      if (kind_ == DQM_KIND_INT) {
        scalar_.num = (int64_t)x;
      } else if (kind_ == DQM_KIND_REAL) {
        scalar_.real = x;
      } else {
        assert(object_ || !"Histogram type but ROOT object not set");
        object_->Fill(x);
      }
    }

    void MonitorElement::doFill(int64_t x) const {
      std::scoped_lock lock(lock_);
      assert(kind_ == DQM_KIND_INT || kind_ == DQM_KIND_REAL || kind_ == DQM_KIND_TH1F || kind_ == DQM_KIND_TH1S ||
             kind_ == DQM_KIND_TH1D);
      if (kind_ == DQM_KIND_INT) {
        scalar_.num = x;
      } else if (kind_ == DQM_KIND_REAL) {
        scalar_.real = (double)x;
      } else {
        assert(object_ || !"Histogram type but ROOT object not set");
        object_->Fill((double)x);
      }
    }

    void MonitorElement::Fill(std::string &value) const {
      std::scoped_lock lock(lock_);
      assert(kind_ == DQM_KIND_STRING);
      scalar_.str = value;
    }

    void MonitorElement::Fill(double x, double yw) const {
      std::scoped_lock lock(lock_);
      assert(
          // 1D histograms, this will be a x, weight fill
          kind_ == DQM_KIND_TH1F || kind_ == DQM_KIND_TH1S || kind_ == DQM_KIND_TH1D ||
          // 2D histograms, this will be a x, y fill
          kind_ == DQM_KIND_TH2D || kind_ == DQM_KIND_TH2F || kind_ == DQM_KIND_TH2S ||
          // 1D Profile == 2D histo
          kind_ == DQM_KIND_TPROFILE);
      assert(object_ || !"Histogram type but ROOT object not set");
      // rely on ROOT to do the right thing.
      object_->Fill(x, yw);
    }

    void MonitorElement::Fill(double x, double y, double zw) const {
      std::scoped_lock lock(lock_);
      if (kind_ == DQM_KIND_TH2F || kind_ == DQM_KIND_TH2D || kind_ == DQM_KIND_TH2S) {
        // 2D histograms, this will be a x, y, weight fill
        auto th2 = dynamic_cast<TH2 *>(object_);
        assert(th2 || !"Histogram type but ROOT object not set or wrong type");
        th2->Fill(x, y, zw);
      } else if (kind_ == DQM_KIND_TH3F) {
        // 3D histogram, this will be a x, y, z fill
        auto th3 = dynamic_cast<TH3 *>(object_);
        assert(th3 || !"Histogram type but ROOT object not set or wrong type");
        th3->Fill(x, y, zw);
      } else if (kind_ == DQM_KIND_TPROFILE) {
        //  1D Profile == 2D histo, x, y, weight fill
        auto tprofile = dynamic_cast<TProfile *>(object_);
        assert(tprofile || !"Histogram type but ROOT object not set or wrong type");
        tprofile->Fill(x, y, zw);
      } else if (kind_ == DQM_KIND_TPROFILE2D) {
        //  2D Profile == 3D histo, x, y, z fill
        auto tprofile2d = dynamic_cast<TProfile2D *>(object_);
        assert(tprofile2d || !"Histogram type but ROOT object not set or wrong type");
        tprofile2d->Fill(x, y, zw);
      } else {
        assert(!"Operation not supported on this kind of MonitorElement");
      }
    }

    void MonitorElement::Fill(double x, double y, double z, double w) const {
      std::scoped_lock lock(lock_);
      if (kind_ == DQM_KIND_TH3F) {
        // 3D histogram, this will be a x, y, z, weight fill
        auto th3 = dynamic_cast<TH3 *>(object_);
        assert(th3 || !"Histogram type but ROOT object not set or wrong type");
        th3->Fill(x, y, z, w);
      } else if (kind_ == DQM_KIND_TPROFILE2D) {
        //  2D Profile == 3D histo, x, y, z, weight fill
        auto tprofile2d = dynamic_cast<TProfile2D *>(object_);
        assert(tprofile2d || !"Histogram type but ROOT object not set or wrong type");
        tprofile2d->Fill(x, y, z, w);
      } else {
        assert(!"Operation not supported on this kind of MonitorElement");
      }
    }

    void MonitorElement::ShiftFillLast(double y, double ye, int32_t xscale) const { assert(!"NIY"); }

    void MonitorElement::Reset() {
      std::scoped_lock lock(lock_);
      scalar_.num = 0;
      scalar_.real = 0;
      scalar_.str = "";
      if (kind_ >= DQM_KIND_TH1F) {
        assert(object_ || !"Histogram type but ROOT object not set");
        object_->Reset();
      }
    }

    std::string MonitorElement::valueString() const { assert(!"NIY"); }
    /* almost unused */ std::string MonitorElement::tagString() const { assert(!"NIY"); }
    /* almost unused */ std::string MonitorElement::tagLabelString() const { assert(!"NIY"); }
    /* almost unused */ std::string MonitorElement::effLabelString() const { assert(!"NIY"); }
    /* almost unused */ std::string MonitorElement::qualityTagString(const DQMNet::QValue &qv) const { assert(!"NIY"); }

    const QReport *MonitorElement::getQReport(const std::string &qtname) const { assert(!"NIY"); }
    std::vector<QReport *> MonitorElement::getQReports() const { assert(!"NIY"); }
    std::vector<QReport *> MonitorElement::getQWarnings() const { assert(!"NIY"); }
    std::vector<QReport *> MonitorElement::getQErrors() const { assert(!"NIY"); }
    std::vector<QReport *> MonitorElement::getQOthers() const { assert(!"NIY"); }

    /* almost unused */ void MonitorElement::runQTests() { assert(!"NIY"); }

    // const and data-independent -- safe
    int MonitorElement::getNbinsX() const { assert(!"NIY"); }
    int MonitorElement::getNbinsY() const { assert(!"NIY"); }
    int MonitorElement::getNbinsZ() const { assert(!"NIY"); }
    std::string MonitorElement::getAxisTitle(int axis) const { assert(!"NIY"); }
    std::string MonitorElement::getTitle() const { assert(!"NIY"); }

    // const but data-dependent -- semantically unsafe in RECO
    double MonitorElement::getMean(int axis) const { assert(!"NIY"); }
    double MonitorElement::getMeanError(int axis) const { assert(!"NIY"); }
    double MonitorElement::getRMS(int axis) const { assert(!"NIY"); }
    double MonitorElement::getRMSError(int axis) const { assert(!"NIY"); }
    double MonitorElement::getBinContent(int binx) const { assert(!"NIY"); }
    double MonitorElement::getBinContent(int binx, int biny) const { assert(!"NIY"); }
    double MonitorElement::getBinContent(int binx, int biny, int binz) const { assert(!"NIY"); }
    double MonitorElement::getBinError(int binx) const { assert(!"NIY"); }
    double MonitorElement::getBinError(int binx, int biny) const { assert(!"NIY"); }
    double MonitorElement::getBinError(int binx, int biny, int binz) const { assert(!"NIY"); }
    double MonitorElement::getEntries() const { assert(!"NIY"); }
    double MonitorElement::getBinEntries(int bin) const { assert(!"NIY"); }

    // non-const -- thread safety and semantical issues
    void MonitorElement::setBinContent(int binx, double content) { assert(!"NIY"); }
    void MonitorElement::setBinContent(int binx, int biny, double content) { assert(!"NIY"); }
    void MonitorElement::setBinContent(int binx, int biny, int binz, double content) { assert(!"NIY"); }
    void MonitorElement::setBinError(int binx, double error) { assert(!"NIY"); }
    void MonitorElement::setBinError(int binx, int biny, double error) { assert(!"NIY"); }
    void MonitorElement::setBinError(int binx, int biny, int binz, double error) { assert(!"NIY"); }
    void MonitorElement::setBinEntries(int bin, double nentries) { assert(!"NIY"); }
    void MonitorElement::setEntries(double nentries) { assert(!"NIY"); }
    void MonitorElement::setBinLabel(int bin, const std::string &label, int axis) { assert(!"NIY"); }
    void MonitorElement::setAxisRange(double xmin, double xmax, int axis) { assert(!"NIY"); }
    void MonitorElement::setAxisTitle(const std::string &title, int axis) { assert(!"NIY"); }
    void MonitorElement::setAxisTimeDisplay(int value, int axis) { assert(!"NIY"); }
    void MonitorElement::setAxisTimeFormat(const char *format, int axis) { assert(!"NIY"); }
    void MonitorElement::setTitle(const std::string &title) { assert(!"NIY"); }

    void MonitorElement::softReset() { assert(!"NIY"); }

    TObject *MonitorElement::getRootObject() const { assert(!"NIY"); }
    TH1 *MonitorElement::getTH1() const { assert(!"NIY"); }
    TH1F *MonitorElement::getTH1F() const { assert(!"NIY"); }
    TH1S *MonitorElement::getTH1S() const { assert(!"NIY"); }
    TH1D *MonitorElement::getTH1D() const { assert(!"NIY"); }
    TH2F *MonitorElement::getTH2F() const { assert(!"NIY"); }
    TH2S *MonitorElement::getTH2S() const { assert(!"NIY"); }
    TH2D *MonitorElement::getTH2D() const { assert(!"NIY"); }
    TH3F *MonitorElement::getTH3F() const { assert(!"NIY"); }
    TProfile *MonitorElement::getTProfile() const { assert(!"NIY"); }
    TProfile2D *MonitorElement::getTProfile2D() const { assert(!"NIY"); }

    TObject *MonitorElement::getRefRootObject() const { assert(!"NIY"); }

    int64_t MonitorElement::getIntValue() const { assert(!"NIY"); }
    double MonitorElement::getFloatValue() const { assert(!"NIY"); }
    const std::string &MonitorElement::getStringValue() const { assert(!"NIY"); }

    void MonitorElement::setXTitle(std::string const &title) { assert(!"NIY"); }
    void MonitorElement::setYTitle(std::string const &title) { assert(!"NIY"); }
    void MonitorElement::enableSumw2() { assert(!"NIY"); }
    void MonitorElement::disableAlphanumeric() { assert(!"NIY"); }
    void MonitorElement::setOption(const char *option) { assert(!"NIY"); }

  }  // namespace legacy
}  // namespace dqm
