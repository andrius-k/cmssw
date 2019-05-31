#include "DQMServices/Core/interface/DQMStore.h"
namespace dqm {
  namespace legacy {

    bool MonitorElement::checkCompatibility(MonitorElement const& a, MonitorElement const& b) {
      // TODO
      return true;
    }

    MonitorElement::MonitorElement() { assert(!"NIY"); }
    MonitorElement::MonitorElement(MonitorElement &&) {
      assert(!"NIY");
    }  // needed to construct object inside container?
    MonitorElement::~MonitorElement() { assert(!"NIY"); }

    void MonitorElement::Fill(double x) const { assert(!"NIY"); }
    void MonitorElement::Fill(std::string &value) const { assert(!"NIY"); }

    void MonitorElement::Fill(double x, double yw) const { assert(!"NIY"); }
    void MonitorElement::Fill(double x, double y, double zw) const { assert(!"NIY"); }
    void MonitorElement::Fill(double x, double y, double z, double w) const { assert(!"NIY"); }
    void MonitorElement::ShiftFillLast(double y, double ye, int32_t xscale) const { assert(!"NIY"); }

    void MonitorElement::Reset() { assert(!"NIY"); }

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

    void MonitorElement::doFill(int64_t x) const { assert(!"NIY"); }
    TH1 *MonitorElement::accessRootObject(const char *func, int reqdim) const { assert(!"NIY"); }

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
