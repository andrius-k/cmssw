#include "DQMServices/Core/interface/DQMStore.h"
namespace dqm {
  namespace legacy {

    MonitorElementData const *MonitorElement::release() {
      auto ptr = internal_;
      internal_ = nullptr;
      is_readonly_ = is_owned_ = false;
      return ptr;
    }

    void MonitorElement::setInternal(MonitorElementData const *data, bool is_owned, bool is_readonly) {
      if (is_owned_) {
        // this should only beed needed to remove prototypes in harvesting,
        // so we check for that to not loose data by accident.
        assert(internal_->key_.coveredrange_.startRun() == 0);
        delete internal_;
      }
      internal_ = data;
      is_owned_ = is_owned;
      is_readonly_ = is_readonly;
    }

    void MonitorElement::makeMutable() const {
      assert(internal_ || !"attempting to modify an invalid ME");
      if (!is_readonly_)
        return;            // nothing to do
      assert(!is_owned_);  // if we own it we should be able to modify it
      internal_ = DQMStore::cloneMonitorElementData(internal_);
      is_owned_ = true;
      is_readonly_ = false;
    }

    MonitorElement::~MonitorElement() {
      if (is_owned_) {
        // This should raraly be hit for normal MEs (the MEData should be
        // handed over to a product in the common case), but it might happen
        // for temporaries during booking.
        if (internal_) {
          delete internal_;
        }
      }
    }

    bool MonitorElement::checkCompatibility(MonitorElement *a, MonitorElement *b) {
      // TODO
      return true;
    }

    void MonitorElement::Fill(double x) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      assert(
          internal_->key_.kind_ == MonitorElement::Kind::INT || internal_->key_.kind_ == MonitorElement::Kind::REAL ||
          internal_->key_.kind_ == MonitorElement::Kind::TH1F || internal_->key_.kind_ == MonitorElement::Kind::TH1S ||
          internal_->key_.kind_ == MonitorElement::Kind::TH1D);
      if (internal_->key_.kind_ == MonitorElement::Kind::INT) {
        access.scalar.num = (int64_t)x;
      } else if (internal_->key_.kind_ == MonitorElement::Kind::REAL) {
        access.scalar.real = x;
      } else {
        assert(access.object || !"Histogram type but ROOT object not set");
        access.object->Fill(x);
      }
    }

    void MonitorElement::doFill(int64_t x) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      assert(
          internal_->key_.kind_ == MonitorElement::Kind::INT || internal_->key_.kind_ == MonitorElement::Kind::REAL ||
          internal_->key_.kind_ == MonitorElement::Kind::TH1F || internal_->key_.kind_ == MonitorElement::Kind::TH1S ||
          internal_->key_.kind_ == MonitorElement::Kind::TH1D);
      if (internal_->key_.kind_ == MonitorElement::Kind::INT) {
        access.scalar.num = x;
      } else if (internal_->key_.kind_ == MonitorElement::Kind::REAL) {
        access.scalar.real = (double)x;
      } else {
        assert(access.object || !"Histogram type but ROOT object not set");
        access.object->Fill((double)x);
      }
    }

    void MonitorElement::Fill(std::string &value) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      assert(internal_->key_.kind_ == MonitorElement::Kind::STRING);
      access.scalar.str = value;
    }

    void MonitorElement::Fill(double x, double yw) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      assert(
          // 1D histograms, this will be a x, weight fill
          internal_->key_.kind_ == MonitorElement::Kind::TH1F || internal_->key_.kind_ == MonitorElement::Kind::TH1S ||
          internal_->key_.kind_ == MonitorElement::Kind::TH1D ||
          // 2D histograms, this will be a x, y fill
          internal_->key_.kind_ == MonitorElement::Kind::TH2D || internal_->key_.kind_ == MonitorElement::Kind::TH2F ||
          internal_->key_.kind_ == MonitorElement::Kind::TH2S ||
          // 1D Profile == 2D histo
          internal_->key_.kind_ == MonitorElement::Kind::TPROFILE);
      assert(access.object || !"Histogram type but ROOT object not set");
      // rely on ROOT to do the right thing.
      access.object->Fill(x, yw);
    }

    void MonitorElement::Fill(double x, double y, double zw) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      if (internal_->key_.kind_ == MonitorElement::Kind::TH2F || internal_->key_.kind_ == MonitorElement::Kind::TH2D ||
          internal_->key_.kind_ == MonitorElement::Kind::TH2S) {
        // 2D histograms, this will be a x, y, weight fill
        auto th2 = dynamic_cast<TH2 *>(access.object.get());
        assert(th2 || !"Histogram type but ROOT object not set or wrong type");
        th2->Fill(x, y, zw);
      } else if (internal_->key_.kind_ == MonitorElement::Kind::TH3F) {
        // 3D histogram, this will be a x, y, z fill
        auto th3 = dynamic_cast<TH3 *>(access.object.get());
        assert(th3 || !"Histogram type but ROOT object not set or wrong type");
        th3->Fill(x, y, zw);
      } else if (internal_->key_.kind_ == MonitorElement::Kind::TPROFILE) {
        //  1D Profile == 2D histo, x, y, weight fill
        auto tprofile = dynamic_cast<TProfile *>(access.object.get());
        assert(tprofile || !"Histogram type but ROOT object not set or wrong type");
        tprofile->Fill(x, y, zw);
      } else if (internal_->key_.kind_ == MonitorElement::Kind::TPROFILE2D) {
        //  2D Profile == 3D histo, x, y, z fill
        auto tprofile2d = dynamic_cast<TProfile2D *>(access.object.get());
        assert(tprofile2d || !"Histogram type but ROOT object not set or wrong type");
        tprofile2d->Fill(x, y, zw);
      } else {
        assert(!"Operation not supported on this kind of MonitorElement");
      }
    }

    void MonitorElement::Fill(double x, double y, double z, double w) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      if (internal_->key_.kind_ == MonitorElement::Kind::TH3F) {
        // 3D histogram, this will be a x, y, z, weight fill
        auto th3 = dynamic_cast<TH3 *>(access.object.get());
        assert(th3 || !"Histogram type but ROOT object not set or wrong type");
        th3->Fill(x, y, z, w);
      } else if (internal_->key_.kind_ == MonitorElement::Kind::TPROFILE2D) {
        //  2D Profile == 3D histo, x, y, z, weight fill
        auto tprofile2d = dynamic_cast<TProfile2D *>(access.object.get());
        assert(tprofile2d || !"Histogram type but ROOT object not set or wrong type");
        tprofile2d->Fill(x, y, z, w);
      } else {
        assert(!"Operation not supported on this kind of MonitorElement");
      }
    }

    void MonitorElement::ShiftFillLast(double y, double ye, int32_t xscale) const { assert(!"NIY"); }

    void MonitorElement::Reset() {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.scalar.num = 0;
      access.scalar.real = 0;
      access.scalar.str = "";
      if (access.object != nullptr) {
        access.object->Reset();
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

    TAxis *MonitorElement::accessAxis(TH1 &object, int axis) {
      if (axis == 1) {
        return object.GetXaxis();
      } else if (axis == 2) {
        return object.GetYaxis();
      } else if (axis == 3) {
        return object.GetZaxis();
      }
      assert(!"Axis number must be 1, 2, or 3.");
    }

    // const and data-independent -- safe
    int MonitorElement::getNbinsX() const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetNbinsX();
    }
    int MonitorElement::getNbinsY() const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetNbinsY();
    }
    int MonitorElement::getNbinsZ() const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetNbinsZ();
    }
    std::string MonitorElement::getAxisTitle(int axis) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return accessAxis(*access.object, axis)->GetTitle();
    }
    std::string MonitorElement::getTitle() const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetTitle();
    }

    // const but data-dependent -- semantically unsafe in RECO
    double MonitorElement::getMean(int axis) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetMean(axis);
    }
    double MonitorElement::getMeanError(int axis) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetMeanError(axis);
    }
    double MonitorElement::getRMS(int axis) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetRMS(axis);
    }
    double MonitorElement::getRMSError(int axis) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetRMSError(axis);
    }
    double MonitorElement::getBinContent(int binx) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetBinContent(binx);
    }
    double MonitorElement::getBinContent(int binx, int biny) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetBinContent(binx, biny);
    }
    double MonitorElement::getBinContent(int binx, int biny, int binz) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetBinContent(binx, biny, binz);
    }
    double MonitorElement::getBinError(int binx) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetBinError(binx);
    }
    double MonitorElement::getBinError(int binx, int biny) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetBinError(binx, biny);
    }
    double MonitorElement::getBinError(int binx, int biny, int binz) const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetBinError(binx, biny, binz);
    }
    double MonitorElement::getEntries() const {
      MonitorElementData::Value::Access access(internal_->value_);
      return access.object->GetEntries();
    }
    double MonitorElement::getBinEntries(int bin) const {
      MonitorElementData::Value::Access access(internal_->value_);
      if (internal_->key_.kind_ == Kind::TPROFILE) {
        return static_cast<TProfile *>(access.object.get())->GetBinEntries(bin);
      } else if (internal_->key_.kind_ == Kind::TPROFILE2D) {
        return static_cast<TProfile2D *>(access.object.get())->GetBinEntries(bin);
      } else {
        assert(!"getBinEntries only exists on TProfile and TProfile2D!");
      }
    }

    // non-const -- thread safety and semantical issues
    void MonitorElement::setBinContent(int binx, double content) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetBinContent(binx, content);
    }
    void MonitorElement::setBinContent(int binx, int biny, double content) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetBinContent(binx, biny, content);
    }
    void MonitorElement::setBinContent(int binx, int biny, int binz, double content) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetBinContent(binx, biny, binz, content);
    }
    void MonitorElement::setBinError(int binx, double error) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetBinError(binx, error);
    }
    void MonitorElement::setBinError(int binx, int biny, double error) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetBinError(binx, biny, error);
    }
    void MonitorElement::setBinError(int binx, int biny, int binz, double error) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetBinError(binx, biny, binz, error);
    }
    void MonitorElement::setBinEntries(int bin, double nentries) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      if (internal_->key_.kind_ == Kind::TPROFILE) {
        return static_cast<TProfile *>(access.object.get())->SetBinEntries(bin, nentries);
      } else if (internal_->key_.kind_ == Kind::TPROFILE2D) {
        return static_cast<TProfile2D *>(access.object.get())->SetBinEntries(bin, nentries);
      } else {
        assert(!"setBinEntries only exists on TProfile and TProfile2D!");
      }
    }
    void MonitorElement::setEntries(double nentries) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetEntries(nentries);
    }
    void MonitorElement::setBinLabel(int bin, const std::string &label, int axis) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      accessAxis(*access.object, axis)->SetBinLabel(bin, label.c_str());
    }
    void MonitorElement::setAxisRange(double xmin, double xmax, int axis) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      accessAxis(*access.object, axis)->SetRangeUser(xmin, xmax);
    }
    void MonitorElement::setAxisTitle(const std::string &title, int axis) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      accessAxis(*access.object, axis)->SetTitle(title.c_str());
    }
    void MonitorElement::setAxisTimeDisplay(int value, int axis) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      accessAxis(*access.object, axis)->SetTimeDisplay(value);
    }
    void MonitorElement::setAxisTimeFormat(const char *format, int axis) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      accessAxis(*access.object, axis)->SetTimeFormat(format);
    }
    void MonitorElement::setTitle(const std::string &title) {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetTitle(title.c_str());
    }

    void MonitorElement::softReset() { assert(!"NIY"); }

    TObject *MonitorElement::getRootObject() const {
      // users could to anything to the object, including modifications
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      // leaking the pointer without the lock. Would be better to hold the lock
      // while the object is in use, but for harvesting there should only be
      // a single thread and we can make an exception.
      return access.object.get();
    }

    TH1 *MonitorElement::getTH1() const { return dynamic_cast<TH1 *>(getRootObject()); }
    TH1F *MonitorElement::getTH1F() const { return dynamic_cast<TH1F *>(getRootObject()); }
    TH1S *MonitorElement::getTH1S() const { return dynamic_cast<TH1S *>(getRootObject()); }
    TH1D *MonitorElement::getTH1D() const { return dynamic_cast<TH1D *>(getRootObject()); }
    TH2F *MonitorElement::getTH2F() const { return dynamic_cast<TH2F *>(getRootObject()); }
    TH2S *MonitorElement::getTH2S() const { return dynamic_cast<TH2S *>(getRootObject()); }
    TH2D *MonitorElement::getTH2D() const { return dynamic_cast<TH2D *>(getRootObject()); }
    TH3F *MonitorElement::getTH3F() const { return dynamic_cast<TH3F *>(getRootObject()); }
    TProfile *MonitorElement::getTProfile() const { return dynamic_cast<TProfile *>(getRootObject()); }
    TProfile2D *MonitorElement::getTProfile2D() const { return dynamic_cast<TProfile2D *>(getRootObject()); }

    TObject *MonitorElement::getRefRootObject() const { assert(!"NIY"); }

    int64_t MonitorElement::getIntValue() const {
      assert(internal_ && internal_->key_.kind_ == Kind::INT);
      MonitorElementData::Value::Access access(internal_->value_);
      return access.scalar.num;
    }
    double MonitorElement::getFloatValue() const {
      assert(internal_ && internal_->key_.kind_ == Kind::REAL);
      MonitorElementData::Value::Access access(internal_->value_);
      return access.scalar.real;
    }
    const std::string &MonitorElement::getStringValue() const {
      assert(internal_ && internal_->key_.kind_ == Kind::STRING);
      MonitorElementData::Value::Access access(internal_->value_);
      return access.scalar.str;
    }

    void MonitorElement::setXTitle(std::string const &title) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetXTitle(title.c_str());
    }
    void MonitorElement::setYTitle(std::string const &title) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetYTitle(title.c_str());
    }

    void MonitorElement::enableSumw2() const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->Sumw2();
    }

    void MonitorElement::disableAlphanumeric() const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->GetXaxis()->SetNoAlphanumeric(false);
      access.object->GetYaxis()->SetNoAlphanumeric(false);
    }

    void MonitorElement::setOption(const char *option) const {
      makeMutable();
      MonitorElementData::Value::Access access(internal_->value_);
      access.object->SetOption(option);
    }

    void MonitorElement::dump(std::ostream &os) const {
      os << "MonitorElement@" << (void *)this << "{\n  .internal_ = " << (void *)this->internal_ << ",\n";
      os << "  .is_owned_ = " << this->is_owned_ << ", .is_readonly_ = " << this->is_readonly_ << ",\n";
      if (this->internal_) {
        os << "  .kind_ = " << static_cast<int>(this->internal_->key_.kind_) << ",\n";
        os << "  .scope_ = " << static_cast<int>(this->internal_->key_.scope_) << ",\n";
        os << "  .path_ = " << this->getFullname() << ",\n";
        os << "  .coveredrange_ = [" << this->internal_->key_.coveredrange_.startRun() << ":"
           << this->internal_->key_.coveredrange_.startLumi() << " - " << this->internal_->key_.coveredrange_.endRun()
           << ":" << this->internal_->key_.coveredrange_.endLumi() << "],\n";
        MonitorElementData::Value::Access access(this->internal_->value_);
        os << "  .scalar_ = {" << access.scalar.num << ", " << access.scalar.real << ", \"" << access.scalar.str
           << "\"},\n";
        os << "  .object_ = " << (void *)access.object.get() << "\n";
      }
      os << "}";
    }

    std::ostream &operator<<(std::ostream &os, MonitorElement const &me) {
      me.dump(os);
      return os;
    }

  }  // namespace legacy
}  // namespace dqm
