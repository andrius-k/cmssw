#if !WITHOUT_CMS_FRAMEWORK
#include "DQMServices/Core/src/DQMService.h"
#include "DQMServices/Core/interface/DQMNet.h"
#include "DQMServices/Core/interface/DQMStore.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "classlib/utils/Regexp.h"
#include "classlib/utils/Error.h"
#include <mutex>
#include <iostream>
#include <string>
#include <memory>
#include "TBufferFile.h"

// -------------------------------------------------------------------
DQMService::DQMService(const edm::ParameterSet &pset, edm::ActivityRegistry &ar)
    : store_(std::make_unique<DQMStore>()), net_(nullptr), filter_(nullptr), lastFlush_(0), publishFrequency_(5.0) {
  assert(!"NIY");
}

DQMService::~DQMService() { assert(!"NIY"); }

// Flush updates to the network layer at the end of each event.  This
// is the only point at which the main application and the network
// layer interact outside initialisation and exit.
void DQMService::flushStandalone() { assert(!"NIY"); }
void DQMService::flush(edm::StreamContext const &sc) { assert(!"NIY"); }

// Disengage the network service.
void DQMService::shutdown() { assert(!"NIY"); }

#endif  // !WITHOUT_CMS_FRAMEWORK
