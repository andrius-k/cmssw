import FWCore.ParameterSet.Config as cms

### 
from DQMOffline.Trigger.EgHLTOfflineSource_cfi import *

egHLTOffDQMSourceHEP17 = egHLTOffDQMSource.clone()
egHLTOffDQMSourceHEP17.subDQMDirName=cms.string('HEP17')
egHLTOffDQMSourceHEP17.doHEP =cms.bool(True)

