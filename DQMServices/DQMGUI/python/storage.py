import re
import time
import zlib
import struct
import socket
import sqlite3
import tempfile
import subprocess

from collections import namedtuple, defaultdict

from DQMServices.DQMGUI import nanoroot

DBNAME = '../data/directory.sqlite'

DBSCHEMA = """
BEGIN;
CREATE TABLE IF NOT EXISTS samples(dataset, run, lumi, filename, menamesid, meoffsetsid);
CREATE INDEX IF NOT EXISTS samplelookup ON samples(dataset, run, lumi);
CREATE UNIQUE INDEX IF NOT EXISTS uniquesamples on samples(dataset, run, lumi, filename);
CREATE TABLE IF NOT EXISTS menames(menamesid INTEGER PRIMARY KEY, menameblob);
CREATE UNIQUE INDEX IF NOT EXISTS menamesdedup ON menames(menameblob);
CREATE TABLE IF NOT EXISTS meoffsets(meoffsetsid INTEGER PRIMARY KEY, meoffsetsblob);
COMMIT;
"""

EfficiencyFlag = namedtuple("EfficiencyFlag", ["name"])
ScalarValue = namedtuple("ScalarValue", ["name", "value", "type"])
QTest = namedtuple("QTest", ["name", "qtestname", "status", "result", "algorithm", "message"])

with sqlite3.connect(DBNAME) as db:
    db.executescript(DBSCHEMA)

class DQMDataStore:

    db = sqlite3.connect(DBNAME)
    db.executescript(DBSCHEMA)

    # TODO: Close connection at some point!
    # def __del__(self):
        # DQMDataStore.db.close()

    @staticmethod
    def __execute(sql, args=None):
        c = DQMDataStore.db.cursor()
        if args:
            return  c.execute(sql, args)
        else:
            return c.execute(sql)

    @staticmethod
    def get_samples(run, dataset):
        if run:
            run = '%%%s%%' % run
        if dataset:
            dataset = '%%%s%%' % dataset

        results = []

        if run == dataset == None:
            return samples
        elif run != None and dataset != None:
            sql = 'SELECT DISTINCT run, dataset FROM samples WHERE dataset LIKE ? AND run LIKE ?'
            results = DQMDataStore.__execute(sql, (dataset, run))
        elif run != None:
            sql = 'SELECT DISTINCT run, dataset FROM samples WHERE run LIKE ?'
            results = DQMDataStore.__execute(sql, (run,))
        elif dataset != None:
            sql = 'SELECT DISTINCT run, dataset FROM samples WHERE dataset LIKE ?'
            results = DQMDataStore.__execute(sql, (dataset,))

        return results

    @staticmethod
    def get_me_list_blob(run, dataset):
        """
        # One me is represented as a multiple of 2 lines in a list
        # First line contains and ME path and secnod line contains secondary string:
        # a metadada of that ME, if it exists.
        # Possible cases of metadata: efficiency flag or QTest
        # If one ME has multiple secondary strings, its name in the list will appear multiple times,
        # so lines can always be parsed 2 at a time.
        # If secondary string doesn't exist, second line will be empty. That is 99% of all the cases.
        # Example:
        # me1
        # me1_qtest1
        # me1
        # me1_qtest2
        """
        # For now adding a LIMIT 1 because there might be multiple version of the same file.
        sql = 'SELECT menameblob FROM samples JOIN menames ON samples.menamesid = menames.menamesid WHERE run = ? AND dataset = ? LIMIT 1;'
        blob = DQMDataStore.__execute(sql, (int(run), dataset))
        blob = list(blob)[0][0]
        return blob

    # def get_me_offsets_blob(self, run, dataset):
    #     # For now adding a LIMIT 1 because there might be multiple version of the same file.
    #     sql = 'SELECT meoffsetsblob FROM samples JOIN meoffsets ON samples.meoffsetsid = meoffsets.meoffsetsid WHERE run = ? AND dataset = ? LIMIT 1;'
    #     blob = self.__execute(sql, (int(run), dataset))
    #     blob = list(blob)[0][0]
    #     return blob

    @staticmethod
    def get_blobs_and_filename(run, dataset):
        # For now adding a LIMIT 1 because there might be multiple version of the same file.
        sql = '''
        SELECT
            filename,
            menameblob,
            meoffsetsblob
        FROM
            samples
            JOIN menames ON samples.menamesid = menames.menamesid
            JOIN meoffsets ON samples.meoffsetsid = meoffsets.meoffsetsid
        WHERE
            run = ?
        AND 
            dataset = ?
        LIMIT 1;
        '''
        cur = DQMDataStore.__execute(sql, (int(run), dataset))
        cur = list(cur)

        filename = cur[0][0]
        list_blob = cur[0][1]
        offsets_blob = cur[0][2]

        return (filename, list_blob, offsets_blob)


    @staticmethod
    def meoffsetsfromblob(offsetblob):
        return MEInfo.blobtolist(offsetblob)

    @staticmethod
    def melistfromblob(namesblob):
        return zlib.decompress(namesblob).splitlines()


# This class represents a ME in storage. It needs to be very small, since we 
# will store billions of these in DB. For example, it does not know it's name.
# It does store the offsets into the ROOT file and everything else needed to
# read the data from the ROOT file.
class MEInfo:
    idtotype = {
      # these match the DQMIO encoding where possible. But they don't really need to.
      0: b"Int",
      1: b"Float",
      2: b"String",
      3: b"TH1F",
      4: b"TH1S",
      5: b"TH1D",
      6: b"TH2F",
      7: b"TH2S",
      8: b"TH2D",
      9: b"TH3F",
      10: b"TProfile",
      11: b"TProfile2D",
      20: b"Flag",
      21: b"QTest",
      22: b"XMLString", # For string type in TDirectory
    }
    typetoid = {v: k for k, v in idtotype.items()}

    # These are used to store the MEInfo into a blob. The scalar
    # version stores the value directly. They are all the
    # same size to allow direct indexing.
    normalformat = struct.Struct("<qiihh")
    # This is a bit of a hack: the deltaencode below needs all topmost bits to be unused.
    # so we spread the double and int64 values for scalars with a bit of padding to keep them free.
    scalarformat = struct.Struct("<xBBBBBBxxBBxxxxxHxx") 
    intformat    = struct.Struct("<q")
    floatformat  = struct.Struct("<d")
    
    @staticmethod
    def listtoblob(meinfos):
        # For the meinfos, Zlib compression is not very effective: there is
        # little repetition for the LZ77 and the Huffmann coder struggles with the
        # large numbers.
        # But, since the list is roughly increasing in order, delta coding makes most
        # values small. Then, the Huffmann coder in Zlib compresses well.
        # Decreases output size about 4x.
        words = MEInfo.normalformat
        def deltacode(a):
            prev = [0,0,0,0,0]
            for x in a:
                new = words.unpack(x)
                yield words.pack(*[a-b for a, b in zip(new, prev)])
                prev = new
        delta = deltacode(i.pack() for i in meinfos)
        buf = b''.join(delta)
        infoblob = zlib.compress(buf)
        print (f"MEInfo: compression {len(buf)/len(infoblob)}, total {len(buf)}")
        return infoblob
    
    @staticmethod
    def blobtopacked(infoblob):
        words = MEInfo.normalformat
        def deltadecode(d):
            prev = [0,0,0,0,0]
            for x in d:
                new = [a+b for a, b in zip(prev, words.unpack(x))]
                yield words.pack(*new)
                prev = new
        buf = zlib.decompress(infoblob)
        packed = deltadecode(buf[i:i+words.size] for i in range(0, len(buf), words.size))
        return list(packed)
    
    @staticmethod
    def blobtolist(infoblob):
        packed = MEInfo.blobtopacked(infoblob)
        return [MEInfo.unpack(x) for x in packed]
    
    def __init__(self, metype, seekkey = 0, offset = 0, size = -1, value = None, qteststatus = 0):
        self.metype = metype
        self.seekkey = seekkey
        self.offset = offset
        self.size = size
        self.value = value
        self.qteststatus = qteststatus
        
    def __repr__(self):
        print('ASASASASA')
        return (f"MEInfo(metype={repr(self.metype)}" +
            f"{', seekkey=%d' % self.seekkey if self.seekkey else ''}" +
            f"{', offset=%d' % self.offset if self.offset else ''}" +    
            f"{', size=%d' % self.size if self.size > 0 else ''}" +
            f"{', value=%s' % repr(self.value) if self.value else ''}" +
            f"{', qteststatus=%d' % self.qteststatus if self.qteststatus else ''}" +
            ")")
    
    def __lt__(self, other):
        return (self.metype, self.seekkey, self.offset) <  (other.metype, other.seekkey, other.offset)
    
    def pack(self):
        if self.metype == b'Int':
            buf = MEInfo.intformat.pack(self.value)
            return MEInfo.scalarformat.pack(*buf, MEInfo.typetoid[self.metype])
        if self.metype == b'Float':
            buf = MEInfo.floatformat.pack(self.value)
            return MEInfo.scalarformat.pack(*buf, MEInfo.typetoid[self.metype])
        return MEInfo.normalformat.pack(self.seekkey, self.offset, self.size, 
                                        MEInfo.typetoid[self.metype], self.qteststatus)
    @staticmethod
    def unpack(buf):
        k, o, s, t, st = MEInfo.normalformat.unpack(buf)
        metype = MEInfo.idtotype[t]
        if metype == b'Int':
            buf = MEInfo.scalarformat.unpack(buf)
            v, = MEInfo.intformat.unpack(bytes(buf[:-1])) # last is metype again
            return MEInfo(metype, value = v)
        if metype == b'Float':
            buf = MEInfo.scalarformat.unpack(buf)
            v, = MEInfo.floatformat.unpack(bytes(buf[:-1])) # last is metype again
            return MEInfo(metype, value = v)
        return MEInfo(metype, seekkey = k, offset = o, size = s, qteststatus = st)
    
    def read(self, rootfile):
        if self.value != None:
            return ScalarValue(b'', self.value, b'') # TODO: do sth. better.
        key = nanoroot.TKey(rootfile, self.seekkey)
        data = key.objdata()
        if self.metype == b'QTest':
            # TODO: this won't work for DQMIO, but we don't have QTests there anyways...
            return parsestringentry(key.objname())
        if self.metype == b'XMLString':
            return parsestringentry(key.objname())
        if self.offset == 0 and self.size == -1:
            obj = data
        else:
            obj = data[self.offset : self.offset + self.size]
        if self.metype == b'String':
            s = nanoroot.String.unpack(obj, 0, len(obj), None)
            return ScalarValue(b'', s, b's')
        # else
        classversion = 3 #TODO: we need to have a better guess here...
        # metype doubles as root class name here.
        return nanoroot.TBufferFile(obj, self.metype, classversion) 


@staticmethod
def parsestringentry(val):
    # non-object data is stored in fake-XML stings in the TDirectory.
    # This decodes these strings into an object of correct type.
    assert val[0] == b'<'[0]
    name = val[1:].split(b'>', 1)[0]
    value = val[1+len(name)+1:].split(b'<', 1)[0]
    if value == b"e=1": # efficiency flag on this ME
        return EfficiencyFlag(name)
    elif len(value) >= 2 and value[1] == b'='[0]:
        return ScalarValue(name, value[2:], value[0:1])
    else: # should be a qtest in this case
        assert value.startswith(b'qr=')
        assert b'.' in name
        mename, qtestname = name.split(b'.', 1)
        parts = value[3:].split(b':', 4)
        assert len(parts) == 5, "Expect 5 parts, not " + repr(parts)
        x, status, result, algorithm, message = parts
        assert x == b'st'
        return QTest(mename, qtestname, status, result, algorithm, message)