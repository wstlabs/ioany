import abc
import csv
import collections
from copy import deepcopy
import simplejson as json
from ioany.util.itertools import lzip
# from .util.itertools import lzip

class DataFrame(metaclass=abc.ABCMeta):

    def __init__(self,stream,header=None,types=None):
        self._rowset = None
        self._stream = stream
        self._header = deepcopy(header)
        self._types  = deepcopy(types)
        self._index  = None
        self.build()

    def _init_header(self):
        if self._header is None:
            s = self._stream
            if hasattr(s,'__len__'):
                self._header = deepcopy(s[0])
            else:
                self._header = deepcopy(next(s))

    @property
    def header(self):
        return deepcopy(self._header)

    @property
    def width(self):
        if self._header is None:
            return None
        return len(self._header)

    @abc.abstractmethod
    def depth(self):
        pass

    def __len__(self):
        return self.depth

    @property
    def dims(self):
        return (self.width,self.depth)

    def _build_index(self):
        # print("header = %s,%s" % (type(self._header),self._header))
        self._index = {}
        for i,label in enumerate(self.header):
            if label in self._index:
                raise ValueError("invalid header - duplicate label '%s' at position %d" % (label,i))
            self._index[label] = i

    def _is_built(self):
        return self._index is not None

    def _assert_unbuilt(self):
        if self._is_built():
            raise RuntimeError("invalid operation -- DataStream already built.")

    @abc.abstractmethod
    def build(self):
        pass

    def position(self,label):
        '''Returns the header position of a given :label if present, or None otherwise'''
        return self._index.get(label)

    @property
    def types(self):
        return self._types

    @abc.abstractmethod
    def rowiter(self):
        pass

    def rows(self):
        """Yields from our rowset of interest, applying the type list (supplied to
        the constructor) to emitted values the way out."""
        types = self.types
        if types is None:
            yield from self.rowiter()
        else:
            for values in self.rowiter():
                yield list(apply_types(self._types,values))

    def recs(self,ordered=False):
        """Yields a sequence of dicts based on the current input stream.
           If :ordered is selected, then these will be OrderedDict structs."""
        if ordered:
            for values in self.rows():
                yield collections.OrderedDict(lzip(self._header,values))
        else:
            for values in self.rows():
                yield dict(lzip(self._header,values))

    @abc.abstractmethod
    def column(self):
        pass

    def write_csv(self,f,csvargs=None):
        if csvargs is None:
            csvargs = {}
        writer = csv.writer(f,**csvargs)
        count = 0
        if self.header:
            writer.writerow(self.header)
        for row in self.rows():
            writer.writerow(row)
            count += 1
        return count

    def save_csv(self,path,encoding='utf-8',csvargs=None):
        f = open(path,"wt",encoding=encoding)
        return self.write_csv(f,csvargs)


class DataFrameStatic(DataFrame):

    def __str__(self):
        values = (type(self._stream),self.width)
        return("DataStreamStatic(streamtype=%s,width=%d)" % values)

    def _ingest(self,source):
        self._assert_unbuilt()
        if hasattr(source,'__len__'):
            self._rowset = list(source[1:])
        else:
            self._rowset = list(source)

    def build(self):
        self._init_header()
        self._ingest(self._stream)
        self._assert_unbuilt()
        self._build_index()

    def rowiter(self):
        yield from self._rowset

    @property
    def depth(self):
        '''Returns the depth of our rowset.'''
        return len(self._rowset)

    def column(self,label):
        j = self.position(label)
        if j is None:
            raise ValueError("invalid label")
        return [r[j] for r in self.rows()]


class DataFrameIterative(DataFrame):

    def __str__(self):
        values = (type(self._stream),self.width)
        return("DataStreamIterative(streamtype=%s,width=%d)" % values)

    def build(self):
        self._init_header()
        if self._is_built():
            raise RuntimeError("invalid operation -- DataStream already built.")
        self._build_index()

    @property
    def depth(self):
        '''Returns the depth of our rowset; since an iterative DataFrame has no :depth, None is returned.'''
        pass

    def rowiter(self):
        yield from self._stream

    def column(self,label):
        j = self.position(label)
        if j is None:
            raise ValueError("invalid label")
        yield from (r[j] for r in self.rows())


def apply_types(types,values):
    if types is None:
        raise ValueError("need a types list")
    if len(types) != len(values):
        raise ValueError("invalid usage - len(values) != len(types)")
    for t,v in zip(types,values):
        if type(v) is t:
            yield v
        else:
            yield t(v)


def read_csv(path,encoding='utf-8',header=None,types=None,csvargs=None):
    if csvargs is None:
        csvargs = {}
    f = open(path,"rt",encoding=encoding)
    reader = csv.reader(f,**csvargs)
    return DataFrameIterative(reader,header=header,types=types)

def load_csv(path,encoding='utf-8',header=None,types=None,csvargs=None):
    if csvargs is None:
        csvargs = {}
    f = open(path,"rt",encoding=encoding)
    reader = csv.reader(f,**csvargs)
    return DataFrameStatic(reader,header=header,types=types)

def save_csv(path,stream,encoding='utf-8',header=None,csvargs=None):
    df = DataFrameIterative(stream,header=header)
    return df.save_csv(path,csvargs)



def load_json(path,encoding='utf-8',header=None,types=None):
    raise RuntimeError("not yet implemented")
    # with open(path,"rt",encoding=encoding) as f:
    # return DataFrameStatic(reader,header=header,types=types)

def save_json(path,obj,sort_keys=True,indent=4):
    with open(path,"wt",encoding="utf-8") as f:
        f.write(json.dumps(obj,sort_keys=sort_keys,indent=indent))

def read_recs(*args,**kwargs):
    df = read_csv(*args,**kwargs)
    yield from df.recs()

def save_recs(path,stream,encoding='utf-8',header=None,csvargs=None):
    if header is None:
        raise ValueError("must have an explicit header")
    rowiter = ([r[k] for k in header] for r in stream)
    return save_csv(path,rowiter,encoding,header,csvargs)


#
# Line-oriented reading/saving/slurping
#

# XXX lots of detail to take care of, re: line endings
def read_lines(path,encoding='utf-8',strip=True):
    with open(path,"rt",encoding=encoding) as f:
        if strip:
            yield from (line.strip() for line in f)
        else:
            yield from (line for line in f)

def slurp_lines(*args,**kwargs):
    return list(read_lines(*args,**kwargs))

def slurp_text(*args,**kwargs):
    lines = read_lines(*args,**kwargs)
    return "\n".join(lines)

def save_lines(path,lines,encoding='utf-8'):
    with open(path,"wt",encoding=encoding) as f:
        for x in lines:
            f.write(str(x) + "\n")

def save_text(path,text,encoding='utf-8'):
    with open(path,"wt",encoding=encoding) as f:
        f.write(text)

def save_content(path,content,encoding='utf-8'):
    if isinstance(content,list):
        save_lines(path,content,encoding=encoding)
    elif isinstance(content,str):
        save_text(path,content,encoding=encoding)
    else:
        raise ValueError("unknown content type '%s'" % type(content))
#
# Plain-Python "slurpers" that return fully populated, native structs.
#

def slurp_json(path,encoding='utf-8'):
    f = open(path,"rt",encoding=encoding)
    return json.load(f)

def slurp_csv(*args,**kwargs):
    df = load_csv(*args,**kwargs)
    return list(df.rows())

def slurp_recs(*args,**kwargs):
    return list(read_recs(*args,**kwargs))


#
# speculative
#

def reciter(f,**kwargs):
    header = None
    reader = csv.reader(f,**kwargs)
    for row in reader:
        if header is None:
            header = row
        else:
            yield dict(zip(header,row))


class Series(collections.Iterator):

    def __init__(self,label,stream,dtype=None):
        self.label  = label
        self.stream = stream

    def __str__(self):
        return("Series(label='%s',stype=%s,dtype=%s)" % (self.label,type(self.stream),self.dtype))

    def __iter__(self):
        if dtype is None:
            yield from self.values
        else:
            yield from (self.dtype(x) for x in values)


