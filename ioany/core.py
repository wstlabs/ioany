import abc
import csv
import collections
from copy import deepcopy
import simplejson as json
from .util.itertools import lzip
from . import fixed

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

    def recs(self,ordered=True):
        """Yields a sequence of dicts based on the current input stream.
           By default these will be OrderedDict structs.  However, if :ordered
           is set to False, they will be vanilla dicts."""
        if ordered:
            for values in self.rows():
                yield collections.OrderedDict(lzip(self._header,values))
        else:
            for values in self.rows():
                yield dict(lzip(self._header,values))

    @abc.abstractmethod
    def column(self):
        pass

    def write_csv(self,f,csvargs=None,apply_json=False):
        if csvargs is None:
            csvargs = {}
        # apply some reasonably sane defaults.
        if 'dialect' not in csvargs:
            csvargs['dialect'] = 'unix'
        if 'quoting' not in csvargs:
            csvargs['quoting'] = csv.QUOTE_MINIMAL
        writer = csv.writer(f,**csvargs)
        count = 0
        if self.header:
            writer.writerow(self.header)
        for row in self.rows():
            if apply_json:
                row = jsonify(row)
            writer.writerow(row)
            count += 1
        return count

    def save_csv(self,path,encoding='utf-8',csvargs=None,apply_json=False):
        f = open(path,"wt",encoding=encoding)
        return self.write_csv(f,csvargs,apply_json)


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

BOOLISH = {}
BOOLISH[True] = set(['true','t','yes','y','1'])
BOOLISH[False] = set(['false','f','no','n','0'])
def boolify(s):
    if type(s) is not str:
        return s
    ss = s.lower()
    if ss in BOOLISH[True]:
        return True
    if ss in BOOLISH[False]:
        return False
    return s

def apply_type(t,v):
    # if we're already of the desired type - pass the value on through
    if type(v) is t:
        return v
    # otherwise there's one case we need to provide special treatment for:
    # boolean casts of string values
    if type(v) is str and t is bool:
        return boolify(v)
    # otherwise, try our look in casting to the desired type
    return t(v)

def apply_types(types,values):
    if types is None:
        raise ValueError("need a types list")
    if len(types) != len(values):
        raise ValueError("invalid usage - len(values) != len(types)")
    for t,v in zip(types,values):
        yield apply_type(t,v)

def read_fixed(path,encoding='utf-8',spec=None):
    if spec is None:
        raise ValueError("need a spec")
    f = open(path,"rt",encoding=encoding)
    reader = fixed.reader(f,spec)
    header,types = fixed.divine(spec)
    return DataFrameIterative(reader,header=header,types=types)


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

def save_json(path,obj,sort_keys=True,indent=4):
    with open(path,"wt",encoding="utf-8") as f:
        f.write(json.dumps(obj,sort_keys=sort_keys,indent=indent))

def read_recs(path,**kwargs):
    lowpath = path.lower()
    if lowpath.endswith('.csv'):
        return read_recs_csv(path,**kwargs)
    if lowpath.endswith('.txt'):
        return read_recs_fixed(path,**kwargs)
    raise ValueError("unknown file extension")

def read_recs_csv(path,**kwargs):
    df = read_csv(path,**kwargs)
    yield from df.recs()

def read_recs_fixed(path,**kwargs):
    df = read_fixed(path,**kwargs)
    yield from df.recs()

def read_rows(path,**kwargs):
    lowpath = path.lower()
    if lowpath.endswith('.csv'):
        return read_rows_csv(path,**kwargs)
    if lowpath.endswith('.txt'):
        return read_rows_fixed(path,**kwargs)
    raise ValueError("unknown file extension")

def read_rows_csv(path,**kwargs):
    df = read_csv(path,**kwargs)
    yield from df.rows()

def read_rows_fixed(path,**kwargs):
    df = read_fixed(path,**kwargs)
    yield from df.rows()

def peekaboo(stream):
    if isinstance(stream,list):
        if len(stream) < 1:
            raise ValueError("can't look ahead in empty list struct")
        _stream = (_ for _ in stream)
        return peekaboo_iterator(_stream)
    else:
        return peekaboo_iterator(stream)

def peekaboo_iterator(stream):
    r = next(stream)
    header = list(r.keys())
    def newstream():
        yield r
        yield from stream
    _stream = newstream()
    return header,_stream

def save_recs(path,stream,encoding='utf-8',header=None,csvargs=None):
    if header is None:
        header,stream = peekaboo(stream)
    rowiter = ([r.get(k) for k in header] for r in stream)
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
    n = 0
    with open(path,"wt",encoding=encoding) as f:
        for x in lines:
            f.write(str(x) + "\n")
            n += 1
    return n

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

def _jsonify(x):
    if type(x) in (list,dict):
        return json.dumps(s)
    else:
        return x

def jsonify(row):
    assert isinstance(row,list)
    return [_jsonify(_) for _ in row]

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


