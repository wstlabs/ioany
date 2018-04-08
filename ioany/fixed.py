

def reader(*args,**kwargs):
    return FWReader(*args,**kwargs)

def divine(spec):
    header = list(spec.keys())
    types = None
    return header,types

class FWReader(object):

    def __init__(self,f,spec):
        self.f = f
        self.spec = spec

    def __iter__(self):
        for line in self.f:
            yield line2vals(line,self.spec)

def line2vals(line,spec):
    return [val for k,val in parse(line,spec)]

def parse(line,spec):
    for k,xy in spec.items():
        x,y = xy
        yield k,line[x:y]

