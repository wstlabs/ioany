
def lzip(*args):
    """A variant of the zip builtin which acts as a left (rather than inner) join on
    the two input sequences.  That is, unlike zip which only emits up to the minimum
    capacity of the two respective iterators, this version always emits up to the
    capacity of the first iterator, slotting in None values when the second iterator
    is exhausted.

    Note that unlike the zip builtin which can take an open-ended number of arguments,
    this function always takes exactly two arguments.
    """
    if len(args) != 2:
        raise ValueError("invalid usage - takes exactly 2 args")
    xx,yy = iter(args[0]),iter(args[1])
    for x in xx:
        try:
            y = next(yy)
            yield x,y
        except StopIteration as e:
            yield x,None
    for x in xx:
        yield x,None

