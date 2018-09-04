import sys
import ioany

infile = sys.argv[1]
outfile = sys.argv[2]

df = ioany.load_csv(infile)
recs = df.pick(len(df))

ioany.save_recs(outfile,recs)

