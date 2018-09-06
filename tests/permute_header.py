import ioany

def main():
    infile = "data/this.csv"
    inrecs = ioany.read_recs(infile)
    ioany.save_recs("data/bad.csv",inrecs,header=['bbb','aaa'])

if __name__ == '__main__':
    main()
