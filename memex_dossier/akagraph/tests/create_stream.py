from sys import stdout
from cbor import dump
import argparse

def identifier(i):
    return 'email', '{}@{}.com'.format(i,i)

def main(n):

    for i in range(n):
        rec = {'url': 'http://{}.localdomain/{}'.format(i,i)}
        k, v = identifier(i)
        rec[k]=[v]
        dump(rec, stdout)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("n", type=int, help="how many samples to create")
    args = parser.parse_args()
    main(args.n)
