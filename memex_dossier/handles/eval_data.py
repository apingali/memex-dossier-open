
import json
import os

s1 = '''
    '''
s2 = ''

usernames_with_saved_data = [
]

def findall(path):
    texts = []
    return texts

def load_eval_data(username):
    #yield (s1, map(lambda x: x.strip(), s1.split('|')))
    #yield (s2, s2.split())
    path = os.path.join(os.path.dirname(__file__), 'tests/data', username)
    for root, dirs, fnames in os.walk(path):
        for fname in fnames:
            if fname.endswith('.json'):
                fpath = os.path.join(root, fname)
                expected = json.load(open(fpath))
                fpath = fpath[:-5] + '.html'
                raw_data = open(fpath).read()
                yield raw_data, expected
