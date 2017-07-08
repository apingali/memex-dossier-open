
import json
import sys

for line in sys.stdin:
    data = json.loads(line)
    for un in data.get('username', []):
        print un
