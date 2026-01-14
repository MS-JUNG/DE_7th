
import sys
import re

# Reads each line from stdin, extracts words, and outputs "word\t1".
word_re = re.compile(r"[A-Za-z0-9']+")

for line in sys.stdin:
    for w in word_re.findall(line):
        print(f"{w.lower()}\t1")
