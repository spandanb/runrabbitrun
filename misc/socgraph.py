
import pdb
"""
Reads the facebook network data
"""


def socgraph():
    """
    Reads FB data and maps username -> [u0, ..., uk], where ui is the ith connection.
    """
    filepath = "/home/ubuntu/facebook/0.edges"

    with open(filepath) as fptr:
        paths = {}
        for line in fptr: 
            a, b = line.strip().split()
            a = int(a)
            b = int(b)
            if a not in paths:
                paths[a] = []
            if b not in paths:
                paths[b] = []
            paths[a].append(b)
            paths[b].append(a)

    pdb.set_trace()

if __name__ == "__main__":
    socgraph()
