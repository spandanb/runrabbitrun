import json, pdb
import random

def genwalk():
    """
    Generate walking data. Using randwalk with step=0.0001, n=50
    generates walks about 0.5 miles.
    """
    gis_file = "./data/TRAFFIC_Signals.geojson"
    path_gis_file = "./data//walked_paths.geojson"

    with open(gis_file) as fptr:
        fdata = json.load(fptr)
        outf = open(path_gis_file, "w") #output file

        for point in fdata["features"]:
            res = randwalk(*point["geometry"]["coordinates"])
            outf.write(json.dumps(res))
            outf.write("\n")

        outf.close()

def randwalk(x, y, step=0.0001, n=50):
    """
    Generates a random walk starting 
    at point `x`, `y` (lat, long), with `step` size and takes
    `n` steps.
    Returns the vector of positions
    """
    
    xstep = step * random.choice([-1, 1])
    ystep = step * random.choice([-1, 1])

    posvect = [(x, y)]

    for i in range(n):
        #skewed in one direction so there is an actual walk
        r = random.randint(-1, 5) 
        x += xstep * r
        r = random.randint(-1, 5) 
        y += ystep * r
        posvect.append((x, y))

    return posvect


if __name__ == "__main__":
    genwalk()
