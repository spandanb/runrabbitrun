import random
import uuid
import os
import datetime
import csv

#Seed locations
seeds = [
37.86406, -122.26555,
37.79667, -122.40379,
37.78952, -122.39427,
37.78995, -122.39474,
37.78668, -122.39988,
37.78719, -122.39902,
37.78769, -122.39978,
37.78707, -122.40045,
37.78429, -122.39442,
37.78318, -122.39296,
37.78154, -122.39219,
37.78240, -122.40233,
37.78237, -122.40529,
37.77677, -122.41073,
37.77763, -122.39214,
37.77661, -122.39217,
37.76715, -122.39362,
37.76118, -122.41635,
37.67393, -122.39443,
37.64803, -122.39322,
37.62813, -122.42868,
37.57847, -122.34838,
37.53776, -122.25367,
37.48630, -122.23869,
37.45238, -122.17975,
37.42129, -122.20530,
37.38609, -122.03642,
37.36691, -122.04007,
37.33182, -122.03118,
37.29514, -121.92611,
37.25958, -121.96269,
37.41928, -122.08856,
37.43016, -122.09822,
37.42327, -122.07057
]
seeds = [(seeds[i*2], seeds[i*2+1]) for i, s in enumerate(seeds[::2])]

basepath = "/home/ubuntu/Synthetic_Trajectories/"
epoch = datetime.datetime.utcfromtimestamp(0)

def _user_id():
    return str(random.randint(1000, 10000))
    
def _path_id():
    return str(random.randint(3000000000000, 4000000000000))

def _date():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def _time(seed):
    if not seed:
        return datetime.datetime.now().strftime("%H:%M:%S")
    else:
        oldt = datetime.datetime.strptime(seed, "%H:%M:%S")
        totsecs = (oldt - epoch).total_seconds()
        newt = random.randint(2, 7) + totsecs
        return datetime.datetime.fromtimestamp(newt).strftime("%H:%M:%S")

def cointoss(odds=0.5):
    return random.random < odds

def get_users():
    """
    Returns user names
    """
    return [child for child in os.listdir(basepath)
                if os.path.isdir(os.path.join(basepath, child))]

def write_traj(user_id, path_id, data):
    
    #if user dir DNE, create it
    userdir = os.path.join(basepath, user_id )
    if not os.path.isdir( userdir ):
        os.makedirs(userdir)
        os.makedirs(os.path.join(basepath, user_id, "Trajectory"))

    trajfile = "{}.plt".format(user_id)
    trajpath = os.path.join(basepath, user_id, "Trajectory", trajfile)

    with open(trajpath, 'wb') as fptr:
        writer = csv.writer(fptr)
        writer.writerows(data)


def _randwalk(x, y, step=0.00001, n=1000):
    """
    Generates a random walk starting 
    at point `x`, `y` (lat, long), with `step` size and takes
    `n` steps.
    Returns the vector of positions
    """
    
    xstep = step * random.choice([-1, 1])
    ystep = step * random.choice([-1, 1])

    posvect = [(x, y, 0, 1024, 46000, _date(), _time(None) )]

    for i in range(n):
        #skewed in one direction so there is an actual walk
        r = random.randint(-1, 5) 
        x += xstep * r
        r = random.randint(-1, 5) 
        y += ystep * r
        
        val = (x, y, 0, 1024, 46000, _date(), _time(posvect[-1][6]))

        posvect.append(val)

    return posvect

def randwalk():
    """
    """
    pathcount = 0
    while pathcount < 1000000:

        users = get_users()
        #pick existing
        if cointoss(odds=0.6) and users:
            user = random.choice(users)
        else:
            user = _user_id()
            
        path = _path_id
            
        seed = random.choice(seeds) 
        traj = _randwalk(seed[0], seed[1])

        write_traj(user, path, traj)

        pathcount += 1


if __name__ == "__main__":
    randwalk()    
