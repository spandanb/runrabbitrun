import math
import dateutil.parser as dateparser

def _distance(lat0, lon0, lat1, lon1):
    "Computes (km) distance between (lat0, lon0)->(lat1, lon1)"
    R = 6371 #Radius of Earth

    dlat = math.radians(lat1-lat0)
    dlon = math.radians(lon1-lon0)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat0)) \
        * math.cos(math.radians(lat1)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d


def speed(p0, p1):
    dist = _distance(p0[0], p0[1], p1[0], p1[1])
    tdiff = (dateparser.parse(p1[6]) - dateparser.parse(p0[6])).total_seconds()
    if tdiff != 0:
        #speed (km/h)
        user_speed = 3600 * dist/tdiff 
    return user_speed

if __name__ == "__main__":
    p0 = [37.4263597,-122.1432371,0,1024,46000,"2016-10-07","00:45:29"]
    p1 = [37.4263197,-122.1432371,0,1024,46000,"2016-10-07","00:45:35"]
    print speed(p0, p1)
