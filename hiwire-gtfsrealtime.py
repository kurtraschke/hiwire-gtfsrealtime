import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.text_format import MessageToString
from flask import Flask, request, make_response
from flask.ext.cache import Cache

import json
import time
from datetime import datetime

app = Flask(__name__)
app.debug = True
cache = Cache(app,config={'CACHE_TYPE': 'simple'})

default_endpoint = "http://realtimemap.mta.maryland.gov/RealTimeManager"

@cache.memoize(24 * 60 * 60)
def get_line_dir_ids(endpoint):
    r = requests.post(endpoint,
                      data=json.dumps({"method": "GetListOfLines",
                                       "version": "1.1"}),
                      headers={"Content-Type": "application/json"})

    r.raise_for_status()

    response =  r.json()

    line_dir_ids = [dir["lineDirId"]
                    for line in response["result"]["retLineWithDirInfos"]
                    for dir in line["drInfos"]]

    app.logger.info("Fetched %i lines", len(line_dir_ids))

    return line_dir_ids

@cache.memoize(30)
def get_active_trips(endpoint, line_dir_ids):
    r = requests.post(endpoint,
                      data=json.dumps({"method": "GetTravelPoints",
                                       "params": {"travelPointsReqs": [{"lineDirId": line_dir_id,
                                                                        "callingApp": "RMD"}
                                                                       for line_dir_id in line_dir_ids],
                                                  "interval": 10},
                                       "version": "1.1"}),
                      headers={"Content-Type": "application/json"})

    r.raise_for_status()

    response = r.json()

    active_trips = [{"trip_id": str(trip["TripId"]),
                     "delay": -1 * trip["ESchA"]}
                    for trip in response["result"]["travelPoints"]
                    if trip["VehicleStatus"] == 1 and trip["ESchA"] != -9999]

    app.logger.info("Fetched %i active trips", len(active_trips))
    
    return active_trips

@app.route("/trip-updates")
def get_trip_updates():
    endpoint = request.args.get('endpoint', default_endpoint)
    active_trips = get_active_trips(endpoint, get_line_dir_ids(endpoint))

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "1.0"
    feed.header.timestamp = int(time.mktime(datetime.now().timetuple()))

    for trip in active_trips:
        entity = feed.entity.add()
        entity.id = trip["trip_id"]
        entity.trip_update.trip.trip_id = trip["trip_id"]
        entity.trip_update.delay = trip["delay"]

    assert feed.IsInitialized()

    if (request.args.has_key('debug')):
        response = make_response(MessageToString(feed))
        response.headers['Content-Type'] = "text/plain"
    else:
        response = make_response(feed.SerializeToString())
        response.headers['Content-Type'] = "application/octet-stream"

    return response

if __name__ == "__main__":
    app.run()
