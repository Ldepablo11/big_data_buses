import requests
import time
import json
import base64

hbase_rest_url = "http://ec2-54-89-237-222.compute-1.amazonaws.com:8070"
api_bus_key = "bEz4dWVWnCtsTAjiQdgWcKB3q"

def safe(x):
    return "" if x is None else str(x)

def b64(x: str):
    return base64.b64encode(x.encode()).decode()

def pull_vehicles(route):
    url = (
        "https://www.ctabustracker.com/bustime/api/v3/getvehicles/"
        f"?key={api_bus_key}&format=json&tmres=s&rt={route}"
    )
    r = requests.get(url)
    data = r.json()

    if "vehicle" in data.get("bustime-response", {}):
        return data["bustime-response"]["vehicle"]

    return []

def pull_active_routes():
    url = (
        "https://www.ctabustracker.com/bustime/api/v3/getroutes"
        f"?key={api_bus_key}&format=json"
    )

    r = requests.get(url)
    data = r.json()

    routes = data.get("bustime-response", {}).get("routes", [])

    active_routes = []

    for rt in routes:
        if "rt" in rt:
            active_routes.append(rt["rt"])

    return active_routes

def write_to_hbase(vehicle):
    rowkey = f"{vehicle['vid']}_{int(time.time())}"

    columns = {
        "info:vid": vehicle.get("vid", ""),
        "info:rt": vehicle.get("rt", ""),
        "info:lat": str(vehicle.get("lat", "")),
        "info:lon": str(vehicle.get("lon", "")),
        "info:des": vehicle.get("des", ""),
        "info:hdg": str(vehicle.get("hdg", "")),
        "info:pdist": str(vehicle.get("pdist", "")),
        "info:dly": str(vehicle.get("dly", "")),
        "info:tmstmp": vehicle.get("tmstmp", "")
    }

    row = {
        "Row": [{
            "key": b64(rowkey),
            "Cell": [
                {
                    "column": b64(col),
                    "$": b64(val)
                }
                for col, val in columns.items()
            ]
        }]
    }

    url = f"{hbase_rest_url}/rt_vehicles_hb/{rowkey}"

    r = requests.put(url, json=row, headers={"Content-Type": "application/json"})

    if r.status_code not in (200, 201):
        print("HBase ERROR:", r.status_code, r.text)
    else:
        print("Inserted:", rowkey)

def refresh():
    routes = pull_active_routes()

    for rt in routes:
        vehicles = pull_vehicles(rt)
        print(f"Route {rt}: pulled {len(vehicles)} vehicles")
        for v in vehicles:
            write_to_hbase(v)

if __name__ == "__main__":
    while True:
        refresh()
        time.sleep(35)