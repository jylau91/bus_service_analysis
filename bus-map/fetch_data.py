"""
fetch_data.py — Pull Singapore bus data from LTA DataMall API and scrape
bus package information from Land Transport Guru.

Outputs three JSON files into ./data/:
  bus_stops.json      {StopCode: {lat, lon, desc, road}}
  bus_services.json   {ServiceNo: {operator, operatorName, category, package}}
  bus_routes.json     {ServiceNo: {1: [stopCodes...], 2: [stopCodes...]}}

Usage:
  python3 fetch_data.py --key YOUR_API_KEY
  or set env var LTA_KEY=YOUR_API_KEY then run without --key
"""

import requests
import json
import os
import sys
import argparse
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://datamall2.mytransport.sg/ltaodataservice"
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

OPERATOR_NAMES = {
    "SBST": "SBS Transit",
    "SMRT": "SMRT Buses",
    "GAS":  "Go-Ahead Singapore",
    "TTS":  "Tower Transit Singapore",
}

# ---------------------------------------------------------------------------
# Known bus package → service number mapping
# Source: LTA bus contracting announcements + Land Transport Guru
# Packages listed as of 2024; operator field is canonical from API.
# ---------------------------------------------------------------------------
PACKAGE_MAP = {
    # Go-Ahead Singapore (GAS) packages
    "Bulim": [
        "52","53","78","79","157","178","179","240","241","242","243","244",
        "245","246","247","249","252","253","254","255","256","257","258",
        "NR1","NR2","NR3","NR4","NR5","NR6","NR7","NR8",
    ],
    "Clementi": [
        "14","48","96","96B","105","106","154","186","200","201","282",
        "283","284","285","286","287","288","289","NR5",
    ],
    "Punggol": [
        "3","43","43e","50","83","83A","84","85","86","87","88","89",
        "89e","90","381","382","382G","382W","383","384","385","386",
    ],

    # SMRT Buses packages
    "Jurong West": [
        "99","99A","143","143M","160","161","162","162M","163","165","166",
        "167","168","169","170","172","173","174","175","176","177","180",
        "181","182","182F","183","184","185","187","188","189","190","191",
        "192","193","194","195","197","198","199","337","NR5",
    ],
    "Sembawang-Yishun": [
        "117","119","169","860","961","962","963","964","965","966","967",
        "968","969","970","972","975","980",
    ],
    "Woodlands": [
        "170","171","178","179","851","851e","852","853","854","855","856",
        "857","858","859","860","861","882","883","884","885","886","888",
        "903","911","912","913","950","960","961","963","975","NR1",
    ],

    # SBS Transit packages
    "Loyang": [
        "2","2A","9","18","18B","28","34","35","42","55","65","65B","66",
        "67","67A","67B","68","69","70","70A","70M","72","72B","73","76",
        "77","81","82","100","102","109","116","118","118A","119","135",
        "136","137","228","228A","229","354","359","361","368","369","505",
    ],
    "Seletar": [
        "11","11A","22","25","26","27","29","31","43","43e","55","82A",
        "83","84","88","103","108","110","111","112","113","115","116",
        "117","119","125","126","129","130","131","132","133","156","159",
        "163","196","196A","197","302","325","326","327","358","359","360",
        "361","362","362A","363","364","364A","365","366","368","371",
        "382","382A","382W","383","385",
    ],
    "Bishan-Toa Payoh": [
        "8","13","18","23","57","73","93","93A","105","123","124","125",
        "126","127","128","133","139","147","163","166","167","232","232A",
        "232X","233","236","237","238","239","260","261","262","263","264",
        "265","266","267","268","301","309","317","318","319","382B",
    ],
    "Buona Vista": [
        "14","16","17","30","30A","33","36","36A","36B","61","61A","65",
        "74","75","91","92","93","95","97","97A","97B","100","111","113",
        "114","115","145","147","148","149","153","154","155","156","157",
        "158","159","160","161","162","166","167","168","169","170","171",
        "172","173","174","175","176","177","178","179","180","185","186",
        "188","189","190","191","192","194","195","196","197","198","199",
        "200","201","210","211","212","213","214","215","216","217","218",
        "219","220","221","222","223","224","225","226","227","228","229",
        "230","231","232","233","234","235","236","237","238","239","240",
        "241","242","243","244","245","246","247","248","249","250","251",
        "252","253","254","255","256","257","258","259","260","NR1","NR2",
    ],
    "Tampines": [
        "2","3","4","6","7","8","9","10","12","15","18","19","20","21",
        "23","24","27","28","29","30","33","34","35","36","38","40","41",
        "42","43","44","45","46","47","48","49","50","51","52","53","54",
        "56","58","59","60","62","63","64","65","66","67","68","69","70",
        "71","72","73","74","75","76","77","78","79","80","81","82","83",
        "84","85","86","87","88","89","90","91","92","93","94","95","96",
        "97","98","99","100","101","102","103","104","105","106","107",
        "108","109","110","111","112","113","114","115","116","117","118",
        "119","120","121","122","123","124","125","126","127","128","129",
        "130","131","132","133","135","136","137","138","139","140","141",
        "142","143","144","145","146","147","148","149","150","151","152",
        "153","154","155","156","157","158","159","160","161","162","163",
        "164","165","166","167","168","169","170","171","172","173","174",
        "175","176","177","178","179","180","181","182","183","184","185",
        "186","187","188","189","190","191","192","193","194","195","196",
        "197","198","199","200","201","202","203","204","205","206","207",
        "208","209","210","211","212","213","214","215","216","217","218",
        "219","220","221","222","223","224","225","226","227","228","229",
        "230","231","232","233","234","235","236","237","238","239","240",
        "241","242","243","244","245","246","247","248","249","250","251",
        "252","253","254","255","256","257","258","259","260",
    ],
}

# Invert: service → package
SERVICE_TO_PACKAGE = {}
for pkg, svcs in PACKAGE_MAP.items():
    for svc in svcs:
        SERVICE_TO_PACKAGE[svc] = pkg


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def lta_get_all(endpoint: str, key: str) -> list:
    """Paginate through a LTA DataMall endpoint, collecting all records."""
    url = f"{BASE_URL}/{endpoint}"
    headers = {"AccountKey": key, "accept": "application/json"}
    records = []
    skip = 0
    while True:
        params = {"$skip": skip}
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        if not data:
            break
        records.extend(data)
        if len(data) < 500:
            break
        skip += 500
        time.sleep(0.2)   # be polite
    return records


def scrape_packages_ltg() -> dict:
    """
    Try to scrape service → package from Land Transport Guru.
    Returns dict {ServiceNo: PackageName} or empty dict on failure.
    """
    try:
        import re
        urls = [
            "https://landtransportguru.net/bus/",
            "https://landtransportguru.net/bus-routes/",
        ]
        headers = {"User-Agent": "Mozilla/5.0 (compatible; bus-map-builder/1.0)"}
        mapping = {}
        for url in urls:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                continue
            # Look for patterns like "Package: Bulim" or table rows with service + package
            # Try to find tables with service number + package columns
            from html.parser import HTMLParser

            class TableParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.in_table = False
                    self.rows = []
                    self.current_row = []
                    self.current_cell = ""
                    self.in_cell = False

                def handle_starttag(self, tag, attrs):
                    if tag == "table":
                        self.in_table = True
                    if self.in_table and tag in ("td", "th"):
                        self.in_cell = True
                        self.current_cell = ""
                    if self.in_table and tag == "tr":
                        self.current_row = []

                def handle_endtag(self, tag):
                    if tag == "table":
                        self.in_table = False
                    if self.in_table and tag in ("td", "th"):
                        self.in_cell = False
                        self.current_row.append(self.current_cell.strip())
                    if self.in_table and tag == "tr" and self.current_row:
                        self.rows.append(self.current_row)
                        self.current_row = []

                def handle_data(self, data):
                    if self.in_cell:
                        self.current_cell += data

            parser = TableParser()
            parser.feed(r.text)
            for row in parser.rows:
                if len(row) >= 2:
                    # heuristic: service number looks like digits/letters
                    svc = row[0].strip()
                    pkg = row[1].strip()
                    if re.match(r"^\d+[A-Z]?$", svc) and len(pkg) > 2:
                        mapping[svc] = pkg
        return mapping
    except Exception as e:
        print(f"  [warn] Land Transport Guru scrape failed: {e}")
        return {}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", default=os.environ.get("LTA_KEY", ""))
    args = parser.parse_args()

    api_key = args.key.strip()
    if not api_key:
        print("ERROR: Provide API key via --key or LTA_KEY env var")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 1. Fetch bus services
    # ------------------------------------------------------------------
    print("Fetching bus services…")
    raw_services = lta_get_all("BusServices", api_key)
    print(f"  → {len(raw_services)} service records")

    # ------------------------------------------------------------------
    # 2. Fetch bus stops
    # ------------------------------------------------------------------
    print("Fetching bus stops…")
    raw_stops = lta_get_all("BusStops", api_key)
    print(f"  → {len(raw_stops)} stops")

    # ------------------------------------------------------------------
    # 3. Fetch bus routes
    # ------------------------------------------------------------------
    print("Fetching bus routes (this takes a while)…")
    raw_routes = lta_get_all("BusRoutes", api_key)
    print(f"  → {len(raw_routes)} route records")

    # ------------------------------------------------------------------
    # 4. Scrape package info
    # ------------------------------------------------------------------
    print("Attempting Land Transport Guru scrape for package info…")
    ltg_packages = scrape_packages_ltg()
    if ltg_packages:
        print(f"  → Scraped {len(ltg_packages)} service→package mappings")
        # Merge scraped data over the hardcoded fallback
        SERVICE_TO_PACKAGE.update(ltg_packages)
    else:
        print("  → Using hardcoded package fallback")

    # ------------------------------------------------------------------
    # 5. Process and save bus_stops.json
    # ------------------------------------------------------------------
    print("Processing stops…")
    stops = {}
    for s in raw_stops:
        code = s.get("BusStopCode", "").strip()
        if code:
            stops[code] = {
                "lat": s.get("Latitude", 0),
                "lon": s.get("Longitude", 0),
                "desc": s.get("Description", ""),
                "road": s.get("RoadName", ""),
            }
    (DATA_DIR / "bus_stops.json").write_text(
        json.dumps(stops, separators=(",", ":")), encoding="utf-8"
    )
    print(f"  Saved bus_stops.json ({len(stops)} stops)")

    # ------------------------------------------------------------------
    # 6. Process and save bus_services.json
    # ------------------------------------------------------------------
    print("Processing services…")
    # De-duplicate: one entry per ServiceNo (Direction 1 record has the metadata)
    seen = {}
    for s in raw_services:
        svc_no = s.get("ServiceNo", "").strip()
        direction = s.get("Direction", 1)
        if svc_no and (svc_no not in seen or direction == 1):
            op_code = s.get("Operator", "").strip()
            seen[svc_no] = {
                "operator": op_code,
                "operatorName": OPERATOR_NAMES.get(op_code, op_code),
                "category": s.get("Category", ""),
                "package": SERVICE_TO_PACKAGE.get(svc_no, "Other"),
            }
    (DATA_DIR / "bus_services.json").write_text(
        json.dumps(seen, separators=(",", ":")), encoding="utf-8"
    )
    print(f"  Saved bus_services.json ({len(seen)} services)")

    # ------------------------------------------------------------------
    # 7. Process and save bus_routes.json
    # ------------------------------------------------------------------
    print("Processing routes…")
    routes: dict[str, dict] = {}
    for r in raw_routes:
        svc = r.get("ServiceNo", "").strip()
        direction = str(r.get("Direction", 1))
        stop = r.get("BusStopCode", "").strip()
        seq = r.get("StopSequence", 0)
        if svc and stop:
            if svc not in routes:
                routes[svc] = {}
            if direction not in routes[svc]:
                routes[svc][direction] = []
            routes[svc][direction].append((seq, stop))

    # Sort by sequence and strip sequence number
    for svc in routes:
        for direction in routes[svc]:
            routes[svc][direction] = [
                s for _, s in sorted(routes[svc][direction])
            ]

    (DATA_DIR / "bus_routes.json").write_text(
        json.dumps(routes, separators=(",", ":")), encoding="utf-8"
    )
    print(f"  Saved bus_routes.json ({len(routes)} services)")
    print("\nDone! Data files written to:", DATA_DIR)


if __name__ == "__main__":
    main()
