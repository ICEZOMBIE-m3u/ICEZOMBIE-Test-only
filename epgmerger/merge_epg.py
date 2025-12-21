#!/usr/bin/env python3
import gzip
import requests
import xml.etree.ElementTree as ET
from io import BytesIO
import time
import schedule

# ✅ Add your EPG URLs here
EPG_URLS = [
    "https://epgshare01.online/epgshare01/epg_ripper_US2.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_US_LOCALS1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_PLEX1.xml.gz", 
    "https://epgshare01.online/epgshare01/epg_ripper_PEACOCK1.xml.gz",
    "https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz",
    "https://github.com/matthuisman/i.mjh.nz/raw/refs/heads/master/all/epg.xml.gz",
    "https://raw.githubusercontent.com/BuddyChewChew/tubi-scraper/refs/heads/main/tubi_epg.xml",
    "https://github.com/BuddyChewChew/xumo-playlist-generator/raw/refs/heads/main/playlists/xumo_epg.xml.gz"
]

OUTPUT_FILE = "merged.xml.gz"


def download_and_decompress(url):
    print(f"Downloading: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    # Decompress .gz
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
        xml_data = gz.read()

    return xml_data


def merge_epgs():
    print("Starting merge...")

    merged_tv = ET.Element("tv")
    channels_seen = set()
    programmes_seen = set()

    for url in EPG_URLS:
        try:
            xml_data = download_and_decompress(url)
            root = ET.fromstring(xml_data)

            # ✅ Merge channels
            for channel in root.findall("channel"):
                channel_id = channel.get("id")
                if channel_id not in channels_seen:
                    merged_tv.append(channel)
                    channels_seen.add(channel_id)

            # ✅ Merge programmes
            for programme in root.findall("programme"):
                key = (
                    programme.get("start"),
                    programme.get("stop"),
                    programme.get("channel")
                )
                if key not in programmes_seen:
                    merged_tv.append(programme)
                    programmes_seen.add(key)

        except Exception as e:
            print(f"Error processing {url}: {e}")

    # ✅ Write merged XML and compress it
    tree = ET.ElementTree(merged_tv)
    xml_bytes = BytesIO()
    tree.write(xml_bytes, encoding="utf-8", xml_declaration=True)

    with gzip.open(OUTPUT_FILE, "wb") as f:
        f.write(xml_bytes.getvalue())

    print(f"✅ Merge complete → {OUTPUT_FILE}")


def job():
    print("Running scheduled EPG merge...")
    merge_epgs()


# ✅ Run immediately once
merge_epgs()

# ✅ Schedule every 8 hours
schedule.every(8).hours.do(job)

print("Scheduler started (every 8 hours)...")

while True:
    schedule.run_pending()
    time.sleep(30)
