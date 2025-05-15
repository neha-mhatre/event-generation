#!/usr/bin/python

import sqlite3
import msgpack

VERBOSE = False
def dprint(debugMsg):
    if VERBOSE:
        print(debugMsg)

conn = sqlite3.connect('/var/data/events.db')
dprint("Opened database successfully\n")

# Fetch latest 3 entries per event_type
for event_type in range(1, 42):
    print("\n\n****************************************************************************")
    print("Current event type: ", event_type)
    print("****************************************************************************")

    # Get the latest 3 entries for this event_type based on boot_seq
    cursor = conn.execute(f"""
        SELECT boot_seq, event_id, event_type, uptime_sec, uptime_nsec, body
        FROM events
        WHERE event_type = ?
        ORDER BY boot_seq DESC
        LIMIT 3
    """, (event_type,))

    for row in cursor:
        with open("data.msgpack", "wb") as dataFile:
            dataFile.write(row[5])

        with open("data.msgpack", "rb") as file:
            unp = msgpack.Unpacker(file)
            for data in unp:
                print(data)
                print("----------------------------------------------------------------------------")

dprint("\n\nOperation done successfully")
conn.close()

