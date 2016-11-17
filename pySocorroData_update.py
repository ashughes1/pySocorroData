import httplib, json, os, requests, sqlite3, sys, time, urllib2
from datetime import date, datetime, timedelta

def get_json(url):
    try:
        json = requests.get(url).json()
    except:
        print('WARNING: No data for {} | Reattempting...').format(url)
        json = ''
    return json

def process_json(i):
    yesterday = date.today()-timedelta(days=i+1)
    today = date.today()-timedelta(days=i)
    url = 'https://crash-stats.mozilla.com/api/SuperSearch/?proto_signature=gfx&_columns=build_id&_columns=cpu_arch&_columns=startup_crash&_columns=graphics_critical_error&_columns=graphics_startup_test&_columns=install_time&_columns=last_crash&_columns=safe_mode&_columns=release_channel&_columns=process_type&_columns=contains_memory_report&_columns=moz_crash_reason&_columns=date&_columns=product&_columns=version&_columns=platform_pretty_version&_columns=uuid&_columns=signature&_columns=proto_signature&_columns=app_notes&_columns=uptime&_columns=shutdown_progress&date=%3E%3D' + str(yesterday) + '&date=<' + str(today) + '&_results_number=1000&_results_offset=0'
    data = get_json(url)
    total = data['total']
    processed = 0
    rows = []
    print "[" + str(datetime.now()) + "] Querying Socorro for " + str(today) + "..."
    
    for index in range(total/1000+1):
        results_offset = index*1000
        if index != 0:
            url = url[:url.index('_results_offset=')] + '_results_offset=' + str(results_offset)
            data = get_json(url)
        for d in data['hits']:            
            row = {}
            
            # ID
            if d['uuid']:
                row['id'] = d['uuid'].encode('ascii','ignore')
            else:
                row['id'] = -1
            
            # DATE
            if d['date']:
                row['date'] = d['date'][:10].encode('ascii','ignore')
            else:
                row['date'] = -1
                
            # PRODUCT
            if d['product'] and d['version'] and d['build_id']:
                row['product'] = d['product'].encode('ascii','ignore') + ' ' + d['version'].encode('ascii','ignore') + ' ' + d['build_id'].encode('ascii','ignore')
            else:
                row['product'] = -1
                
            # PLATFORM
            if d['platform_pretty_version']:
                row['platform'] = d['platform_pretty_version'].encode('ascii','ignore')
                if d['cpu_arch']:
                    row['platform'] += ' ' + d['cpu_arch']
            else:
                row['platform'] = -1
                
            # SIGNATURE
            if d['proto_signature']:
                row['signature'] = d['proto_signature'].encode('ascii','ignore')
            else:
                row['signature'] = -1
            
            # TYPE
            if d['process_type']:
                row['type'] = d['process_type'].encode('ascii','ignore')
            else:
                row['type'] = "browser"
            if d['safe_mode'] == "1":
                row['type'] += ';safemode'
            if d['startup_crash'] == "1":
                row['type'] += ';startup'
            if d['shutdown_progress']:
                row['type'] += ';shutdown'
            if d['graphics_startup_test'] == "1":
                row['type'] += ';gst'
            if d['moz_crash_reason']:
                row['type'] += ';mozcrash'
            
            # UPTIME
            if d['uptime']:
                row['uptime'] = d['uptime']
            else:
                row['uptime'] = -1
                
            # LAST_CRASH
            if d['last_crash']:
                row['last_crash'] = d['last_crash']
            else:
                row['last_crash'] = -1
            
            # INSTALL_TIME
            if d['install_time']:
                row['install_time'] = int(d['install_time'])
            else:
                row['install_time'] = -1
            
            # NOTES
            if d['app_notes']:
                row['notes'] = d['app_notes'].encode('ascii','ignore')
                if d['graphics_critical_error']:
                    row['notes'] += ';' + d['graphics_critical_error'].encode('ascii','ignore')
            else:
                row['notes'] = ""
                    
            if row['id'] != -1 and row['date'] != -1 and row['product'] != -1 and row['platform'] != -1 and row['signature'] != -1 and row['uptime'] != -1 and row['install_time'] != -1 and row['notes'] != -1:
                rows.append(row)
            
            processed += 1
    print "[" + str(datetime.now()) + "] Found " + str(processed) + " items, " + str(len(rows)) + " valid for insertion."
    return [processed, add_rows_to_database(rows)]

def initialize_database():
    db_connection = sqlite3.connect('socorro.sqlite')
    db_cursor = db_connection.cursor()
    db_cursor.execute('CREATE TABLE IF NOT EXISTS crashes(id TEXT PRIMARY KEY, date TEXT, product TEXT, platform TEXT, signature TEXT, notes TEXT, type TEXT, uptime INTEGER, last_crash INTEGER, install_time INTEGER)')
    db_connection.commit()
    db_connection.close()
    return 1

def add_rows_to_database(rows):
    inserted = 0
    db_connection = sqlite3.connect('socorro.sqlite')
    db_cursor = db_connection.cursor()
    print "[" + str(datetime.now()) + "] Begin inserting " + str(len(rows)) + " items..."
    for i in range(0, len(rows)):
        row = rows[i]
        r = (row['id'], row['date'], row['product'], row['platform'], row['signature'], row['notes'], row['type'], row['uptime'], row['last_crash'], row['install_time'])
        db_cursor.execute('INSERT OR IGNORE INTO crashes VALUES(?,?,?,?,?,?,?,?,?,?)', r)
        inserted += db_cursor.rowcount
    db_connection.commit()
    db_connection.close()
    print "[" + str(datetime.now()) + "] " + str(inserted) + " new items inserted, " + str(len(rows)-inserted) + " duplicate items ignored."
    return inserted

print "###################################################################################"
total_inserted = 0
total_processed = 0
time_start = datetime.now()
initialize_database()
for i in range(179,0,-1):
    counts = process_json(i)
    total_processed += counts[0]
    total_inserted += counts[1]
    print ""
time_end = datetime.now()
print "RESULT: " + str(total_processed) + " items processed, " + str(total_inserted) + " new items added in " + str(time_end - time_start)






