import json, sys

with open('/Users/ares/Development/rust/caissify-explorer/doc/scripts/har.har') as f:
    har = json.load(f)

for entry in har['log']['entries']:
    u = entry['request']['url']
    if 'sync.com' in u and 'datakey=' in u:
        cookies = '; '.join(f"{c['name']}={c['value']}" for c in entry['request']['cookies'])
        print(f"PGN_URL={u}")
        print(f"PGN_COOKIE={cookies}")
        break
else:
    print("ERROR: no sync.com download URL found", file=sys.stderr)
    sys.exit(1)
