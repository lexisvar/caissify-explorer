import json, urllib.request, sys

with open('/Users/ares/Development/rust/caissify-explorer/har.har') as f:
    har = json.load(f)

url, cookies = None, None
for entry in har['log']['entries']:
    u = entry['request']['url']
    if 'sync.com' in u and 'datakey=' in u:
        url = u
        cookies = '; '.join(f"{c['name']}={c['value']}" for c in entry['request']['cookies'])
        break

if not url:
    print("ERROR: no sync.com URL in HAR", file=sys.stderr)
    sys.exit(1)

req = urllib.request.Request(url, headers={
    'Cookie': cookies,
    'Range': 'bytes=0-5242879',  # 5 MB
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
})

out = '/Users/ares/Development/rust/caissify-explorer/omotb_test.pgn'
with urllib.request.urlopen(req, timeout=120) as resp:
    print(f"HTTP {resp.status} | Content-Range: {resp.headers.get('Content-Range')}")
    data = resp.read(5 * 1024 * 1024)

# Trim to the last complete game so the PGN is not cut mid-game
last_game_end = data.rfind(b'\n\n\n')
if last_game_end != -1:
    data = data[:last_game_end + 3]

with open(out, 'wb') as f:
    f.write(data)

print(f"Saved {len(data):,} bytes  →  {out}")
print(f"Preview:\n{data[:200].decode('utf-8', errors='replace')}")
