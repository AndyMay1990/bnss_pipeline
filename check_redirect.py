import requests

s = requests.Session()
r = s.get("https://asmodeus.free.nf/index.php?i=1", timeout=30, allow_redirects=False)
print(r.status_code, r.headers.get("Location"))
