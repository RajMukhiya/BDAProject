import requests

for host in ["localhost", "master", "127.0.0.1"]:
    try:
        r = requests.get(f"http://{host}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState", timeout=3)
        if r.status_code == 200:
            beans = r.json().get("beans", [])
            if beans:
                b = beans[0]
                print(f"--- NameNode JMX Telemetry (via {host}:9870) ---")
                for k in sorted(b.keys()):
                    print(f"{k}: {b[k]}")
            break
    except Exception:
        continue

