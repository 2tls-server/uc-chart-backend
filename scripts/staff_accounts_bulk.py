import requests
import yaml

handles: list[int] = []
type = "mod"  # or unmod, admin, unadmin

BASE = "http://127.0.0.1:39000"

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

headers = {config["server"]["auth-header"]: config["server"]["auth"]}

for handle in handles:
    resp = requests.get(f"{BASE}/api/accounts/handle/{handle}/")
    if resp.status_code != 200:
        print(f"[{handle}] failed to resolve: {resp.status_code} {resp.text}")
        continue

    sonolus_id = resp.json()["sonolus_id"]
    resp = requests.patch(
        f"{BASE}/api/accounts/{sonolus_id}/staff/{type}/", headers=headers
    )
    print(f"[{handle}] {sonolus_id}: {resp.status_code} {resp.json()}")
