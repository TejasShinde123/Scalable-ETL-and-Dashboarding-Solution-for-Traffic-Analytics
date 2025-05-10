import os, json, random
from datetime import datetime

def generate_sample_traffic_data():
    data = {
        "timestamp": datetime.utcnow().isoformat(),
        "region": random.choice(["East", "West", "North", "South"]),
        "vehicle_type": random.choice(["Car", "Bus", "Truck", "Bike"]),
        "count": random.randint(50, 500),
        "road_name": random.choice(["Highway 1", "Ring Road", "Expressway"]),
    }
    return data

if __name__ == "__main__":
    output_path = "./dags/raw/traffic_raw_data.json"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    sample_data = [generate_sample_traffic_data() for _ in range(10)]
    with open(output_path, "w") as f:
        json.dump(sample_data, f, indent=2)
    print("Synthetic traffic data generated.")
