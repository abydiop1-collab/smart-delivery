import json
import time
import random
import os
from kafka import KafkaProducer

# ─── CONFIGURATION DAKAR ──────────────────────────────────────────
DAKAR_LAT = 14.7167
DAKAR_LON = -17.4677

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    api_version=(0, 10, 1)
)

# Liste des livreurs avec noms exacts de ton Dashboard
DRIVERS = {
    "DRV_001": {"name": "Amadou BA", "lat": DAKAR_LAT, "lon": DAKAR_LON},
    "DRV_002": {"name": "Mouhamed SOW", "lat": 14.7241, "lon": -17.4520},
    "DRV_003": {"name": "Ousmane DIOP", "lat": 14.7480, "lon": -17.4900},
    "DRV_004": {"name": "Badou NDIAYE", "lat": 14.7350, "lon": -17.4700}
}

def simulate_movement(pos):
    """Déplacement aléatoire autour de la position actuelle."""
    return {
        "lat": pos["lat"] + random.uniform(-0.002, 0.002),
        "lon": pos["lon"] + random.uniform(-0.002, 0.002)
    }

print("🇸🇳 Simulateur Smart Delivery Dakar - V2 (Vitesse incluse)")
print("==> Données envoyées : Nom, Batterie, Lat/Lon, Vitesse, Timestamp")
print("==> Appuyez sur Ctrl+C pour arrêter.\n")

try:
    while True:
        for driver_id, info in DRIVERS.items():
            # 1. Mise à jour de la position
            new_pos = simulate_movement(info)
            DRIVERS[driver_id]["lat"] = new_pos["lat"]
            DRIVERS[driver_id]["lon"] = new_pos["lon"]

            # 2. Simulation batterie
            # Ousmane DIOP (DRV_003) reste en alerte sur ton écran
            battery = random.randint(5, 14) if driver_id == "DRV_003" else random.randint(20, 100)
            
            # 3. Simulation Vitesse (km/h)
            vitesse = random.randint(15, 45)

            # 4. Création du message JSON complet
            message = {
                "driver_id": driver_id,
                "full_name": info["name"],
                "lat": round(new_pos["lat"], 6),
                "lon": round(new_pos["lon"], 6),
                "vitesse": vitesse,
                "battery": battery,
                "timestamp": int(time.time())
            }

            # 5. Envoi vers le topic 'delivery-locations'
            future = producer.send(
                topic='delivery-locations',
                key=driver_id,
                value=message
            )

            # Confirmation visuelle dans le terminal
            meta = future.get(timeout=10)
            status = "🚨" if battery < 15 else "✅"
            print(f"{status} {info['name']} | Vitesse: {vitesse} km/h | Batt: {battery}%")

        producer.flush()
        print(f"--- Attente 5 secondes ---\n")
        time.sleep(5)

except KeyboardInterrupt:
    print("\n==> Arrêt du simulateur.")
    producer.close()