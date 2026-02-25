"""
Étape C : Consommateur "Alert-Service" - Processeur de Filtrage
Lit le flux delivery-locations et détecte les batteries critiques (< 15%).
Envoie une alerte avec le NOM RÉEL dans le topic 'urgent-maintenance'.
"""

import json
from kafka import KafkaConsumer, KafkaProducer

# ─── Configuration ───────────────────────────────────────────────

BATTERY_THRESHOLD = 15  # Seuil d'alerte critique (%)

# Consommateur : membre du groupe "alert-group"
consumer = KafkaConsumer(
    'delivery-locations',
    bootstrap_servers=['localhost:9092'],
    group_id='alert-group',          # ← Consumer Group dédié aux alertes
    auto_offset_reset='earliest',    # Lit depuis le début si pas d'offset sauvegardé
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    api_version=(0, 10, 1)           # Stabilisation de la connexion
)

# Producteur : pour envoyer vers le topic urgent-maintenance
alert_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    api_version=(0, 10, 1)           # Stabilisation de la connexion
)

print("🇸🇳 Alert-Service Smart Delivery Dakar - Démarré")
print(f"    Surveillance des batteries < {BATTERY_THRESHOLD}%\n")

# ─── Traitement des messages ──────────────────────────────────────

for message in consumer:
    data = message.value
    driver_id = data.get("driver_id")
    # On récupère le nom complet envoyé par le producteur
    full_name = data.get('full_name', driver_id) 
    battery = data.get("battery")

    # Filtrage : vérification de la batterie
    if battery is not None and battery < BATTERY_THRESHOLD:
        # ⚠️  ALERTE CRITIQUE : on inclut le NOM dans l'alerte
        alert = {
            **data,
            "alert_type": "LOW_BATTERY",
            "alert_message": f"Batterie critique : {battery}% pour {full_name}"
        }

        # Affichage console avec le NOM
        print(
            f"🚨 ALERTE CRITIQUE | {full_name} ({driver_id}) | "
            f"Batterie: {battery}% | "
            f"Position: ({data.get('lat')}, {data.get('lon')})"
        )

        # Envoi vers le topic urgent-maintenance
        alert_producer.send(
            topic='urgent-maintenance',
            key=driver_id,
            value=alert
        )
        alert_producer.flush()
        print(f"   ↳ Alerte envoyée vers le topic 'urgent-maintenance'")

    else:
        # Message normal : batterie OK (Affichage avec le NOM)
        print(
            f"✅ OK          | {full_name.ljust(15)} | "
            f"Batterie: {battery}% | "
            f"Partition: {message.partition}"
        )