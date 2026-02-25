"""
Étape D : Consommateur "Archiver" - Persistance des données
Lit TOUS les messages et sauvegarde le NOM RÉEL + ID 
dans une base SQLite ET un fichier CSV.
"""

import json
import csv
import sqlite3
import os
from datetime import datetime
from kafka import KafkaConsumer

# ─── Configuration ───────────────────────────────────────────────

DB_PATH = "deliveries_dakar.db"
CSV_PATH = "deliveries_dakar.csv"

# Consommateur : groupe DIFFÉRENT de l'alert-group
consumer = KafkaConsumer(
    'delivery-locations',
    bootstrap_servers=['localhost:9092'],
    group_id='archiver-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    api_version=(0, 10, 1)  # Stabilisation pour Windows
)

# ─── Initialisation SQLite ────────────────────────────────────────

def init_database():
    """Crée la table avec la colonne full_name si elle n'existe pas."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            driver_id   TEXT NOT NULL,
            full_name   TEXT,
            lat         REAL NOT NULL,
            lon         REAL NOT NULL,
            battery     INTEGER NOT NULL,
            timestamp   INTEGER,
            archived_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    print(f"==> Base SQLite initialisée avec support des noms : {DB_PATH}")

def save_to_sqlite(data):
    """Insère un enregistrement incluant le nom complet."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO positions (driver_id, full_name, lat, lon, battery, timestamp, archived_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data["driver_id"],
        data.get("full_name", "Inconnu"), # Récupère le nom envoyé par le producer
        data["lat"],
        data["lon"],
        data["battery"],
        data.get("timestamp"),
        datetime.now().isoformat()
    ))
    conn.commit()
    conn.close()

# ─── Initialisation CSV ───────────────────────────────────────────

def init_csv():
    """Crée le fichier CSV avec la nouvelle colonne full_name."""
    if not os.path.exists(CSV_PATH):
        with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["driver_id", "full_name", "lat", "lon", "battery", "timestamp", "archived_at"])
        print(f"==> Fichier CSV initialisé : {CSV_PATH}")

def save_to_csv(data):
    """Ajoute une ligne avec le nom complet dans le CSV."""
    with open(CSV_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            data["driver_id"],
            data.get("full_name", "Inconnu"),
            data["lat"],
            data["lon"],
            data["battery"],
            data.get("timestamp", ""),
            datetime.now().isoformat()
        ])

# ─── Main ─────────────────────────────────────────────────────────

init_database()
init_csv()

print("🇸🇳 Archiver Smart Delivery Dakar - Démarré")
print(f"Sauvegarde en cours dans {DB_PATH} et {CSV_PATH}...\n")

record_count = 0

try:
    for message in consumer:
        data = message.value
        full_name = data.get("full_name", data["driver_id"])

        # Sauvegarde dans les deux formats
        save_to_sqlite(data)
        save_to_csv(data)

        record_count += 1
        print(
            f"[ARCHIVÉ #{record_count}] {full_name.ljust(15)} | "
            f"Batt: {data['battery']}% | ({data['lat']}, {data['lon']}) → OK"
        )
except KeyboardInterrupt:
    print("\n==> Arrêt de l'archivage.")