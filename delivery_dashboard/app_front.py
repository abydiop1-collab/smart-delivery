from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import json
import threading

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Liste dynamique des livreurs à afficher
drivers_list = ["DRV_001", "DRV_002", "DRV_003", "DRV_004"]

def kafka_reader():
    """Lit les messages Kafka et les envoie au navigateur uniquement s'ils sont dans la liste."""
    try:
        consumer = KafkaConsumer(
            'delivery-locations',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            api_version=(0, 10, 1)
        )
        for message in consumer:
            data = message.value
            # FILTRE : On n'envoie le message que si le livreur est dans notre liste active
            if data['driver_id'] in drivers_list:
                socketio.emit('new_location', data)
                
    except Exception as e:
        print(f"Erreur Kafka: {e}")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/manage_driver', methods=['POST'])
def manage_driver():
    """Gère l'ajout ou la suppression via les boutons de l'interface."""
    data = request.json
    action = data.get('action')
    driver_id = data.get('driver_id')
    
    if action == 'add':
        if driver_id and driver_id not in drivers_list:
            drivers_list.append(driver_id)
            print(f"--- AJOUT : Le livreur {driver_id} est maintenant suivi sur la carte.")
    
    elif action == 'remove':
        if driver_id in drivers_list:
            drivers_list.remove(driver_id)
            print(f"--- SUPPRESSION : Le livreur {driver_id} a été retiré.")
            
    return jsonify(success=True)

if __name__ == '__main__':
    # Lancement du thread Kafka en arrière-plan
    threading.Thread(target=kafka_reader, daemon=True).start()
    print("Dashboard Smart Delivery lancé sur http://127.0.0.1:5000")
    socketio.run(app, port=5000, debug=False)