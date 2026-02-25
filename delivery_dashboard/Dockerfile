# Utilisation d'une image Python légère
FROM python:3.9-slim

# Définition du dossier de travail dans le conteneur
WORKDIR /app

# Copie du fichier des dépendances
COPY requirements.txt .

# Installation des bibliothèques nécessaires
RUN pip install --no-cache-dir -r requirements.txt

# Copie de tout le code (app.py, templates, static)
COPY . .

# Exposition du port 5000 (celui de Flask)
EXPOSE 5000

# Commande pour lancer l'application
CMD ["python", "app.py"]