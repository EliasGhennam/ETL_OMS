# Guide de Déploiement ETL-OMS

## Configuration des Secrets GitHub

Pour que le déploiement automatique fonctionne, vous devez configurer les secrets suivants dans votre repository GitHub :

### 1. Secrets Docker Hub (optionnel)
```
DOCKER_USERNAME=votre_username_docker_hub
DOCKER_PASSWORD=votre_password_docker_hub
```

### 2. Secrets Serveur de Production
```
SERVER_HOST=ip_ou_domaine_du_serveur
SERVER_USER=utilisateur_ssh
SSH_PRIVATE_KEY=clé_privée_ssh_complète
```

### 3. Secrets Base de Données
```
POSTGRES_USER=utilisateur_postgres
POSTGRES_PASSWORD=mot_de_passe_postgres_sécurisé
```

### 4. Secrets Notifications (optionnel)
```
SLACK_WEBHOOK=url_webhook_slack
DISCORD_WEBHOOK=url_webhook_discord
```

## Configuration du Serveur de Production

### 1. Prérequis
```bash
# Installation de Docker et Docker Compose
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Installation de Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

### 2. Structure des Dossiers
```bash
mkdir -p /app/etl-oms
cd /app/etl-oms

# Créer le dossier SSL pour les certificats (optionnel)
mkdir -p ssl
```

### 3. Variables d'Environnement
Créer un fichier `.env` :
```bash
# Base de données
POSTGRES_USER=etloms_user
POSTGRES_PASSWORD=MotDePasseSecurise123!

# URLs des APIs (pour le frontend)
NEXT_PUBLIC_API_ETL_URL=http://votre-domaine.com/api/etl
NEXT_PUBLIC_API_IA_URL=http://votre-domaine.com/api/ia
NEXT_PUBLIC_API_JAVA_URL=http://votre-domaine.com/api/java
```

## Déploiement Manuel

### 1. Build des Images
```bash
# Build de toutes les images
docker build -t etl-oms-api-etl:latest ./API-ETL
docker build -t etl-oms-api-ia:latest ./API-IA
docker build -t etl-oms-api-java:latest ./API-JAVA/API-Pandemie
docker build -t etl-oms-frontend:latest ./FRONT/front_next
```

### 2. Lancement en Production
```bash
# Copier les fichiers de configuration
cp docker-compose.prod.yml /app/etl-oms/
cp nginx.conf /app/etl-oms/

# Lancer les services
cd /app/etl-oms
docker-compose -f docker-compose.prod.yml up -d
```

### 3. Vérification
```bash
# Vérifier l'état des services
docker-compose -f docker-compose.prod.yml ps

# Vérifier les logs
docker-compose -f docker-compose.prod.yml logs -f

# Test de santé
curl http://localhost/health
```

## Monitoring et Maintenance

### 1. Logs
```bash
# Logs de tous les services
docker-compose -f docker-compose.prod.yml logs -f

# Logs d'un service spécifique
docker-compose -f docker-compose.prod.yml logs -f frontend
```

### 2. Sauvegarde Base de Données
```bash
# Créer un script de sauvegarde
cat > /app/etl-oms/backup.sh << 'EOF'
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
docker exec etl-oms-postgres pg_dump -U $POSTGRES_USER pandemie > backup_$DATE.sql
gzip backup_$DATE.sql
EOF

chmod +x /app/etl-oms/backup.sh

# Ajouter au crontab (sauvegarde quotidienne à 2h du matin)
echo "0 2 * * * /app/etl-oms/backup.sh" | crontab -
```

### 3. Mise à Jour
```bash
# Arrêter les services
docker-compose -f docker-compose.prod.yml down

# Pull des nouvelles images
docker-compose -f docker-compose.prod.yml pull

# Redémarrer avec les nouvelles images
docker-compose -f docker-compose.prod.yml up -d

# Nettoyer les anciennes images
docker system prune -f
```

## Sécurité

### 1. Firewall
```bash
# Ouvrir seulement les ports nécessaires
sudo ufw allow 22/tcp    # SSH
sudo ufw allow 80/tcp    # HTTP
sudo ufw allow 443/tcp   # HTTPS
sudo ufw enable
```

### 2. SSL/TLS (Recommandé)
```bash
# Installer Certbot
sudo apt install certbot

# Obtenir un certificat Let's Encrypt
sudo certbot certonly --standalone -d votre-domaine.com

# Copier les certificats
sudo cp /etc/letsencrypt/live/votre-domaine.com/fullchain.pem /app/etl-oms/ssl/cert.pem
sudo cp /etc/letsencrypt/live/votre-domaine.com/privkey.pem /app/etl-oms/ssl/key.pem
sudo chown $USER:$USER /app/etl-oms/ssl/*

# Décommenter la section HTTPS dans nginx.conf
# Redémarrer nginx
docker-compose -f docker-compose.prod.yml restart nginx
```

## Troubleshooting

### Problèmes Courants

1. **Services ne démarrent pas**
   ```bash
   # Vérifier les logs
   docker-compose -f docker-compose.prod.yml logs
   
   # Vérifier l'espace disque
   df -h
   
   # Vérifier la mémoire
   free -h
   ```

2. **Base de données inaccessible**
   ```bash
   # Vérifier la connexion PostgreSQL
   docker exec -it etl-oms-postgres psql -U $POSTGRES_USER -d pandemie
   
   # Vérifier les variables d'environnement
   docker-compose -f docker-compose.prod.yml config
   ```

3. **Frontend ne se charge pas**
   ```bash
   # Vérifier les variables d'environnement Next.js
   docker exec -it etl-oms-frontend env | grep NEXT_PUBLIC
   
   # Vérifier la configuration Nginx
   docker exec -it etl-oms-nginx nginx -t
   ```

### Commandes Utiles
```bash
# Redémarrer un service
docker-compose -f docker-compose.prod.yml restart service_name

# Voir les ressources utilisées
docker stats

# Nettoyer l'espace
docker system prune -a

# Vérifier les volumes
docker volume ls
``` 