# Image de l'équipe : Java 17 + Python 3.12 + PySpark 4.1.1
# (mêmes versions que sur nos Mac, pour éviter le "ça marche chez moi")

# 1. Système de base : Ubuntu 24.04 avec Java 17 déjà installé
FROM eclipse-temurin:17-jre-noble

# 2. Python 3.12 (fourni par Ubuntu 24.04) + venv + procps (utilisé par Spark)
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv procps \
 && rm -rf /var/lib/apt/lists/*

# 3. Environnement Python isolé, pour installer les bibliothèques proprement
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 4. Bibliothèques du projet (PySpark, PyYAML), lues dans requirements.txt
COPY climate-project/ingestion_batch/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 5. Dossier de travail : le projet y sera partagé depuis le Mac (voir docker-compose.yml)
WORKDIR /projet