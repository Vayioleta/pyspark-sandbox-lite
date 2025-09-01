# Mini APACHE SPARK EN DOCKER

## Inicio Rápido

### 1) Levantar el contenedor
```bash
docker compose up -d
```

### 2) Entrar a la terminal del contenedor
```bash
docker compose exec spark bash
```
### 3) Ejecutar scripts
```bash
# Script básico
spark-submit --master local[*] load_data.py
```

```bash
# Ejecutar comandos directamente
docker compose exec spark python3 /workspace/load_data.py

# Instalar dependencias adicionales (si es necesario)
docker compose exec spark pip install -r requirements.txt
```
