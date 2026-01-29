# ETL OpenFoodFacts - Datamart Nutritionnel

Projet de création d'un datamart nutritionnel à partir des données OpenFoodFacts avec PySpark.

## Installation

### 1. Créer l'environnement virtuel
```bash
python -m venv lib/venv
```

### 2. Activer l'environnement
**Windows :**
```bash
lib\venv\Scripts\activate
```

**Linux/Mac :**
```bash
source lib/venv/bin/activate
```

### 3. Installer les dépendances
```bash
pip install -r requirements.txt
```

### 3.5 Configurer postgres
```
DB_PASSWORD = "postgres"  # Mots de passe par defaut, a modifier selon votre configuration
```

### 4. Lancer Jupyter
```bash
jupyter notebook
```

## Fichier de Données

### Où placer le fichier ?
Le fichier CSV doit être placé dans le dossier :
```
data/bronze/
```

### Nom du fichier
Par défaut, le notebook cherche :
```
en.openfoodfacts.org.products_echantillon_10000.csv
```

### Configuration
```
Le driver postgresql-42.7.2 dois etre place a l'emplacement suivant: venv\Lib\site-packages\pyspark\jars
```

### Modifier le fichier source

Pour utiliser un autre fichier, modifier la **CELLULE 3** du notebook :

```python
# CELLULE 3 - Configuration
INPUT_PATH = "data/bronze/en.openfoodfacts.org.products_echantillon_10000.csv"  # ← MODIFIER ICI
```

**Exemples :**
```python
# Fichier 3000 lignes
INPUT_PATH = "data/bronze/en.openfoodfacts.org.products_echantillon_3000.csv"

# Fichier 10000 lignes
INPUT_PATH = "data/bronze/en.openfoodfacts.org.products_echantillon_10000.csv"

# Autre fichier
INPUT_PATH = "data/bronze/mon_fichier.csv"
```

### Séparateurs supportés
Le notebook détecte **automatiquement** le séparateur :
- `;` (point-virgule)
- `\t` (tabulation)
- `,` (virgule)


## 🔧 Pipeline ETL

```
Bronze (CSV brut)
    ↓
Silver (nettoyé, validé, dédoublonné)
    ↓
Gold (schéma en étoile)
    ├── dim_brand
    ├── dim_country
    ├── dim_category
    ├── dim_time
    ├── dim_product
    └── fact_nutrition_snapshot
```

## Règles de Qualité Appliquées

### Filtrage
- Code et nom produit obligatoires
- Minimum 3 nutriments renseignés
- Dédoublonnage sur le code

### Bornes des Nutriments (pour 100g)
| Nutriment | Min | Max |
|-----------|-----|-----|
| Énergie (kcal) | 0 | 900 |
| Lipides | 0 | 100 |
| Glucides/Sucres | 0 | 100 |
| Protéines | 0 | 100 |
| Sel | 0 | 100 |
| Fibres | 0 | 100 |
| Sodium | 0 | 40 |

### Anomalies (lignes supprimées si dépassé)
- Sucres > 80g
- Sel > 25g
- Protéines > 90g
- Somme nutriments > 110g

### Cohérence
- Graisses saturées ≤ Lipides
- Harmonisation sel ↔ sodium (facteur 2.5)

## Notes

- Les valeurs "unknown" sont remplacées par des cases vides
- Toutes les valeurs numériques sont arrondies à 2 décimales
- Le séparateur de sortie est configurable (par défaut `;`)

