"""
Script de creation de la base de donnees PostgreSQL pour OpenFoodFacts
Executer ce script UNE SEULE FOIS avant de lancer le notebook ETL
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ============================================
# CONFIGURATION
# ============================================
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "postgres"
DB_PASSWORD = "postgres"  # Modifier ici
DB_NAME = "openfoodfacts_dw"

# ============================================
# ETAPE 1 : Creer la base de donnees
# ============================================
def create_database():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        exists = cursor.fetchone()

        if exists:
            cursor.execute(f"""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = '{DB_NAME}'
                AND pid <> pg_backend_pid();
            """)
            cursor.execute(f"DROP DATABASE {DB_NAME}")
            print(f"Base '{DB_NAME}' supprimee")

        cursor.execute(f"CREATE DATABASE {DB_NAME} WITH ENCODING = 'UTF8' TEMPLATE = template0;")
        print(f"Base '{DB_NAME}' creee (UTF-8)")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Erreur creation base: {e}")
        return False

# ============================================
# ETAPE 2 : Creer les tables avec FK et index
# ============================================
def create_tables():
    ddl = """
    -- Supprimer les tables existantes
    DROP TABLE IF EXISTS fact_nutrition_snapshot CASCADE;
    DROP TABLE IF EXISTS bridge_product_category CASCADE;
    DROP TABLE IF EXISTS dim_product CASCADE;
    DROP TABLE IF EXISTS dim_nutri CASCADE;
    DROP TABLE IF EXISTS dim_category CASCADE;
    DROP TABLE IF EXISTS dim_country CASCADE;
    DROP TABLE IF EXISTS dim_brand CASCADE;
    DROP TABLE IF EXISTS dim_time CASCADE;

    -- dim_time
    CREATE TABLE dim_time (
        time_sk BIGINT PRIMARY KEY,
        date DATE NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        iso_week INTEGER NOT NULL
    );

    -- dim_brand
    CREATE TABLE dim_brand (
        brand_sk BIGINT PRIMARY KEY,
        brand_name VARCHAR(500) NOT NULL
    );

    -- dim_country
    CREATE TABLE dim_country (
        country_sk BIGINT PRIMARY KEY,
        country_code VARCHAR(100) NOT NULL,
        country_name_fr VARCHAR(255)
    );

    -- dim_category
    CREATE TABLE dim_category (
        category_sk BIGINT PRIMARY KEY,
        category_code VARCHAR(500) NOT NULL,
        category_name_fr VARCHAR(500),
        level INTEGER,
        parent_category_sk BIGINT REFERENCES dim_category(category_sk)
    );

    -- dim_nutri
    CREATE TABLE dim_nutri (
        nutri_sk BIGINT PRIMARY KEY,
        nutriscore_grade VARCHAR(1),
        nova_group INTEGER,
        ecoscore_grade VARCHAR(1)
    );

    -- dim_product avec FK
    CREATE TABLE dim_product (
        product_sk BIGINT PRIMARY KEY,
        code VARCHAR(50) NOT NULL,
        product_name VARCHAR(1000),
        brand_sk BIGINT REFERENCES dim_brand(brand_sk),
        primary_category_sk BIGINT REFERENCES dim_category(category_sk),
        countries_multi TEXT,
        effective_from DATE,
        effective_to DATE,
        is_current BOOLEAN DEFAULT TRUE
    );

    -- bridge_product_category avec FK
    CREATE TABLE bridge_product_category (
        product_sk BIGINT REFERENCES dim_product(product_sk),
        category_sk BIGINT REFERENCES dim_category(category_sk),
        PRIMARY KEY (product_sk, category_sk)
    );

    -- fact_nutrition_snapshot avec FK
    CREATE TABLE fact_nutrition_snapshot (
        fact_id BIGINT PRIMARY KEY,
        product_sk BIGINT REFERENCES dim_product(product_sk),
        time_sk BIGINT REFERENCES dim_time(time_sk),
        energy_kcal_100g DECIMAL(10,2),
        fat_100g DECIMAL(10,2),
        saturated_fat_100g DECIMAL(10,2),
        sugars_100g DECIMAL(10,2),
        salt_100g DECIMAL(10,2),
        proteins_100g DECIMAL(10,2),
        fiber_100g DECIMAL(10,2),
        sodium_100g DECIMAL(10,2),
        nutriscore_grade VARCHAR(1),
        nova_group INTEGER,
        ecoscore_grade VARCHAR(1),
        completeness_score DECIMAL(5,4),
        quality_issues_json TEXT
    );

    -- Index pour performances
    CREATE INDEX idx_fact_product ON fact_nutrition_snapshot(product_sk);
    CREATE INDEX idx_fact_time ON fact_nutrition_snapshot(time_sk);
    CREATE INDEX idx_fact_nutriscore ON fact_nutrition_snapshot(nutriscore_grade);
    CREATE INDEX idx_product_brand ON dim_product(brand_sk);
    CREATE INDEX idx_product_category ON dim_product(primary_category_sk);
    CREATE INDEX idx_product_code ON dim_product(code);
    CREATE INDEX idx_category_level ON dim_category(level);
    CREATE INDEX idx_time_date ON dim_time(date);
    """

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit()
        cursor.close()
        conn.close()

        print("Tables creees:")
        print("  dim_time, dim_brand, dim_country, dim_category")
        print("  dim_nutri, dim_product, bridge_product_category")
        print("  fact_nutrition_snapshot")
        print("FK et index configures")
        return True

    except Exception as e:
        print(f"Erreur: {e}")
        return False

# ============================================
# ETAPE 3 : Verifier
# ============================================
def verify():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        cursor = conn.cursor()

        cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' ORDER BY table_name;
        """)
        tables = cursor.fetchall()

        print(f"\n{len(tables)} tables creees:")
        for t in tables:
            print(f"  - {t[0]}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Erreur: {e}")
        return False

# ============================================
# EXECUTION
# ============================================
if __name__ == "__main__":
    print("=" * 40)
    print("Setup PostgreSQL - OpenFoodFacts")
    print("=" * 40)
    
    if create_database():
        if create_tables():
            verify()
    
    print("\n" + "=" * 40)
    print("Pret! Lancez le notebook ETL")
    print("=" * 40)
