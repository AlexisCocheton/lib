-- ============================================
-- REQUETES ANALYTIQUES - OpenFoodFacts
-- Conformes aux exigences du sujet TRDE703
-- ============================================

-- ============================================
-- 1. TOP 10 MARQUES PAR PROPORTION NUTRI-SCORE A/B
-- ============================================
-- Objectif: Identifier les marques les plus "saines"

SELECT 
    b.brand_name AS marque,
    COUNT(*) AS total_produits,
    SUM(CASE WHEN f.nutriscore_grade IN ('A','B') THEN 1 ELSE 0 END) AS nb_produits_AB,
    SUM(CASE WHEN f.nutriscore_grade = 'A' THEN 1 ELSE 0 END) AS nb_A,
    SUM(CASE WHEN f.nutriscore_grade = 'B' THEN 1 ELSE 0 END) AS nb_B,
    ROUND(
        SUM(CASE WHEN f.nutriscore_grade IN ('A','B') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) AS pourcentage_AB
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
WHERE f.nutriscore_grade IS NOT NULL
GROUP BY b.brand_name
HAVING COUNT(*) >= 5  -- Minimum 5 produits pour etre significatif
ORDER BY pourcentage_AB DESC, total_produits DESC
LIMIT 10;


-- ============================================
-- 2. DISTRIBUTION NUTRI-SCORE PAR CATEGORIE
-- ============================================
-- Objectif: Voir la repartition des scores par type de produit

SELECT 
    c.category_name_fr AS categorie,
    f.nutriscore_grade AS grade,
    COUNT(*) AS nb_produits,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY c.category_name_fr), 
        2
    ) AS pourcentage_dans_categorie
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category c ON p.primary_category_sk = c.category_sk
WHERE f.nutriscore_grade IS NOT NULL
GROUP BY c.category_name_fr, f.nutriscore_grade
ORDER BY c.category_name_fr, f.nutriscore_grade;

-- Version pivot (tableau croise)
SELECT 
    c.category_name_fr AS categorie,
    COUNT(*) AS total,
    SUM(CASE WHEN f.nutriscore_grade = 'A' THEN 1 ELSE 0 END) AS "A",
    SUM(CASE WHEN f.nutriscore_grade = 'B' THEN 1 ELSE 0 END) AS "B",
    SUM(CASE WHEN f.nutriscore_grade = 'C' THEN 1 ELSE 0 END) AS "C",
    SUM(CASE WHEN f.nutriscore_grade = 'D' THEN 1 ELSE 0 END) AS "D",
    SUM(CASE WHEN f.nutriscore_grade = 'E' THEN 1 ELSE 0 END) AS "E"
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category c ON p.primary_category_sk = c.category_sk
WHERE f.nutriscore_grade IS NOT NULL
GROUP BY c.category_name_fr
HAVING COUNT(*) >= 5
ORDER BY total DESC;


-- ============================================
-- 3. HEATMAP PAYS x CATEGORIE : MOYENNE SUCRES
-- ============================================
-- Objectif: Identifier les combinaisons pays/categorie les plus sucrees

SELECT 
    co.country_name_fr AS pays,
    cat.category_name_fr AS categorie,
    COUNT(*) AS nb_produits,
    ROUND(AVG(f.sugars_100g)::numeric, 2) AS moyenne_sucres_100g,
    ROUND(MIN(f.sugars_100g)::numeric, 2) AS min_sucres,
    ROUND(MAX(f.sugars_100g)::numeric, 2) AS max_sucres
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category cat ON p.primary_category_sk = cat.category_sk
JOIN dim_country co ON p.countries_multi LIKE '%' || co.country_code || '%'
WHERE f.sugars_100g IS NOT NULL
GROUP BY co.country_name_fr, cat.category_name_fr
HAVING COUNT(*) >= 3  -- Minimum 3 produits
ORDER BY moyenne_sucres_100g DESC;

-- Top 20 combinaisons les plus sucrees
SELECT 
    co.country_name_fr AS pays,
    cat.category_name_fr AS categorie,
    COUNT(*) AS nb_produits,
    ROUND(AVG(f.sugars_100g)::numeric, 2) AS moyenne_sucres_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category cat ON p.primary_category_sk = cat.category_sk
JOIN dim_country co ON p.countries_multi LIKE '%' || co.country_code || '%'
WHERE f.sugars_100g IS NOT NULL
GROUP BY co.country_name_fr, cat.category_name_fr
HAVING COUNT(*) >= 3
ORDER BY moyenne_sucres_100g DESC
LIMIT 20;


-- ============================================
-- 4. TAUX DE COMPLETUDE DES NUTRIMENTS PAR MARQUE
-- ============================================
-- Objectif: Evaluer la qualite des donnees par marque

SELECT 
    b.brand_name AS marque,
    COUNT(*) AS nb_produits,
    -- Score global
    ROUND(AVG(f.completeness_score)::numeric * 100, 2) AS completude_globale_pct,
    -- Detail par nutriment
    ROUND(AVG(CASE WHEN f.energy_kcal_100g IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_energie,
    ROUND(AVG(CASE WHEN f.fat_100g IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_lipides,
    ROUND(AVG(CASE WHEN f.saturated_fat_100g IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_sat_fat,
    ROUND(AVG(CASE WHEN f.sugars_100g IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_sucres,
    ROUND(AVG(CASE WHEN f.salt_100g IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_sel,
    ROUND(AVG(CASE WHEN f.proteins_100g IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_proteines,
    ROUND(AVG(CASE WHEN f.fiber_100g IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_fibres,
    -- Scores
    ROUND(AVG(CASE WHEN f.nutriscore_grade IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_nutriscore,
    ROUND(AVG(CASE WHEN f.nova_group IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_nova
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY b.brand_name
HAVING COUNT(*) >= 3
ORDER BY completude_globale_pct DESC;

-- Marques avec la plus faible completude (a ameliorer)
SELECT 
    b.brand_name AS marque,
    COUNT(*) AS nb_produits,
    ROUND(AVG(f.completeness_score)::numeric * 100, 2) AS completude_pct
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY b.brand_name
HAVING COUNT(*) >= 5
ORDER BY completude_pct ASC
LIMIT 10;


-- ============================================
-- 5. LISTE DES ANOMALIES
-- ============================================
-- Objectif: Identifier les produits avec valeurs hors normes
-- Note: salt_100g > 25 ou sugars_100g > 80 sont des anomalies

SELECT 
    p.code AS code_barre,
    p.product_name AS nom_produit,
    b.brand_name AS marque,
    cat.category_name_fr AS categorie,
    f.salt_100g AS sel_100g,
    f.sugars_100g AS sucres_100g,
    f.energy_kcal_100g AS energie_100g,
    CASE 
        WHEN f.salt_100g > 25 AND f.sugars_100g > 80 THEN 'SEL ET SUCRES EXCESSIFS'
        WHEN f.salt_100g > 25 THEN 'SEL EXCESSIF (>25g/100g)'
        WHEN f.sugars_100g > 80 THEN 'SUCRES EXCESSIFS (>80g/100g)'
    END AS type_anomalie,
    f.quality_issues_json AS autres_problemes
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
LEFT JOIN dim_brand b ON p.brand_sk = b.brand_sk
LEFT JOIN dim_category cat ON p.primary_category_sk = cat.category_sk
WHERE f.salt_100g > 25 OR f.sugars_100g > 80
ORDER BY 
    CASE 
        WHEN f.salt_100g > 25 AND f.sugars_100g > 80 THEN 1
        WHEN f.salt_100g > 25 THEN 2
        ELSE 3
    END,
    f.salt_100g DESC NULLS LAST,
    f.sugars_100g DESC NULLS LAST;

-- Comptage des anomalies par type
SELECT 
    CASE 
        WHEN f.salt_100g > 25 AND f.sugars_100g > 80 THEN 'Sel ET Sucres'
        WHEN f.salt_100g > 25 THEN 'Sel > 25g'
        WHEN f.sugars_100g > 80 THEN 'Sucres > 80g'
        ELSE 'Aucune'
    END AS type_anomalie,
    COUNT(*) AS nb_produits
FROM fact_nutrition_snapshot f
GROUP BY 
    CASE 
        WHEN f.salt_100g > 25 AND f.sugars_100g > 80 THEN 'Sel ET Sucres'
        WHEN f.salt_100g > 25 THEN 'Sel > 25g'
        WHEN f.sugars_100g > 80 THEN 'Sucres > 80g'
        ELSE 'Aucune'
    END
ORDER BY nb_produits DESC;


-- ============================================
-- 6. EVOLUTION HEBDOMADAIRE DE LA COMPLETUDE
-- ============================================
-- Objectif: Suivre l'evolution de la qualite des donnees dans le temps

SELECT 
    t.year AS annee,
    t.iso_week AS semaine_iso,
    t.date AS date_snapshot,
    COUNT(*) AS nb_produits,
    ROUND(AVG(f.completeness_score)::numeric * 100, 2) AS completude_moyenne_pct,
    ROUND(MIN(f.completeness_score)::numeric * 100, 2) AS completude_min_pct,
    ROUND(MAX(f.completeness_score)::numeric * 100, 2) AS completude_max_pct,
    -- Taux de remplissage des scores
    ROUND(AVG(CASE WHEN f.nutriscore_grade IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_nutriscore,
    ROUND(AVG(CASE WHEN f.nova_group IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_nova,
    ROUND(AVG(CASE WHEN f.ecoscore_grade IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100, 2) AS pct_ecoscore
FROM fact_nutrition_snapshot f
JOIN dim_time t ON f.time_sk = t.time_sk
GROUP BY t.year, t.iso_week, t.date
ORDER BY t.year, t.iso_week;


-- ============================================
-- REQUETES SUPPLEMENTAIRES (BONUS)
-- ============================================

-- Classement marques par qualite nutritionnelle moyenne
SELECT 
    b.brand_name AS marque,
    COUNT(*) AS nb_produits,
    ROUND(AVG(f.sugars_100g)::numeric, 2) AS moy_sucres,
    ROUND(AVG(f.salt_100g)::numeric, 2) AS moy_sel,
    ROUND(AVG(f.fat_100g)::numeric, 2) AS moy_lipides,
    ROUND(AVG(f.saturated_fat_100g)::numeric, 2) AS moy_sat_fat,
    ROUND(AVG(f.proteins_100g)::numeric, 2) AS moy_proteines,
    ROUND(AVG(f.fiber_100g)::numeric, 2) AS moy_fibres,
    -- Score qualite (moins de sucre/sel/gras = mieux)
    ROUND(
        (100 - COALESCE(AVG(f.sugars_100g), 50) - COALESCE(AVG(f.salt_100g), 5) * 4 - COALESCE(AVG(f.saturated_fat_100g), 10))::numeric, 
        2
    ) AS score_qualite
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY b.brand_name
HAVING COUNT(*) >= 5
ORDER BY score_qualite DESC
LIMIT 20;

-- Distribution NOVA par categorie
SELECT 
    c.category_name_fr AS categorie,
    COUNT(*) AS total,
    SUM(CASE WHEN f.nova_group = 1 THEN 1 ELSE 0 END) AS "NOVA_1",
    SUM(CASE WHEN f.nova_group = 2 THEN 1 ELSE 0 END) AS "NOVA_2",
    SUM(CASE WHEN f.nova_group = 3 THEN 1 ELSE 0 END) AS "NOVA_3",
    SUM(CASE WHEN f.nova_group = 4 THEN 1 ELSE 0 END) AS "NOVA_4",
    ROUND(AVG(f.nova_group)::numeric, 2) AS nova_moyen
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category c ON p.primary_category_sk = c.category_sk
WHERE f.nova_group IS NOT NULL
GROUP BY c.category_name_fr
HAVING COUNT(*) >= 5
ORDER BY nova_moyen DESC;

-- Resume global du datamart
SELECT 
    (SELECT COUNT(*) FROM dim_time) AS nb_dates,
    (SELECT COUNT(*) FROM dim_brand) AS nb_marques,
    (SELECT COUNT(*) FROM dim_country) AS nb_pays,
    (SELECT COUNT(*) FROM dim_category) AS nb_categories,
    (SELECT COUNT(*) FROM dim_product) AS nb_produits,
    (SELECT COUNT(*) FROM fact_nutrition_snapshot) AS nb_faits,
    (SELECT ROUND(AVG(completeness_score)::numeric * 100, 2) FROM fact_nutrition_snapshot) AS completude_moy_pct;
