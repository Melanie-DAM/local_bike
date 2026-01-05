{{ config(materialized='table') }}

WITH product_agg AS (
    SELECT 
        product_id
        , product_name
        , category_id

        -- Agrégation des métriques
        , SUM(quantity) AS total_units_sold
        , SUM(total_gross_amount) AS total_gross_revenue
        , SUM(total_net_amount) AS total_net_revenue
        , SUM(discount_amount) AS total_discount_given
        
        -- Taux de remise moyen
        , SAFE_DIVIDE(SUM(discount_amount), SUM(total_gross_amount)) AS avg_discount_rate
    FROM {{ ref('int_local_bike__product_pricing_performance') }}
    GROUP BY 1, 2, 3
)

, pareto_calc AS (
    SELECT 
        *
        -- Revenu cumulé pour la loi de Pareto (80/20)
        , SUM(total_net_revenue) OVER(ORDER BY total_net_revenue DESC) AS cumulative_revenue
        , SUM(total_net_revenue) OVER() AS total_global_revenue
    FROM product_agg
)

, pareto_calc_enriched AS (
    SELECT 
        *
        -- Flag Pareto : Le produit fait-il partie des top générateurs de 80% du CA ?
        , (cumulative_revenue / total_global_revenue) AS cumul_rev_pct_of_total
        , CASE 
            WHEN (cumulative_revenue / total_global_revenue) <= 0.80 THEN 'Top 80% (Core)'
            ELSE 'Bottom 20% (Long Tail)'
        END AS pareto_segment
    FROM pareto_calc
)

SELECT *
FROM pareto_calc_enriched