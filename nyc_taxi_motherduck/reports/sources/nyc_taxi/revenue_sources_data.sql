SELECT 
    source,
    target,
    SUM(revenue) as revenue
FROM main_marts.fct_revenue_flow
GROUP BY source, target
ORDER BY revenue DESC