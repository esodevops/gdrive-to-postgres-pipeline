-- Count transactions rows
SELECT COUNT(*) FROM staging.transactions;

-- Net spend by category for completed transactions
SELECT 
   category, 
   COUNT(*) AS txns,
   ROUND(SUM(signed_amount)::numeric, 2) AS net_amount
FROM staging.transactions
WHERE status = 'Completed'
GROUP BY category
ORDER BY net_amount;

-- Transactions per customer segment (joins all three tables)
SELECT 
	c.customer_segment, 
	COUNT(*) AS txns
FROM staging.transactions t
JOIN staging.accounts a 
ON a.account_id = t.account_id
JOIN staging.customers c 
ON c.customer_id = a.customer_id
GROUP BY c.customer_segment
ORDER BY txns DESC;
