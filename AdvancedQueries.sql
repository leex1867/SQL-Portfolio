PostgreSQL v12

2A

SELECT 
	*,
	AVG(account_balance) OVER (PARTITION BY member_id) AS avg_account_balance,
    COUNT(*) OVER (PARTITION BY member_id) AS member_row_count,
    MIN(load_date) OVER (PARTITION BY member_id) AS min_load_date,
    MAX(load_date) OVER (PARTITION BY member_id) AS max_load_date, 
    CASE WHEN load_date = (MAX(load_date) OVER (PARTITION BY member_id)) THEN 1 ELSE 0 END AS is_latest_record_2,
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY load_date) AS load_date_seq,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY load_date DESC) = 1 THEN 1 ELSE 0 END AS is_latest_record_3
FROM members
ORDER BY member_id, load_date;

SELECT 
	*,
    COUNT(*) OVER (PARTITION BY member_Id) AS member_claim_count,
    SUM(paid_amount) OVER (PARTITION BY member_id) AS member_total_paid_amount,
    paid_amount / (SUM(paid_amount) OVER (PARTITION BY member_id)) AS paid_amount_pct_of_member_total,
    paid_amount / (SUM(paid_amount) OVER ()) AS paid_amount_pct_of_claims_total,
    AVG(paid_amount) OVER () AS avg_paid_amount,
    paid_amount / (AVG(paid_amount) OVER ()) AS pct_of_avg_paid_amount
FROM claims;

2B

2C

Create a "sequence" column to show the month sequence per member ID, or the total number of months that a member has

SELECT 
	*,
    COUNT(*) OVER (PARTITION BY member_id) AS yyyymm_total,
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY yyyymm) AS yyyymm_sequence
FROM eligibility
