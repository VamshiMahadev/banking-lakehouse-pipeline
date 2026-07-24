SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    c.phone_number,
    
    -- Account & Portfolio Metrics
    COALESCE(c360.total_accounts, 0) AS total_bank_accounts,
    COALESCE(c360.total_liquidity_balance, 0.00) AS total_liquidity_balance,
    
    -- Credit Card Exposure
    COUNT(DISTINCT cc.card_id) AS total_credit_cards,
    COALESCE(SUM(cc.credit_limit), 0.00) AS total_credit_limit,
    
    -- Loan Exposure
    COUNT(DISTINCT l.loan_id) AS total_active_loans,
    COALESCE(SUM(l.amount), 0.00) AS total_loan_amount_borrowed,
    
    -- Transaction Volume Insights (via Gold Daily Metrics)
    COALESCE(SUM(m.total_deposits), 0.00) AS aggregated_deposits,
    COALESCE(SUM(m.total_withdrawals), 0.00) AS aggregated_withdrawals,
    COALESCE(SUM(m.transaction_count), 0) AS total_transactions_count,
    
    -- Customer Net Financial Standing
    (COALESCE(c360.total_liquidity_balance, 0.00) - COALESCE(SUM(l.amount), 0.00)) AS net_financial_position

FROM banking_catalog.silver.dim_customers c

-- Join Gold Customer 360 View
LEFT JOIN banking_catalog.gold.v_customer_360 c360 
    ON c.customer_id = c360.customer_id

-- Join Silver Accounts & Gold Daily Metrics
LEFT JOIN banking_catalog.silver.dim_accounts a 
    ON c.customer_id = a.customer_id
LEFT JOIN banking_catalog.gold.fact_daily_account_metrics m 
    ON a.account_id = m.account_id

-- Join Raw/Bronze External Financial Products (Loans & Credit Cards)
LEFT JOIN banking_catalog.bronze.bronze_credit_cards cc 
    ON c.customer_id = cc.customer_id AND UPPER(cc.status) = 'ACTIVE'
LEFT JOIN banking_catalog.bronze.bronze_loans l 
    ON c.customer_id = l.customer_id AND UPPER(l.status) = 'APPROVED'

GROUP BY 
    c.customer_id, 
    c.first_name, 
    c.last_name, 
    c.email, 
    c.phone_number,
    c360.total_accounts,
    c360.total_liquidity_balance

ORDER BY 
    total_liquidity_balance DESC;