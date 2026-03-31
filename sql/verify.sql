-- sql/verify.sql
-- Run after pipeline to confirm data quality in PostgreSQL

\echo '=== ROW COUNT ==='
SELECT COUNT(*) AS total_rows FROM employees_clean;

\echo '=== UNIQUENESS CHECK ==='
SELECT
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT employee_id) AS unique_ids,
    COUNT(DISTINCT email)       AS unique_emails
FROM employees_clean;

\echo '=== NULL CHECK ON REQUIRED FIELDS ==='
SELECT
    COUNT(CASE WHEN employee_id IS NULL THEN 1 END) AS null_employee_id,
    COUNT(CASE WHEN first_name  IS NULL THEN 1 END) AS null_first_name,
    COUNT(CASE WHEN last_name   IS NULL THEN 1 END) AS null_last_name,
    COUNT(CASE WHEN full_name   IS NULL THEN 1 END) AS null_full_name,
    COUNT(CASE WHEN email       IS NULL THEN 1 END) AS null_email,
    COUNT(CASE WHEN hire_date   IS NULL THEN 1 END) AS null_hire_date
FROM employees_clean;

\echo '=== NO FUTURE HIRE DATES ==='
SELECT COUNT(*) AS future_hire_dates
FROM employees_clean
WHERE hire_date > CURRENT_DATE;

\echo '=== SALARY BAND DISTRIBUTION ==='
SELECT
    COALESCE(salary_band, 'NULL') AS salary_band,
    COUNT(*)                      AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM employees_clean
GROUP BY salary_band
ORDER BY count DESC;

\echo '=== DEPARTMENT DISTRIBUTION ==='
SELECT department, COUNT(*) AS count
FROM employees_clean
GROUP BY department
ORDER BY count DESC;

\echo '=== STATUS DISTRIBUTION ==='
SELECT status, COUNT(*) AS count
FROM employees_clean
GROUP BY status
ORDER BY count DESC;

\echo '=== AGE SANITY CHECK (should be 22-65) ==='
SELECT
    MIN(age) AS min_age,
    MAX(age) AS max_age,
    ROUND(AVG(age), 1) AS avg_age
FROM employees_clean
WHERE age IS NOT NULL;

\echo '=== TENURE SANITY CHECK ==='
SELECT
    MIN(tenure_years) AS min_tenure,
    MAX(tenure_years) AS max_tenure,
    ROUND(AVG(tenure_years), 1) AS avg_tenure
FROM employees_clean
WHERE tenure_years IS NOT NULL;

\echo '=== SAMPLE 5 ROWS ==='
SELECT
    employee_id, full_name, email, department,
    salary, salary_band, age, tenure_years, status
FROM employees_clean
LIMIT 5;

\echo '=== ALL CHECKS COMPLETE ==='