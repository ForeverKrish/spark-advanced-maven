/*
Write a sql to get month over month salary and avarage salary of a employee in a dept
*/

SELECT
  dept_id,
  emp_id,
  DATE_TRUNC('month', salary_date) AS month,
  SUM(salary) AS total_salary,
  -- overall average per employee
  AVG(salary) OVER (PARTITION BY dept_id, emp_id) AS avg_salary_overall,
  -- previous month's total
  LAG(SUM(salary)) OVER (
    PARTITION BY dept_id, emp_id
    ORDER BY DATE_TRUNC('month', salary_date)
  ) AS prev_month_salary,
  -- month-over-month difference
  SUM(salary)
    - LAG(SUM(salary)) OVER (
        PARTITION BY dept_id, emp_id
        ORDER BY DATE_TRUNC('month', salary_date)
      ) AS mom_salary_diff
FROM employee_salaries
GROUP BY dept_id, emp_id, DATE_TRUNC('month', salary_date)
ORDER BY dept_id, emp_id, month;