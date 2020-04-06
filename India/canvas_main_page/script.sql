-- Infected States 
SELECT COUNT(DISTINCT * ) AS "States"
FROM "covid19-india"

-- Confirmed Case
SELECT "report_date",SUM("Confirmed") as "Confirmed"
FROM "covid19-india"
GROUP BY "report_date"


SELECT "report_date", (CASE WHEN "new_cases" IS NULL THEN 0
ELSE  SUM("new_cases")/SUM("Confirmed")  END)as "percent_new"
FROM "covid19-india"
GROUP BY "report_date"


SELECT "report_date", SUM("new_cases")  as "new_case"
FROM "covid19-india"
GROUP BY "report_date"
HAVING("new_case") > 0

# % calculation of new cases
SELECT "report_date", SUM("new_cases") as "new_case",
SUM("Confirmed") as "old_case",
ROUND((SUM("new_cases")/SUM("Confirmed")),2) as "change"
FROM "covid19-india"
GROUP BY "report_date"
HAVING("new_case") > 0
ORDER BY "new_case"
# Recovered %
SELECT "report_date", SUM("new_Recovd") as "new_case",
SUM("Recovered") as "old_case",
ROUND((SUM("new_Recovd")/SUM("Recovered")),2) as "change"
FROM "covid19-india"
WHERE "report_date" < NOW()
GROUP BY "report_date"
HAVING("new_Recovd") > 0 
ORDER BY "new_Recovd"


SELECT "report_date", "State/UT" as "State", SUM("Confirmed") as "Confirmed", 
SUM("Reovered") as "Recovered", SUM("Deaths") as "Deaths"
FROM "covid19-india"
GROUP BY "report_date", "State/UT"

# Bubble chart 
SELECT report_date,"State/UT" as State 
,SUM(Confirmed) as Confirmed FROM "covid19-india*"
WHERE report_date < NOW()
GROUP BY report_date, State