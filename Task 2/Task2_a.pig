/* Top 5 employees (employee id and employee name) with highest rating */

A = LOAD 'employee_details.txt' using PigStorage(',') AS (EmpID:int,EmpName:chararray,EmpSalary:double,DepartmentID:int);


B = order A by DepartmentID, EmpName ASC;


C = FOREACH B GENERATE EmpID,EmpName;


D = LIMIT C 5;


/* In relation A we load the employee_details,
   In relation B we arrange the depID and employee name in ascending order
   In relation C we are generating two required columns empID and empName
   In relation D we limit it till top 5 rows */