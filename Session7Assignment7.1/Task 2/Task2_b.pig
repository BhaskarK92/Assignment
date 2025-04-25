/* Top 3 employees (employee id and employee name) with highest salary, whose employee id is an odd number */

A = LOAD '/pig/employee_details.txt' using PigStorage(',') AS (EmpID:int,EmpName:chararray,EmpSalary:long,DepartmentID:int);

B = order A by EmpSalary DESC;

C = Filter B by EmpID%2==1;

D = FOREACH C GENERATE EmpID,EmpName, EmpSalary;

E = Limit D  3;

/* In relation A we load the employee_details,
   In relation B we arrange the employee salary in descending order
   In relation C we filter out empID with odd number
   In relation D we are generating  required columns empID and empName and empSalary
   In relation E we limit it till top 3 rows */