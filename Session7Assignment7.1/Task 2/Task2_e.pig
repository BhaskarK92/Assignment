/ *List of employees (employee id and employee name) having no entry in employee_expenses file */

A = LOAD '/pig/employee_details.txt' using PigStorage(',') AS (EmpID:int,EmpName:chararray,EmpSalary:int,DepartmentID:int);

B = LOAD '/pig/employee_expenses.txt' using PigStorage('\t') AS (EmpID:int,EmpExpense:int);

C = JOIN  A by EmpID LEFT OUTER, B by EmpID;

D = FILTER C by B::EmpID is null;

E = FOREACH D GENERATE A::EmpID, A::EmpName;

DUMP E;


/* In relation A we load the employee_details,
   In relation B we load the employee_expenses data
   In relation c we join the tables
   In relation D we are filtering the empID which is not present in relation C
   In relation E we generating empID and empName in relation D */