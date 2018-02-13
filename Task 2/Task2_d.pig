/*List of employees (employee id and employee name) having entries in employee_expenses file */

A = LOAD '/pig/employee_details.txt' using PigStorage(',') AS (EmpID:int,EmpName:chararray,EmpSalary:int,DepartmentID:int);

B = LOAD '/pig/employee_expenses.txt' using PigStorage('\t') AS (EmpID:int,EmpExpense:int);

C = JOIN  A by EmpID, B by EmpID;

D = FOREACH C GENERATE A::EmpID, A::EmpName;

E = DISTINCT D;

DUMP E;

/* In relation A we load the employee_details,
   In relation B we load the employee_expenses data
   In relation c we join the both tables by empID 
   In relation D we are generating  required columns empID and empName
   In relation E we generating empID and empName in relation D */
