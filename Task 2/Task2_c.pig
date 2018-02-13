 /* Employee (employee id and employee name) with maximum expense */

 A = LOAD '/pig/employee_details.txt' using PigStorage(',') AS (EmpID:int,EmpName:chararray,EmpSalary:int,DepartmentID:int);

B = LOAD '/pig/employee_expenses.txt' using PigStorage('\t') AS (EmpID:int,EmpExpense:int);

C = JOIN  A by EmpID, B by EmpID;

D = order C by B::EmpExpense DESC;

E = FOREACH D GENERATE A::EmpID, A::EmpName;

F = LIMIT E 1;

DUMP F;


/* In relation A we load the employee_details,
   In relation B we load the employee_expenses data
   In relation c we join the both tables by empID 
   In relation D we arrange the employee expeses in descending order
   In relation E we are generating  required columns empID and empName
   In relation F we limit to top row*/