USE Experiment;
GO

CREATE TRIGGER dbo_employeeSecond_delete_and_update_trigger ON dbo.EmployeeSecond 
FOR INSERT, UPDATE, DELETE AS BEGIN 
	DECLARE @tn_b INT;
	SET @tn_b = TRIGGER_NESTLEVEL(( SELECT object_id FROM sys.triggers WHERE name = 'dbo_employeeSecond_delete_and_update_trigger' ));
	
	IF (@tn_b <= 1)
	BEGIN
		IF EXISTS( SELECT 0 FROM inserted )
			BEGIN
				IF EXISTS ( SELECT employee_id FROM inserted ) -- insert or update
				BEGIN
					UPDATE dbo.employeeSecond SET last_updated=CURRENT_TIMESTAMP WHERE employee_id IN ( SELECT employee_id FROM inserted );
				END
			END
		ELSE
			BEGIN
				IF EXISTS ( SELECT employee_id FROM deleted ) -- delete
					BEGIN
						DECLARE @table_id INT;

						SELECT @table_id = min( employee_id ) FROM dbo.EmployeeSecond;

						while @table_id IS NOT NULL 
							BEGIN
								INSERT INTO dbo_employee_deletion ( row_id, deletion_time ) VALUES ( @table_id, CURRENT_TIMESTAMP )
                                
                                SELECT @table_id = min( employee_id ) FROM dbo.EmployeeSecond WHERE employee_id > @table_id;
							END
					END
			END
	END
END
GO

SELECT * FROM Employee;

INSERT INTO EmployeeSecond (employee_id, employee_name, employee_phone) 
VALUES (1, 'Nguh Prince', '688900200') (2, 'Nguh Chris', '679489503'), (3, 'El Dorado', '0590390582'), (4, 'Walter White', 'Heisenberg');

SELECT * FROM EmployeeSecond;