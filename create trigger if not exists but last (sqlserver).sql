-- create trigger if not exists but last_updated column exists
IF EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '${TABLENAME}' AND TABLE_SCHEMA = '${TABLESCHEMA}' AND COLUMN_NAME = 'last_updated')
BEGIN
	IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'TR' AND name = '${TABLESCHEMA}_${TABLENAME}_delete_and_update_trigger')
	BEGIN
		DECLARE @SQL nvarchar(1000)
		
		SET @SQL = 'CREATE TRIGGER ${TABLESCHEMA}_${TABLENAME}_delete_and_update_trigger ON ${TABLESCHEMA}.${TABLENAME} ' 
		+ ' FOR INSERT, UPDATE, DELETE AS BEGIN '
		+ ' DECLARE @tn_b INT; '
		+ ' SET @tn_b = TRIGGER_NESTLEVEL(( SELECT object_id FROM sys.triggers WHERE name = ''${TABLESCHEMA}_${TABLENAME}_delete_and_update_trigger'' )); '
		+ ' IF (@tn_b <= 1) '
		+ ' BEGIN '
			+ ' IF EXISTS (SELECT 0 FROM inserted) '  -- insert or update
			+ ' BEGIN '
				+ 'IF EXISTS (SELECT ${PRIMARYKEYCOLUMN} FROM inserted) '
				+ 'BEGIN '
					+ ' UPDATE ${TABLESCHEMA}.${TABLENAME} SET last_updated=CURRENT_TIMESTAMP WHERE ' -- set the value of the last_updated to now
					+ ' ${PRIMARYKEYCOLUMN} IN (SELECT ${PRIMARYKEYCOLUMN} FROM inserted); '
				+ 'END '
			+ ' END '
			+ ' ELSE ' -- deletion
			+ ' BEGIN '
					+ 'IF EXISTS (SELECT ${PRIMARYKEYCOLUMN} FROM DELETED) '
					+ 'BEGIN '
						+ ' DECLARE @table_id INT; '
						+ ' SELECT @table_id = min(${PRIMARYKEYCOLUMN}) FROM deleted; '
						+ ' WHILE @table_id IS NOT NULL '
						+ ' BEGIN '
							+ ' INSERT INTO ${TABLESCHEMA}_${TABLENAME}_deletion (row_id, deletion_time) '
							+ ' VALUES ( @table_id, CURRENT_TIMESTAMP );'
							+ ' SELECT @table_id = min(${PRIMARYKEYCOLUMN}) FROM deleted WHERE ${PRIMARYKEYCOLUMN} > @table_id;'
						+ ' END '
					+ ' END '
			+ ' END '
		+ ' END END '

		EXEC (@SQL)
	END
END