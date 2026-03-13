USE [SAT_V2];
GO

PRINT '========================================================================';
PRINT 'INICIANDO PLAN MAESTRO DE INDEXACIÓN HÍBRIDA (ETL + ANALÍTICA)';
PRINT '========================================================================';

DECLARE @TableName NVARCHAR(256);
DECLARE @IndexName NVARCHAR(256);
DECLARE @SQL NVARCHAR(MAX);

-- =========================================================================
-- FASE 1: INDEXACIÓN AUTOMÁTICA PARA ANEXO 1A (CABECERAS)
-- =========================================================================
PRINT '--> Analizando tablas del Anexo 1A...';

DECLARE cur_1A CURSOR FOR 
SELECT name FROM sys.tables WHERE name LIKE '%ANEXO_1A%' AND type = 'U';

OPEN cur_1A;
FETCH NEXT FROM cur_1A INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Índice 1: UUID (Acelera la de-duplicación del ETL y los JOINs)
    SET @IndexName = 'IX_' + @TableName + '_UUID';
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = @IndexName AND object_id = OBJECT_ID(@TableName))
    BEGIN
        SET @SQL = 'CREATE NONCLUSTERED INDEX [' + @IndexName + '] ON [dbo].[' + @TableName + '] ([UUID]);';
        PRINT '    [+] Creando: ' + @IndexName;
        EXEC sp_executesql @SQL;
    END
    ELSE PRINT '    [✓] Ya existe: ' + @IndexName;

    -- Índice 2: EmisorRFC (Acelera tus vistas y consultas Ad-Hoc con IN ('RFC1', 'RFC2'))
    SET @IndexName = 'IX_' + @TableName + '_EmisorRFC';
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = @IndexName AND object_id = OBJECT_ID(@TableName))
    BEGIN
        SET @SQL = 'CREATE NONCLUSTERED INDEX [' + @IndexName + '] ON [dbo].[' + @TableName + '] ([EmisorRFC]) INCLUDE ([UUID], [FechaEmision], [Total]);';
        PRINT '    [+] Creando: ' + @IndexName;
        EXEC sp_executesql @SQL;
    END
    ELSE PRINT '    [✓] Ya existe: ' + @IndexName;

    FETCH NEXT FROM cur_1A INTO @TableName;
END
CLOSE cur_1A;
DEALLOCATE cur_1A;

-- =========================================================================
-- FASE 2: INDEXACIÓN AUTOMÁTICA PARA ANEXO 2B (DETALLES)
-- =========================================================================
PRINT CHAR(13) + '--> Analizando tablas del Anexo 2B...';

DECLARE cur_2B CURSOR FOR 
SELECT name FROM sys.tables WHERE name LIKE '%ANEXO_2B%' AND type = 'U';

OPEN cur_2B;
FETCH NEXT FROM cur_2B INTO @TableName;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Índice 1: UUID (Acelera los LEFT JOIN con la tabla 1A)
    SET @IndexName = 'IX_' + @TableName + '_UUID';
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = @IndexName AND object_id = OBJECT_ID(@TableName))
    BEGIN
        SET @SQL = 'CREATE NONCLUSTERED INDEX [' + @IndexName + '] ON [dbo].[' + @TableName + '] ([UUID]);';
        PRINT '    [+] Creando: ' + @IndexName;
        EXEC sp_executesql @SQL;
    END
    ELSE PRINT '    [✓] Ya existe: ' + @IndexName;

    -- Índice 2: HashID (Acelera la validación de duplicados exactos en el ETL)
    SET @IndexName = 'IX_' + @TableName + '_HashID';
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = @IndexName AND object_id = OBJECT_ID(@TableName))
    BEGIN
        SET @SQL = 'CREATE NONCLUSTERED INDEX [' + @IndexName + '] ON [dbo].[' + @TableName + '] ([HashID], [UUID]);';
        PRINT '    [+] Creando: ' + @IndexName;
        EXEC sp_executesql @SQL;
    END
    ELSE PRINT '    [✓] Ya existe: ' + @IndexName;

    FETCH NEXT FROM cur_2B INTO @TableName;
END
CLOSE cur_2B;
DEALLOCATE cur_2B;

PRINT '========================================================================';
PRINT 'INDEXACIÓN FINALIZADA CON ÉXITO.';
PRINT '========================================================================';
GO