namespace Services.Exporting.Odoo

open ClosedXML.Excel
open Npgsql.FSharp
open Model.Constants
open Services.Exporting.Odoo.ExportHelpers

type ExportTableNames () =

    //------------------------------------------------------------------------------------------------------------------
    static let exportUsedTableNames (connectionString : string) (header : string) (modelName : string) =

        let header = [ header |> box ]

        let sql = """
            DO $$
            DECLARE
                rec RECORD;
                sql_query TEXT;
                row_count BIGINT;
            BEGIN
                -- Create a temporary table to store results
                DROP TABLE IF EXISTS temp_module_table_counts;
                CREATE TEMP TABLE temp_module_table_counts (
                    module_name TEXT,
                    table_name TEXT,
                    row_count BIGINT
                );

                -- Loop through all tables that match the pattern
                FOR rec IN
                    SELECT
                        CASE
                            WHEN position('_' in tablename) > 0
                            THEN substring(tablename from 1 for position('_' in tablename) - 1)
                            ELSE tablename
                        END as module_name,
                        tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'  -- Adjust schema name if needed
                        AND tablename LIKE '%_%'  -- Only tables with underscore (following module_model pattern)
                    ORDER BY tablename
                LOOP
                    -- Build dynamic query to count rows
                    sql_query := 'SELECT COUNT(*) FROM ' || quote_ident(rec.tablename);

                    -- Execute the count query
                    EXECUTE sql_query INTO row_count;

                    -- Only insert if table has data
                    IF row_count > 0 THEN
                        INSERT INTO temp_module_table_counts (module_name, table_name, row_count)
                        VALUES (rec.module_name, rec.tablename, row_count);
                    END IF;
                END LOOP;
            END $$;

            -- Display the results
            SELECT
                module_name,
                table_name,
                row_count
            FROM temp_module_table_counts
            ORDER BY module_name, table_name;
        """

        let readerFun (reader : RowReader) =
            [
                reader.text "table_name" |> box
                reader.int "row_count" |> box
            ]

        let data =
            connectionString
            |> Sql.connect
            |> Sql.query sql
            |> Sql.execute readerFun

        let workbook = new XLWorkbook()
        let worksheet = workbook.Worksheets.Add(modelName)

        worksheet.PageSetup.Margins.Left <- 0.5
        worksheet.PageSetup.Margins.Right <- 0.5
        worksheet.Cell("A2").InsertData(data) |> ignore
        worksheet.Column("B").Style.NumberFormat.Format <- "#,##0"

        for i in 1..data.Length do
            if i % 2 = 0 then
                worksheet.Range($"A{i}:B{i}").Style.Fill.BackgroundColor <- XLColor.LightGray

        worksheet.Cell("A1").Style.Font.Bold <- true
        worksheet.Style.Font.FontName <- "Liberation Sans"
        worksheet.Style.Font.FontSize <- 12.0
        worksheet.Columns("A", "B").AdjustToContents() |> ignore
        worksheet.Cell("A1").InsertData(header) |> ignore

        workbook
        |> IExcelBroker.saveFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportNotFoundIn17 (modelName : string) =

        let header = [ "Tablas de Odoo 15 que no existen en Odoo 17" ]

        let sql15 = """
            DO $$
            DECLARE
                rec RECORD;
                sql_query TEXT;
                row_count BIGINT;
            BEGIN
                -- Create a temporary table to store results
                DROP TABLE IF EXISTS temp_module_table_counts;
                CREATE TEMP TABLE temp_module_table_counts (
                    module_name TEXT,
                    table_name TEXT,
                    row_count BIGINT
                );

                -- Loop through all tables that match the pattern
                FOR rec IN
                    SELECT
                        CASE
                            WHEN position('_' in tablename) > 0
                            THEN substring(tablename from 1 for position('_' in tablename) - 1)
                            ELSE tablename
                        END as module_name,
                        tablename
                    FROM pg_tables
                    WHERE schemaname = 'public'  -- Adjust schema name if needed
                        AND tablename LIKE '%_%'  -- Only tables with underscore (following module_model pattern)
                    ORDER BY tablename
                LOOP
                    sql_query := 'SELECT COUNT(*) FROM ' || quote_ident(rec.tablename);

                    EXECUTE sql_query INTO row_count;                -- Execute the count query

                    IF row_count > 0 THEN                            -- Only insert if table has data
                        INSERT INTO temp_module_table_counts (module_name, table_name, row_count)
                        VALUES (rec.module_name, rec.tablename, row_count);
                    END IF;
                END LOOP;
            END $$;

            SELECT table_name
            FROM temp_module_table_counts
        """

        let readerFun (reader : RowReader) = [ reader.text "table_name" ]

        let tableNamesSet15 =
            ISqlBroker.getExportData sql15 readerFun
            |> List.map _.Head
            |> Set.ofList

        let sql17 = """
            select tablename as table_name
            from pg_tables
            where schemaname = 'public'
            order by tablename
        """

        let tableNamesSet17 =
            CONNECTION_STRING_17
            |> Sql.connect
            |> Sql.query sql17
            |> Sql.execute readerFun
            |> List.map _.Head
            |> Set.ofList

        let tableNamesDifferences =
            Set.difference tableNamesSet15 tableNamesSet17
            |> List.ofSeq
            |> List.sort
            |> List.map (fun tableName -> [ tableName ])

        header::tableNamesDifferences
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportUsedTableNames15 (modelName : string) =
        exportUsedTableNames CONNECTION_STRING_15 "Tablas usadas en Odoo 15" modelName
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportUsedTableNames17 (modelName : string) =
        exportUsedTableNames CONNECTION_STRING_17 "Tablas usadas en Odoo 17" modelName
    //------------------------------------------------------------------------------------------------------------------
