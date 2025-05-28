namespace Services.Exporting.Odoo

open Model
open Services.Exporting.Odoo.ExportHelpers

type ExportIr () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportPropertyDefaults (modelName : string) =

        let header = addStampHeadersTo [
            "model" ; "referenced_model" ; "name" ; "external_id/id"
        ]

        let sql = """
            with
            rel_external_id as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where module not like '\_\_%'
            ),
            rel_ir_property as (
                select
                    split_part(value_reference, ',', 1)::varchar as referenced_model,
                    split_part(value_reference, ',', 2)::integer as record_id,
                    *
                from ir_property
            )
            select imf.model, rei.external_id, rip.*
            from rel_ir_property as rip
            join ir_model_fields as imf on rip.fields_id = imf.id
            left join rel_external_id as rei
                on rip.record_id = rei.id
                and rip.referenced_model = rei.model
            where res_id is null
            and value_reference is not null
            order by imf.model, name
        """

        let readerFun (reader : RowReader) =
            [
                reader.text "model"
                reader.text "referenced_model"
                reader.text "name"
                match reader.textOrNone "external_id" with
                | Some externalId ->
                    if externalId.Contains "l10n_es.1_account_common"
                    then externalId.Replace("l10n_es.", "account.")
                    else externalId
                | None when reader.text "name" = "property_stock_journal" ->
                    "account.1_inventory_valuation"
                | None -> reader.int "record_id" |> Some |> Helpers.exportId (reader.text "referenced_model")
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAttachment (modelName : string) =

        failwith "Lo lógico sería hacerlos desde un script de shell."

        let header = [ "id" ; "res_model" ; "res_id/id" ; "name" ; "store_fname" ; "mimetype" ]

        let sql ="""
            select id, res_model, res_id, name, store_fname, mimetype
            from ir_attachment
            where res_model is not null
            and res_model = 'account.move'
            --and res_id = 30
            --limit 3
            order by res_model
        """
        let modelFunMap = Map.ofList [
            ("account.bank.statement", AccountBankStatement.exportId)
            ("account.move", AccountMove.exportId)
            ("account.payment.order", AccountPaymentOrder.exportId)
            // ("ir.ui.menu",
            // ("ir.ui.view",
            // ("l10n.es.aeat.mod303.report",
            // ("l10n.es.aeat.mod347.report",
            // ("mail.channel",
            // ("payment.acquirer",
            // ("payment.icon",
            // ("res.company",
            // ("res.lang",
            // ("res.partner",
        ]

        let readerFun (reader : RowReader) =
            [
                let resModel = reader.text "res_model"

                if modelFunMap.ContainsKey resModel then
                    reader.intOrNone "id" |> IrAttachment.exportId
                    resModel
                    reader.int "res_id/id" |> Some |> modelFunMap[resModel]
                    reader.text "name"
                    reader.text "store_fname"
                    reader.text "mimetype"
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportSequence (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "active" ; "code" ; "implementation" ; "name" ; "number_increment"
            "number_next" ; "number_next_actual" ; "padding" ; "prefix" ; "suffix" ; "use_date_range"
        ]

        let sql = """
            with
			rel_sequence as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'ir.sequence'
                and module not like '\_\_%'
			)
            select rs.external_id as sequence_external_id, irs.*
            from ir_sequence as irs
            left join rel_sequence as rs on irs.id = rs.id
        """

        let readerFun (reader : RowReader) =
            [
                match reader.textOrNone "sequence_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "id" |> Some |> IrSequence.exportId

                reader.boolOrNone "active" |> orEmptyString
                reader.textOrNone "code" |> orEmptyString
                reader.text "implementation"
                reader.text "name"
                reader.int "number_increment" |> string
                reader.int "number_next" |> string

                match reader.text "implementation" with
                | "standard" -> getSequenceName (reader.int "id")
                                |> getSequenceNumberNextActual |> string
                | _ -> ""

                reader.int "padding" |> string
                reader.textOrNone "prefix" |> orEmptyString
                reader.textOrNone "suffix" |> orEmptyString
                reader.boolOrNone "use_date_range" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportSequenceDateRange (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "date_from" ; "date_to" ; "sequence_id/id" ; "number_next" ; "number_next_actual"
        ]

        let sql = """
            with
			rel_sequence as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'ir.sequence'
                and module not like '\_\_%'
			)
            select rs.external_id as sequence_external_id,
                   irs.implementation,
                   irsdr.*
            from ir_sequence_date_range as irsdr
            join ir_sequence as irs on irsdr.sequence_id = irs.id
            left join rel_sequence as rs on irsdr.sequence_id = rs.id
            order by date_from
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> IrSequenceDateRange.exportId
                reader.dateOnly "date_from" |> Some |> dateOrEmptyString
                reader.dateOnly "date_to" |> Some |> dateOrEmptyString

                match reader.textOrNone "sequence_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "sequence_id" |> Some |> IrSequence.exportId

                reader.int "number_next" |> string

                match reader.text "implementation" with
                | "standard" -> getDateRangeSequenceName (reader.int "sequence_id") (reader.int "id")
                                |> getSequenceNumberNextActual |> string
                | _ -> ""

                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportDefault (modelName : string) =

        failwith "No entiendo este modelo."

        let header = addStampHeadersTo [ "id" ; "field_id/id" ; "condition" ; "json_value" ]

        let data =
            [
                [
                    Some 1 |> DefaultValue.exportId ; "account.field_account_move__journal_id/id" ; "" ; "0"
                    "__export__.res_users_2" ; "2023-01-02 11:19:17" ; "base.user_root" ; "2023-06-16 17:18:58"
                ]
            ]

        header::data
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
