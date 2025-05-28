namespace Services.Exporting.Odoo

open System
open Model
open ExportHelpers
open Model.Constants

type ExportAccount () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccount (modelName : string) =

        let header = addStampHeadersTo [ "id" ; "code" ; "name"; "account_type_id" ; "reconcile" ; "last_visible_year" ]

        let accountTypeMap = Map [
            "data_account_type_receivable", "asset_receivable"
            "data_account_type_liquidity", "asset_cash"
            "data_account_type_current_assets", "asset_current"
            "data_account_type_non_current_assets", "asset_non_current"
            "data_account_type_prepayments", "asset_prepayments"
            "data_account_type_fixed_assets", "asset_fixed"
            "data_account_type_payable", "liability_payable"
            "data_account_type_credit_card", "liability_credit_card"
            "data_account_type_current_liabilities", "liability_current"
            "data_account_type_non_current_liabilities", "liability_non_current"
            "data_account_type_equity", "equity"
            "data_unaffected_earnings", "equity_unaffected"
            "data_account_type_revenue", "income"
            "data_account_type_other_income", "income_other"
            "data_account_type_expenses", "expense"
            "data_account_type_depreciation", "expense_depreciation"
            "data_account_type_direct_costs", "expense_direct_cost"
            "data_account_off_sheet", "off_balance"
        ]

        let sql = """
            with rel_account_account_type as (
                select name, res_id as id
                from ir_model_data
                where model = 'account.account.type'
            )
            select aa.*,
                   raat.name as user_type_external_id
            from account_account as aa
            join account_account_type as aat on aa.user_type_id = aat.id
            join rel_account_account_type as raat on aa.user_type_id = raat.id
            where aa.create_uid <> 1
            and not (aa.code like '41%%' or aa.code like '43%%')
            or aa.code in ('430150')
            order by aa.code
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountAccount.exportId
                reader.text "code"
                reader.text "name"
                accountTypeMap[reader.text "user_type_external_id"]
                reader.boolOrNone "reconcile" |> orEmptyString
                reader.int "last_visible_year" |> string
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPaymentTerm (modelName : string) =

        //--------------------------------------------------------------------------------------------------------------
        let exportAccountPaymentTerm (modelName : string) =
            let header = addStampHeadersTo [ "id" ; "name" ; "note" ; "sequence" ]

            let sql = """
                select *
                from account_payment_term
                order by create_date
                """

            let readerFun (reader : RowReader) =
                [
                    reader.intOrNone "id" |> AccountPaymentTerm.exportId
                    reader.text "name"
                    $"""<p>{reader.textOrNone "note" |> Option.defaultValue (reader.text "name")}</p>"""
                    reader.int "sequence" |> string
                    yield! readStampFields reader
                ]

            header::ISqlBroker.getExportData sql readerFun
            |> IExcelBroker.exportFile $"{modelName}.xlsx"
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let exportAccountPaymentTermLine (modelName : string) =

            let header = addStampHeadersTo [
                "id" ; "payment_id/id" ; "value" ; "value_amount" ; "nb_days" ; "days_next_month" ; "delay_type"
            ]

            let sql = """
                select *
                from account_payment_term_line
                order by create_date
                """

            let delayTypeMap = Map.ofList [
                "day_after_invoice_date", "days_after"
                "day_following_month", "days_end_of_month_on_the"
            ]

            let readerFun (reader : RowReader) =
                [
                    reader.int "id" |> Some |> AccountPaymentTermLine.exportId
                    reader.intOrNone "payment_id" |> AccountPaymentTerm.exportId

                    match reader.text "value" with
                    | "balance" -> "percent"
                    | value -> value

                    reader.doubleOrNone "value_amount" |> Option.defaultValue 0.0 |> string

                    let lineOption = reader.text "option"
                    let days = reader.int "days" |> string
                    let dayOfTheMonth = reader.intOrNone "day_of_the_month" |> Option.defaultValue 0 |> string

                    match lineOption with
                    | "day_following_month" -> "0"
                    | "day_after_invoice_date" when dayOfTheMonth <> "0" -> "0"
                    | _ -> days

                    dayOfTheMonth

                    match lineOption with
                    | "day_following_month" -> "days_after_end_of_next_month"
                    | _ when dayOfTheMonth = "0" -> delayTypeMap[lineOption]
                    | _ -> "days_end_of_month_on_the"

                    yield! readStampFields reader
                ]

            let termLines =

                let updatePercentInRow (percentValue : string) (row : string list) =
                    row
                    |> List.mapi (fun i colVal -> if i = 3 then percentValue else colVal)

                let updatePercentInGroup = function
                    | [singleRow] ->
                        [updatePercentInRow "100" singleRow]
                    | firstRow :: secondRow :: _ ->
                        let value = decimal firstRow[3]
                        [
                            firstRow
                            secondRow |> updatePercentInRow (string (100m - value))
                        ]
                    | [] -> []


                ISqlBroker.getExportData sql readerFun
                |> List.groupBy (List.item 1)
                |> List.collect (snd >> updatePercentInGroup)

            header::termLines
            |> IExcelBroker.exportFile $"{modelName}_line.xlsx"
        //--------------------------------------------------------------------------------------------------------------

        exportAccountPaymentTerm modelName
        exportAccountPaymentTermLine modelName
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportJournalBase (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "code"; "type" ; "sequence" ; "sequence_id/id"
            "bank_journal_id/id" ; "bank_cash_move_label"
            "n43_date_type" ; "default_account_id/id" ; "refund_sequence" ; "refund_sequence_id/id"
        ]

        let sql = """
            select aj.*,
				   aa.code as account_id
            from account_journal as aj
            left join account_account as aa on aj.default_account_id = aa.id
            where aj.code <> 'STJ'
            order by aj.bank_journal_id desc
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountJournal.exportId
                reader.text "name"
                reader.text "code"
                reader.text "type"
                reader.int "sequence" |> string
                reader.intOrNone "sequence_id" |> IrSequence.exportId

                reader.intOrNone "bank_journal_id" |> AccountJournal.exportId
                reader.textOrNone "bank_cash_move_label" |> orEmptyString

                reader.text "n43_date_type"
                reader.textOrNone "account_id" |> orEmptyString
                reader.boolOrNone "refund_sequence" |> orEmptyString
                reader.intOrNone "refund_sequence_id" |> IrSequence.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportJournalPaymentMode (modelName : string) =

        let header = addStampHeadersTo [ "id" ; "sales_payment_mode_id/id" ; "buys_payment_mode_id/id" ]

        let sql = """
            select aj.*
            from account_journal as aj
            where aj.code <> 'STJ'
			and (aj.sales_payment_mode_id is not null or aj.buys_payment_mode_id is not null)
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountJournal.exportId
                reader.intOrNone "sales_payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "buys_payment_mode_id" |> AccountPaymentMode.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportBankingMandate (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "message_main_attachment_id/id" ; "format" ; "type"; "partner_bank_id/id"
            "partner_id/id" ; "company_id/.id" ; "unique_mandate_reference" ; "signature_date"
            "last_debit_date" ; "state"  ; "display_name"; "recurrent_sequence_type" ; "scheme"
        ]

        let sql = $"""
            select *
            from account_banking_mandate as abm
            where abm.company_id={ORIG_COMPANY_ID}
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountBankingMandate.exportId
                reader.intOrNone "message_main_attachment_id" |> IrAttachment.exportId
                reader.text "format"
                reader.text "type"
                reader.intOrNone "partner_bank_id" |> ResPartnerBank.exportId
                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.int "company_id" |> string
                reader.text "unique_mandate_reference"
                reader.dateOnlyOrNone "signature_date" |> dateOrEmptyString
                reader.dateOnlyOrNone "last_debit_date" |> dateOrEmptyString
                reader.text "state"
                reader.text "display_name"
                reader.text "recurrent_sequence_type"
                reader.text "scheme"
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPaymentMethod (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "code" ; "payment_type" ; "bank_account_required"
            "payment_order_only" ; "mandate_required" ; "pain_version"
            "convert_to_ascii"
        ]

        let sql = """
            with model_data as (
                select name, res_id as id, module
                from ir_model_data
                where model = 'account.payment.method'
            )
            select md.name as md_id, md.module, apm.*
            from account_payment_method as apm
            join model_data as md on apm.id = md.id
            --where apm.id <> 3
            """

        let readerFun (reader : RowReader) =
            [
                reader.text "module" + "." + reader.text "md_id"
                reader.textOrNone "name" |> orEmptyString
                reader.textOrNone "code" |> orEmptyString
                reader.textOrNone "payment_type" |> orEmptyString
                reader.boolOrNone "bank_account_required" |> orEmptyString

                reader.boolOrNone "payment_order_only" |> orEmptyString
                reader.boolOrNone "mandate_required" |> orEmptyString
                reader.textOrNone "pain_version" |> orEmptyString

                reader.boolOrNone "convert_to_ascii" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPaymentMode (modelName : string) =

        let exportIdPrefix = AccountJournal.exportId <| Some ""

        let header = addStampHeadersTo [
            "id" ; "name" ; "bank_account_link" ; "fixed_journal_id/id"
            "initiating_party_identifier" ; "initiating_party_issuer"
            "initiating_party_scheme" ; "sepa_creditor_identifier"
            "payment_method_id/id" ; "payment_order_ok" ; "default_payment_mode"
            "default_invoice" ; "default_target_move" ; "default_date_type" ; "default_date_prefered"
            "group_lines" ; "default_journal_ids/id" ; "variable_journal_ids/id"
        ]

        let sql = $"""
            with model_data as (
                select name, res_id as id, module
                from ir_model_data
                where model = 'account.payment.method'
            ),
            pm_rel as (
                select account_payment_mode_id as payment_mode_id,
                       string_agg('{exportIdPrefix}' || cast(account_journal_id as varchar(100)), ',') as journal_ids
                from account_journal_account_payment_mode_rel
                group by account_payment_mode_id
            ),
            pm_variable as (
                select payment_mode_id as payment_mode_id,
                       string_agg('{exportIdPrefix}' || cast(journal_id as varchar(100)), ',') as journal_ids
                from account_payment_mode_variable_journal_rel
                group by payment_mode_id
            )

            select apm.*,
                   (md.module || '.' || md.name) as payment_method_external_id,
                   pmr.journal_ids as default_journal_ids,
                   pmv.journal_ids as variable_journal_ids
            from account_payment_mode as apm
            join model_data as md on apm.payment_method_id = md.id
            join pm_rel as pmr on apm.id = pmr.payment_mode_id
            left join pm_variable as pmv on apm.id = pmv.payment_mode_id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountPaymentMode.exportId
                reader.text "name"
                reader.textOrNone "bank_account_link" |> orEmptyString
                reader.intOrNone "fixed_journal_id" |> AccountJournal.exportId
                reader.textOrNone "initiating_party_identifier" |> orEmptyString
                reader.textOrNone "initiating_party_issuer" |> orEmptyString
                reader.textOrNone "initiating_party_scheme" |> orEmptyString
                reader.textOrNone "sepa_creditor_identifier" |> orEmptyString
                reader.textOrNone "payment_method_external_id" |> orEmptyString

                reader.boolOrNone "payment_order_ok" |> orEmptyString
                reader.textOrNone "default_payment_mode" |> orEmptyString

                reader.boolOrNone "default_invoice" |> orEmptyString
                reader.textOrNone "default_target_move" |> orEmptyString
                reader.textOrNone "default_date_type" |> orEmptyString
                reader.textOrNone "default_date_prefered" |> orEmptyString

                reader.boolOrNone "group_lines" |> orEmptyString
                reader.textOrNone "default_journal_ids" |> orEmptyString
                reader.textOrNone "variable_journal_ids" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportOpeningMove (modelName : string) =

        //--------------------------------------------------------------------------------------------------------------
        let header = [
            "id" ; "date" ; "name" ; "partner_id/id" ; "ref" ; "journal_id" ; "line_ids/account_id"
            "line_ids/partner_id/.id" ; "line_ids/name" ; "line_ids/debit" ; "line_ids/credit"
            "line_ids/date_maturity" ; "line_ids/payment_mode_id/id"
        ]

        let moveInfo =
            [
                Some $"dey_{OPENING_MOVE_YEAR}" |> AccountOpeningMove.exportId
                $"{OPENING_MOVE_YEAR}-01-01"
                "/"
                ""
                $"Asiento Apertura Deysanka {OPENING_MOVE_YEAR}"
                "Diario Operaciones varias"
            ]
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let detailsWithBalanceSql = $"""
            with
                account_list as (values
                    ('180000'), ('260000')
                ),
                active_partners as (
                    select aml.partner_id
                    from account_move_line as aml
                    join account_account as aa on aml.account_id = aa.id
                    join account_move as am on aml.move_id = am.id
                    where aml.company_id = {ORIG_COMPANY_ID}
                    and am.state = 'posted'
                    and aa.code in (select * from account_list)
                    group by aa.code, aml.partner_id
                    having round(sum(aml.debit) - sum(aml.credit), 2) <> 0.0
                )
            select aa.id, aa.code as account_id, aml.partner_id, rp.name, aml.ref, round(aml.debit, 2) as debit,
                   round(aml.credit, 2) as credit, round(aml.debit - aml.credit, 2) as balance
            from account_move_line as aml
            join account_account as aa on aml.account_id = aa.id
            join res_partner as rp on aml.partner_id = rp.id
            join account_move as am on aml.move_id = am.id
            where aml.company_id = {ORIG_COMPANY_ID}
            and aml.date < '{OPENING_MOVE_YEAR}-01-01'
            and am.state = 'posted'
            and aml.partner_id in (select partner_id from active_partners)
            and aa.code in (select * from account_list)
            order by aa.code, aml.partner_id
            """

        let detailsWithBalanceReaderFun (reader : RowReader) =
            [
                ""
                reader.text "account_id"
                reader.intOrNone "partner_id" |> orEmptyString
                reader.textOrNone "ref" |> orEmptyString
                reader.double "debit" |> formatDecimal
                reader.double "credit" |> formatDecimal
            ]
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let totalsBalanceSql =
            $"""
            with
                account_totals as (
                    select distinct aa.code, aa.name, 0 as partner_id, '' as ref, round(sum(aml.debit), 2) as debit,
                                    round(sum(aml.credit), 2) as credit
                    from account_move_line as aml
                    join account_move as am on aml.move_id = am.id
                    join account_account as aa on aml.account_id = aa.id
                    where aml.company_id = {ORIG_COMPANY_ID}
                    and aml.date < '{OPENING_MOVE_YEAR}-01-01'
                    and am.state = 'posted'
                    group by aa.code, aa.name
                )""" +
            """
            select at.code as account_id, at.name, at.partner_id, at.ref, at.debit, at.credit,
                   round(debit - credit, 2) as balance
            from account_totals as at
            where round(debit - credit, 2) <> 0.0
            and at.code similar to '(10|11|12|2|551|555|570|572)%'
            or at.code in ('300000',
                           '470010',
                           '523000',
                           '548002', '548003')
            order by 1
            """

        let doublesAreEqual (epsilon : double) (d1 : double) (d2 : double) : bool =
            Double.Abs (d1 - d2) < epsilon

        let areEqual_0001 = doublesAreEqual 0.0001
        let areNotEqual_0001 d1 d2 = not (doublesAreEqual 0.0001 d1 d2)

        let areEqual_001 = doublesAreEqual 0.001
        let areNotEqual_001 d1 d2 = not (doublesAreEqual 0.001 d1 d2)

        let totalsBalanceReaderFun (reader : RowReader) =
            [
                let balance = reader.double "balance"

                // if balance <> 0.0 then
                if areNotEqual_0001 balance 0.0 then
                    ""
                    reader.text "account_id"
                    ""   // Partner_id

                    $"Asiento Apertura {OPENING_MOVE_YEAR}"   // ref


                    if balance < 0.0 then "0.0"

                    balance |> abs |> formatDecimal

                    if balance > 0.0 then "0.0"
            ]
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let pendingMoveLinesSql = $"""
            with
                account_list as (values
                    ('171021'), ('171022'), ('400000'), ('410000'), ('411000'),
                    ('430000'), ('430100'), ('430150'), ('431500'),
                    ('436000'), ('440000'), ('460000'), ('465000'), ('470900'), ('471000'),
                    ('474500'), ('475000'), ('475100'), ('476000'), ('476001')
                ),
                lines_data as (
                    select aml.id, aa.code as account_id, aml.partner_id, aml.credit as amount,
                           aml.credit - sum(apr.amount) as residual, aml.ref, 'C' as move_type,
                           aml.date_maturity, aml.payment_mode_id, am.name as move_name
                    from account_move_line as aml
                    left join account_partial_reconcile as apr on aml.id = apr.credit_move_id
                    join account_account as aa on aml.account_id = aa.id
                    join account_move as am on aml.move_id = am.id
                    where aml.company_id = {ORIG_COMPANY_ID}
                    and aml.date < '{OPENING_MOVE_YEAR}-01-01'
                    and am.state = 'posted'
                    and aml.full_reconcile_id is null
                    and aml.balance <> 0.0
                    and aa.code in (select * from account_list)
                    and aml.credit > 0.0
                    group by aml.id, aa.code, am.name
                )
            select aml.id, aa.code as account_id, aml.partner_id, aml.debit as amount,
                   aml.debit - sum(apr.amount) as residual, aml.ref, 'D' as move_type,
                   aml.date_maturity, aml.payment_mode_id, am.name as move_name
            from account_move_line as aml
            left join account_partial_reconcile as apr on aml.id = apr.debit_move_id
            join account_account as aa on aml.account_id = aa.id
            join account_move as am on aml.move_id = am.id
            where aml.company_id = {ORIG_COMPANY_ID}
            and aml.date < '{OPENING_MOVE_YEAR}-01-01'
            and am.state = 'posted'
            and aml.full_reconcile_id is null
            and aml.balance <> 0.0
            and aa.code in (select * from account_list)
            and aml.credit <= 0.0
            group by aml.id, aa.code, am.name
            --having aml.debit - sum(apr.amount) <> 0.0
            union all
            select *
            from lines_data
            where residual <> 0.0 or (residual is null)
            order by account_id
            """

        let pendingMoveLinesReaderFun (reader : RowReader) =

            let shouldGenerateRow () =

                match reader.doubleOrNone "residual" with
                | Some residual -> not (residual = 0.0)
                | None -> true

            [
                if shouldGenerateRow() then
                    ""
                    reader.text "account_id"
                    reader.intOrNone "partner_id" |> orEmptyString

                    match reader.textOrNone "ref" with
                    | Some ref -> ref
                    | None -> reader.textOrNone "move_name" |> orEmptyString

                    if reader.text "move_type" = "C" then 0.0 |> formatDecimal

                    match reader.doubleOrNone "residual" with
                    | Some residual -> residual |> formatDecimal
                    | None -> (reader.double "amount") |> formatDecimal

                    if reader.text "move_type" = "D" then 0.0 |> formatDecimal

                    reader.dateOnlyOrNone "date_maturity" |> dateOrEmptyString
                    reader.intOrNone "payment_mode_id" |> AccountPaymentMode.exportId
            ]
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let detailsWithBalanceData = ISqlBroker.getExportData detailsWithBalanceSql detailsWithBalanceReaderFun

        let totalsBalanceData = ISqlBroker.getExportData totalsBalanceSql totalsBalanceReaderFun
                                |> List.filter (fun ml -> not ml.IsEmpty)

        let pendigMoveLinesData = ISqlBroker.getExportData pendingMoveLinesSql pendingMoveLinesReaderFun
                                  |> List.filter (fun ml -> not ml.IsEmpty)
        //--------------------------------------------------------------------------------------------------------------

        let allMoveLinesData = pendigMoveLinesData @
                               totalsBalanceData @
                               detailsWithBalanceData
                               |> List.sortBy (fun ml -> ml[COL_ACCOUNT])

        let totalDebit = allMoveLinesData
                         |> List.sumBy (fun ml -> ml[COL_DEBIT] |> double)

        let totalCredit = allMoveLinesData
                          |> List.sumBy (fun ml -> ml[COL_CREDIT] |> double)

        let total129 = totalDebit - totalCredit

        let tmp129 = [
            if total129 <> 0.0 then
                ""
                ""
                ""
                ""
                ""
                ""
                "129000"
                ""
                "Descuadre provisional"
                if total129 > 0.0 then 0.0 |> formatDecimal
                total129 |> abs |> formatDecimal
                if total129 < 0.0 then 0.0 |> formatDecimal
        ]

        let allMoveData = (flattenData [(moveInfo, allMoveLinesData)]) @
                          [tmp129]

        (header::allMoveData)
        |> List.map(
            fun l ->
                l
                |> List.map(fun c ->
                       if c = "__export__.res_partner_7"
                       then "l10n_es_aeat.res_partner_aeat"
                       else c)
                    )
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportMove (modelName : string) =

        failwith "Todavía no implementado al completo."
        //------------------------------------------------------------------------------------------------------------------
        let exportAccountMoveRelModel (modelName : string) (exportIdFun : ExportIdFun) (relFieldName : string) =

            let sql = $"""
                select id, {relFieldName}
                from account_move
                where {relFieldName} is not null
                order by date;
            """

            let header = [ "id" ; $"{relFieldName}/id" ]

            let readerFun (reader : RowReader) =
                [
                    reader.intOrNone "id" |> AccountMove.exportId
                    reader.intOrNone relFieldName |> exportIdFun
                ]

            header::ISqlBroker.getExportData sql readerFun
            |> IExcelBroker.exportFile $"{modelName}_{relFieldName}.xlsx"
        //------------------------------------------------------------------------------------------------------------------

        ExportAccount.exportAccountMoveBase modelName

        [
            ("message_main_attachment_id/id", IrAttachment.exportId)
            ("payment_id/id", AccountPayment.exportId)
            ("payment_order_id/id", AccountPaymentOrder.exportId)
            ("reversed_entry_id/id", AccountMove.exportId)
            ("statement_line_id/id", AccountBankStatementLine.exportId)
        ]
        |> List.iter (fun (relModelName, exportId) -> exportAccountMoveRelModel modelName exportId relModelName)
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member private exportAccountMoveBase (modelName : string) =

        failwith "Hay que arreglar lo la AEAT con left join y detectar el external_id."

        let sql = """
            select
                id, access_token, always_tax_exigible, amount_residual, amount_residual_signed,
                amount_tax, amount_tax_signed, amount_total, amount_total_in_currency_signed,
                amount_total_signed, amount_untaxed, amount_untaxed_signed, auto_post,
                campaign_id, commercial_partner_id, company_id, create_date, create_uid,
                currency_id, date, edi_state, financial_type, fiscal_position_id,
                inalterable_hash, invoice_cash_rounding_id, invoice_date, invoice_date_due,
                invoice_incoterm_id, invoice_origin, invoice_partner_display_name,
                invoice_payment_term_id, invoice_source_email, invoice_user_id, is_move_sent,
                journal_id, mandate_id, medium_id, message_main_attachment_id, move_type, name,
                narration, not_in_mod347, partner_bank_id, partner_id, partner_shipping_id,
                payment_id, payment_mode_id, payment_order_id, payment_reference, payment_state,
                posted_before, qr_code_method, ref, reference_type, reversed_entry_id,
                secure_sequence_number, sequence_number, sequence_prefix, source_id, state,
                statement_line_id, stock_move_id, tax_cash_basis_origin_move_id,
                tax_cash_basis_rec_id, team_id, thirdparty_invoice, thirdparty_number,
                to_check, write_date, write_uid
            from account_move
            order by date;
        """

        let header = [
            "id" ; "access_token" ; "always_tax_exigible" ; "amount_residual" ; "amount_residual_signed"
            "amount_tax" ; "amount_tax_signed" ; "amount_total" ; "amount_total_in_currency_signed"
            "amount_total_signed" ; "amount_untaxed" ; "amount_untaxed_signed" ; "auto_post" ; "campaign_id/id"
            "commercial_partner_id/id" ; "company_id/.id" ; "currency_id/.id" ; "date"
            "financial_type" ; "fiscal_position_id/.id" ; "invoice_date" ; "invoice_date_due"
            "invoice_origin" ; "invoice_partner_display_name" ; "invoice_payment_term_id/id"
            "invoice_source_email" ; "invoice_user_id/id" ; "is_move_sent" ; "journal_id/id"
            "move_type" ; "name" ; "narration" ; "not_in_mod347" ; "partner_bank_id/id"
            "partner_id/id" ; "partner_shipping_id/id" ; "payment_mode_id/id"
            "payment_reference" ; "payment_state" ; "posted_before" ; "qr_code_method"
            "ref" ; "reference_type" ; "secure_sequence_number" ; "sequence_number"
            "sequence_prefix" ; "source_id/id" ; "state" ; "stock_move_id/id"
            "tax_cash_basis_origin_move_id/id" ; "tax_cash_basis_rec_id/id" ; "team_id/.id"
            "thirdparty_invoice" ; "thirdparty_number" ; "to_check"
        ]

        let header = [
            "id" ; "access_token" ; "always_tax_exigible" ; "amount_residual" ; "amount_residual_signed"
            "amount_tax" ; "amount_tax_signed" ; "amount_total" ; "amount_total_in_currency_signed"
            "amount_total_signed" ; "amount_untaxed" ; "amount_untaxed_signed" ; "auto_post" ; "campaign_id/id"
            "commercial_partner_id/id" ; "company_id/.id" ; "currency_id/.id" ; "date"
            "financial_type" ; "fiscal_position_id/.id" ; "invoice_date" ; "invoice_date_due"
            "invoice_origin" ; "invoice_partner_display_name" ; "invoice_payment_term_id/id"
            "invoice_source_email" ; "invoice_user_id/id" ; "is_move_sent" ; "journal_id/id"
            //"move_type" ; "name" ; "narration" ; "not_in_mod347" ; "partner_bank_id/id"
            "name"
            "partner_id/id"
        ]

        // let header = [
        //     "id" ; "date"
        //     "journal_id/id"
        //     // "move_type"
        //     "name" ; "partner_id/id"
        // ]

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountMove.exportId
                reader.textOrNone "access_token" |> orEmptyString
                reader.bool "always_tax_exigible" |> string
                reader.double "amount_residual" |> formatDecimal
                reader.double "amount_residual_signed" |> formatDecimal
                reader.double "amount_tax" |> formatDecimal
                reader.double "amount_tax_signed" |> formatDecimal
                reader.double "amount_total" |> formatDecimal
                reader.double "amount_total_in_currency_signed" |> formatDecimal
                reader.double "amount_total_signed" |> formatDecimal
                reader.double "amount_untaxed" |> formatDecimal
                reader.double "amount_untaxed_signed" |> formatDecimal
                "No"         // auto_post
                ""           // campaign_id
                reader.intOrNone "commercial_partner_id/id" |> ResPartner.exportId
                "1"          // company_id
                "126"        // currency_id
                reader.dateOnlyOrNone "date" |> dateOrEmptyString
                reader.textOrNone "financial_type" |> orEmptyString
                reader.intOrNone "fiscal_position_id/id" |> orEmptyString
                reader.dateOnlyOrNone "invoice_date" |> dateOrEmptyString
                reader.dateOnlyOrNone "invoice_date_due" |> dateOrEmptyString
                reader.textOrNone "invoice_origin" |> orEmptyString
                reader.textOrNone "invoice_partner_display_name" |> orEmptyString
                reader.intOrNone "invoice_payment_term_id/id" |> AccountPaymentTerm.exportId
                reader.textOrNone "invoice_source_email" |> orEmptyString
                reader.intOrNone "invoice_user_id/id" |> ResUsers.exportId
                "false"      // is_move_sent
                reader.intOrNone "journal_id/id" |> AccountJournal.exportId
                // reader.text "move_type"
                reader.text "name"
                // reader.textOrNone "narration" |> orEmptyString
                // reader.bool "not_in_mod347" |> string
                // reader.intOrNone "partner_bank_id/id" |> ResPartnerBank.exportId
                reader.intOrNone "partner_id/id" |> ResPartner.exportId
                // reader.intOrNone "partner_shipping_id/id" |> ResPartner.exportId
                // reader.intOrNone "payment_mode_id/id" |> AccountPaymentMode.exportId
                // reader.textOrNone "payment_reference" |> orEmptyString
                // reader.textOrNone "payment_state" |> orEmptyString
                // reader.boolOrNone "posted_before" |> orEmptyString
                // reader.textOrNone "qr_code_method" |> orEmptyString
                // reader.textOrNone "ref" |> orEmptyString
                // reader.textOrNone "reference_type" |> orEmptyString
                // reader.intOrNone "secure_sequence_number" |> orEmptyString
                // reader.int "sequence_number" |> string
                // reader.text "sequence_prefix" |> string
                // reader.intOrNone "source_id/id" |> orEmptyString
                // "draft"                                 //  reader.text "state" |> string
                // reader.intOrNone "stock_move_id/id" |> orEmptyString
                // reader.intOrNone "tax_cash_basis_origin_move_id/id" |> orEmptyString
                // reader.intOrNone "tax_cash_basis_rec_id/id" |> orEmptyString
                // reader.int "team_id/id" |> string
                // reader.bool "thirdparty_invoice" |> string
                // reader.textOrNone "thirdparty_number" |> orEmptyString
                // reader.boolOrNone "to_check" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> List.take 50
        |> IExcelBroker.exportFile $"{modelName}_base.xlsx"
    //------------------------------------------------------------------------------------------------------------------
