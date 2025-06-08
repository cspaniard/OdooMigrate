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
            select
                raat.name as user_type_external_id,
                aa.*
            from account_account as aa
            join account_account_type as aat on aa.user_type_id = aat.id
            join rel_account_account_type as raat on aa.user_type_id = raat.id
            where (aa.create_uid <> 1 or (aa.code in ('570001', '572001')))
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
                with
                rel_account_payment_term as (
                    select module, model, res_id as id, module || '.' || name as external_id
                    from ir_model_data
                    where model = 'account.payment.term'
                    and module not like '\_\_%'
                )
                select
                    rapt.external_id as payment_term_external_id,
                    apt.*
                from account_payment_term as apt
                left join rel_account_payment_term as rapt on apt.id = rapt.id
                order by create_date
            """

            let readerFun (reader : RowReader) =
                [
                    match reader.textOrNone "payment_term_external_id" with
                    | Some value -> value
                    | None -> reader.int "id" |> Some |> AccountPaymentTerm.exportId

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
                with
                rel_account_payment_term as (
                    select module, model, res_id as id, module || '.' || name as external_id
                    from ir_model_data
                    where model = 'account.payment.term'
                    and module not like '\_\_%'
                )
                select
                    rapt.external_id as payment_term_external_id,
                    aptl.*
                from account_payment_term_line aptl
                left join rel_account_payment_term as rapt on aptl.payment_id = rapt.id
                order by create_date
            """

            let delayTypeMap = Map.ofList [
                "day_after_invoice_date", "days_after"
                "day_following_month", "days_end_of_month_on_the"
            ]

            let readerFun (reader : RowReader) =
                [
                    reader.int "id" |> Some |> AccountPaymentTermLine.exportId

                    match reader.textOrNone "payment_term_external_id" with
                    | Some value -> value
                    | None -> reader.int "payment_id" |> Some |> AccountPaymentTerm.exportId

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
    static member exportFullReconcile (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "exchange_move_id"
        ]

        let sql = """
            select
                afr.*
            from account_full_reconcile as afr
            order by id
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> AccountFullReconcile.exportId
                reader.text "name"
                reader.intOrNone "exchange_move_id" |> AccountMove.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPartialReconcile (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "debit_move_id/id" ; "credit_move_id/id" ; "full_reconcile_id/id" ; "debit_currency_id/id" ;
            "credit_currency_id/id" ; "amount" ; "debit_amount_currency" ; "credit_amount_currency" ;
            "company_id/.id" ; "max_date"
        ]

        let sql = """
            with
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            )
            select
                rrc_debit.external_id as debit_currency_external_id,
                rrc_credit.external_id as credit_currency_external_id,
                apr.*
            from account_partial_reconcile as apr
            join rel_res_currency as rrc_debit on apr.debit_currency_id = rrc_debit.id
            join rel_res_currency as rrc_credit on apr.credit_currency_id = rrc_credit.id
            order by id
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> AccountPartialReconcile.exportId
                reader.int "debit_move_id" |> Some |> AccountMoveLine.exportId
                reader.int "credit_move_id" |> Some |> AccountMoveLine.exportId
                reader.intOrNone "full_reconcile_id" |> AccountFullReconcile.exportId
                reader.text "debit_currency_external_id"
                reader.text "credit_currency_external_id"
                reader.doubleOrNone "amount" |> formatDecimalOption
                reader.doubleOrNone "debit_amount_currency" |> formatDecimalOption
                reader.doubleOrNone "credit_amount_currency" |> formatDecimalOption
                reader.intOrNone "company_id" |> orEmptyString
                reader.dateOnlyOrNone "max_date" |> dateOrEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportReconcileModel (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "message_main_attachment_id/id" ; "active" ; "name" ; "sequence" ; "company_id/.id"
            "rule_type" ; "auto_reconcile" ; "to_check" ; "matching_order" ; "match_text_location_label"
            "match_text_location_note" ; "match_text_location_reference" ; "match_nature" ; "match_amount"
            "match_amount_min" ; "match_amount_max" ; "match_label" ; "match_label_param" ; "match_note"
            "match_note_param" ; "match_transaction_type" ; "match_transaction_type_param"
            "match_same_currency" ; "allow_payment_tolerance" ; "payment_tolerance_param"
            "payment_tolerance_type" ; "match_partner" ; "past_months_limit" ; "decimal_separator"
        ]

        let sql = """
            select
                arm.*
            from account_reconcile_model as arm
            order by id
        """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountReconcileModel.exportId
                ""                // message_main_attachment_id
                reader.boolOrNone "active" |> orEmptyString
                reader.text "name"
                reader.int "sequence" |> string
                reader.int "company_id" |> string
                reader.text "rule_type"
                reader.boolOrNone "auto_reconcile" |> orEmptyString
                reader.boolOrNone "to_check" |> orEmptyString
                reader.text "matching_order"
                reader.boolOrNone "match_text_location_label" |> orEmptyString
                reader.boolOrNone "match_text_location_note" |> orEmptyString
                reader.boolOrNone "match_text_location_reference" |> orEmptyString
                reader.text "match_nature"
                reader.textOrNone "match_amount" |> orEmptyString
                reader.doubleOrNone "match_amount_min" |> formatDecimalOption
                reader.doubleOrNone "match_amount_max" |> formatDecimalOption
                reader.textOrNone "match_label" |> orEmptyString
                reader.textOrNone "match_label_param" |> orEmptyString
                reader.textOrNone "match_note" |> orEmptyString
                reader.textOrNone "match_note_param" |> orEmptyString
                reader.textOrNone "match_transaction_type" |> orEmptyString
                reader.textOrNone "match_transaction_type_param" |> orEmptyString
                reader.boolOrNone "match_same_currency" |> orEmptyString
                reader.boolOrNone "allow_payment_tolerance" |> orEmptyString
                reader.doubleOrNone "payment_tolerance_param" |> formatDecimalOption
                reader.text "payment_tolerance_type"
                reader.boolOrNone "match_partner" |> orEmptyString
                reader.intOrNone "past_months_limit" |> orEmptyString
                reader.textOrNone "decimal_separator" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportReconcileModelLine (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "model_id/id" ; "company_id/.id" ; "sequence" ; "account_id/id" ; "journal_id/id" ; "label"
            "amount_type" ; "force_tax_included" ; "amount" ; "amount_string" ; "analytic_account_id/id"
        ]

        let sql = """
            select
                arml.*
            from account_reconcile_model_line as arml
            order by id
        """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountReconcileModelLine.exportId
                reader.intOrNone "model_id" |> AccountReconcileModel.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.int "sequence" |> string
                reader.int "account_id" |> Some |> AccountAccount.exportId
                reader.intOrNone "journal_id" |> AccountJournal.exportId
                reader.textOrNone "label" |> orEmptyString
                reader.text "amount_type"
                reader.boolOrNone "force_tax_included" |> orEmptyString
                reader.doubleOrNone "amount" |> formatDecimalOption
                reader.text "amount_string"
                ""               //analytic_account_id
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPaymentOrder (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "message_main_attachment_id/id" ; "name" ; "payment_mode_id/id" ; "payment_type"
            "payment_method_id/id" ; "company_id/.id" ; "company_currency_id/id" ; "journal_id/id" ; "state"
            "date_prefered" ; "date_scheduled" ; "date_generated" ; "date_uploaded" ; "generated_user_id/id"
            "total_company_currency" ; "description" ; "charge_bearer" ; "batch_booking"
        ]

        let sql = """
            with
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            ),
			rel_account_payment_method as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'account.payment.method'
                and module not like '\_\_%'
            )
            select
                rapm.external_id as payment_method_external_id,
                rrc.external_id as company_currency_external_id,
                apo.*
            from account_payment_order as apo
            left join rel_account_payment_method as rapm on apo.payment_method_id = rapm.id
            left join rel_res_currency as rrc on apo.company_currency_id = rrc.id
            order by id
        """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountPaymentOrder.exportId
                ""                // message_main_attachment_id
                reader.textOrNone "name" |> orEmptyString
                reader.int "payment_mode_id" |> Some |> AccountPaymentMode.exportId
                reader.text "payment_type"
                reader.textOrNone "payment_method_external_id" |> orEmptyString
                reader.intOrNone "company_id" |> orEmptyString
                reader.textOrNone "company_currency_external_id" |> orEmptyString
                reader.intOrNone "journal_id" |> AccountJournal.exportId
                reader.textOrNone "state" |> orEmptyString
                reader.text "date_prefered"
                reader.dateOnlyOrNone "date_scheduled" |> dateOrEmptyString
                reader.dateOnlyOrNone "date_generated" |> dateOrEmptyString
                reader.dateOnlyOrNone "date_uploaded" |> dateOrEmptyString
                reader.intOrNone "generated_user_id" |> ResUsers.exportId
                reader.doubleOrNone "total_company_currency" |> formatDecimalOption
                reader.textOrNone "description" |> orEmptyString
                reader.textOrNone "charge_bearer" |> orEmptyString
                reader.boolOrNone "batch_booking" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPaymentLine (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "order_id/id" ; "company_id/.id" ; "company_currency_id/id" ; "payment_type" ; "state"
            "move_line_id/id" ; "currency_id/id" ; "amount_currency" ; "partner_id/id" ; "partner_bank_id/id"
            "date" ; "communication" ; "communication_type" ; "mandate_id/id" ; "priority" ; "local_instrument"
            "category_purpose" ; "purpose"
        ]

        let sql = """
            with
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            )
            select
                rrcc.external_id as company_currency_external_id,
                rrc.external_id as currency_external_id,
                apl.*
            from account_payment_line as apl
            left join rel_res_currency as rrcc on apl.company_currency_id = rrcc.id
            left join rel_res_currency as rrc on apl.company_currency_id = rrc.id
            order by id
        """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountPaymentLine.exportId
                reader.textOrNone "name" |> orEmptyString
                reader.intOrNone "order_id" |> AccountPaymentOrder.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.textOrNone "company_currency_external_id" |> orEmptyString
                reader.textOrNone "payment_type" |> orEmptyString
                reader.textOrNone "state" |> orEmptyString
                reader.intOrNone "move_line_id" |> AccountMoveLine.exportId
                reader.textOrNone "currency_external_id" |> orEmptyString
                reader.doubleOrNone "amount_currency" |> formatDecimalOption
                reader.int "partner_id" |> Some |> ResPartner.exportId
                reader.intOrNone "partner_bank_id" |> ResPartnerBank.exportId
                reader.dateOnlyOrNone "date" |> dateOrEmptyString
                reader.textOrNone "communication" |> orEmptyString
                reader.text "communication_type"
                reader.intOrNone "mandate_id" |> AccountBankingMandate.exportId
                reader.textOrNone "priority" |> orEmptyString
                reader.textOrNone "local_instrument" |> orEmptyString
                reader.textOrNone "category_purpose" |> orEmptyString
                reader.textOrNone "purpose" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPayment (modelName : string) =

        let getMissingPaymentMethodLineIds () =

            let sql = """
                with
                rel_a as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'account.payment.method'
                and module not like '\_\_%'
                )
                select
                    aj.code,
                    apml.id
                from rel_a
                join account_payment_method_line as apml on rel_a.id = apml.payment_method_id
                join account_journal as aj on apml.journal_id = aj.id
                where rel_a.external_id
                    in ('account_banking_sepa_credit_transfer.sepa_credit_transfer',
                        'account_banking_sepa_direct_debit.sepa_direct_debit')
                and aj.code in ('PAYC', 'PAYP')
            """

            let readerFun (reader : RowReader) =
                [
                    reader.text "code"
                    reader.int "id" |> Some|> AccountPaymentMethodLine.exportId
                ]

            ISqlBroker.getExportData sql readerFun
            |> List.map (fun r -> r[0], r[1])
            |> Map.ofList

        let missingPaymentMethodLineIds = getMissingPaymentMethodLineIds ()

        let header = addStampHeadersTo [
            "id" ; "message_main_attachment_id/id" ; "move_id/id" ; "is_reconciled" ; "is_matched"
            "partner_bank_id/id" ; "is_internal_transfer" ; "paired_internal_transfer_payment_id/id"
            "payment_method_line_id/id" ; "payment_method_id/id" ; "amount" ; "payment_type" ; "partner_type"
            "payment_reference" ; "currency_id/id" ; "partner_id/id" ; "outstanding_account_id/id"
            "destination_account_id/id" ; "destination_journal_id/id" ; "payment_transaction_id/id"
            "payment_token_id/id" ; "source_payment_id/id" ; "payment_order_id/id"
            "old_bank_payment_line_name" ; "old_bank_payment_line_id/id"
        ]

        let sql = """
            with
			rel_account_payment_method as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'account.payment.method'
                and module not like '\_\_%'
            ),
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            ),
			rel_account_account as (
                select module, model, res_id as id, 'account.' || name as external_id
                from ir_model_data
                where model = 'account.account'
                and module not like '\_\_%'
            )
            select
                rapm.external_id as payment_method_external_id,
                rrc.external_id as currency_external_id,
                roa.external_id as outstanding_account_external_id,
                rda.external_id as destination_account_external_id,
                ap.*
            from account_payment as ap
            left join rel_account_payment_method as rapm on ap.payment_method_id = rapm.id
            left join rel_res_currency as rrc on ap.currency_id = rrc.id
            left join rel_account_account as roa on ap.outstanding_account_id = roa.id
            left join rel_account_account as rda on ap.destination_account_id = rda.id
            order by id
        """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountPayment.exportId
                ""             // message_main_attachment_id
                reader.int "move_id" |> Some |> AccountMove.exportId
                reader.boolOrNone "is_reconciled" |> orEmptyString
                reader.boolOrNone "is_matched" |> orEmptyString
                reader.intOrNone "partner_bank_id" |> ResPartnerBank.exportId
                reader.boolOrNone "is_internal_transfer" |> orEmptyString
                reader.intOrNone "paired_internal_transfer_payment_id" |> AccountPayment.exportId

                match reader.intOrNone "payment_method_line_id" with
                | Some value -> value |> Some |> AccountPaymentMethodLine.exportId
                | None ->
                    if reader.text "payment_type" = "inbound"
                    then missingPaymentMethodLineIds["PAYC"]
                    else missingPaymentMethodLineIds["PAYP"]
                // reader.intOrNone "payment_method_line_id" |> AccountPaymentMethodLine.exportId

                reader.textOrNone "payment_method_external_id" |> orEmptyString
                reader.doubleOrNone "amount" |> formatDecimalOption
                reader.text "payment_type"
                reader.text "partner_type"
                reader.textOrNone "payment_reference" |> orEmptyString
                reader.textOrNone "currency_external_id" |> orEmptyString
                reader.int "partner_id" |> Some |> ResPartner.exportId

                match reader.textOrNone "outstanding_account_external_id" with
                | Some value -> value
                | None ->
                    match reader.intOrNone "outstanding_account_id" with
                    | Some value -> value |> Some |> AccountAccount.exportId
                    | None ->
                        if reader.text "payment_type" = "inbound"
                        then "account.1_account_common_4312"
                        else "account.1_account_common_411"

                match reader.textOrNone "destination_account_external_id" with
                | Some value -> value
                | None -> reader.intOrNone "destination_account_id" |> AccountAccount.exportId

                reader.intOrNone "destination_journal_id" |> AccountJournal.exportId
                ""              // payment_transaction_id
                ""              // payment_token_id
                reader.intOrNone "source_payment_id" |> AccountPayment.exportId
                reader.intOrNone "payment_order_id" |> AccountPaymentOrder.exportId
                reader.textOrNone "old_bank_payment_line_name" |> orEmptyString
                reader.intOrNone "old_bank_payment_line_id" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPaymentMethodLine (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "sequence" ; "payment_method_id/id" ; "payment_account_id/id"
            "journal_id/id" ; "payment_acquirer_id/id"
        ]

        let sql = """
            with
			rel_account_payment_method as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'account.payment.method'
                and module not like '\_\_%'
            )
            select
                rapm.external_id as payment_method_external_id,
                apml.*
            from account_payment_method_line as apml
            left join rel_account_payment_method as rapm on apml.payment_method_id = rapm.id
            order by id
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> AccountPaymentMethodLine.exportId
                reader.textOrNone "name" |> orEmptyString
                reader.intOrNone "sequence" |> orEmptyString
                reader.text "payment_method_external_id"
                reader.intOrNone "payment_account_id" |> AccountAccount.exportId
                reader.intOrNone "journal_id" |> AccountJournal.exportId
                ""           //  payment_acquirer_id
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportMove (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "sequence_prefix" ; "sequence_number" ; "message_main_attachment_id/id" ; "access_token"
            "name" ; "date" ; "ref" ; "narration" ; "state" ; "posted_before" ; "move_type" ; "to_check"
            "journal_id/id" ; "company_id/.id" ; "currency_id/id" ; "partner_id/id" ; "commercial_partner_id/id"
            "is_move_sent" ; "partner_bank_id/id" ; "payment_reference" ; "payment_id/id" ; "statement_line_id/id"
            "amount_untaxed" ; "amount_tax" ; "amount_total" ; "amount_residual" ; "amount_untaxed_signed"
            "amount_tax_signed" ; "amount_total_signed" ; "amount_total_in_currency_signed"
            "amount_residual_signed" ; "payment_state" ; "tax_cash_basis_rec_id/id"
            "tax_cash_basis_origin_move_id/id" ; "always_tax_exigible" ; "auto_post" ; "reversed_entry_id/id"
            "fiscal_position_id/id" ; "invoice_user_id/id" ; "invoice_date" ; "invoice_date_due"
            "invoice_origin" ; "invoice_payment_term_id/id" ; "invoice_incoterm_id/id" ; "qr_code_method"
            "invoice_source_email" ; "invoice_partner_display_name" ; "invoice_cash_rounding_id/id"
            "secure_sequence_number" ; "inalterable_hash"
            "edi_state" ; "stock_move_id/id" ; "campaign_id/id" ; "source_id/id" ; "medium_id/id"
            "team_id/id" ; "partner_shipping_id/id" ; "financial_type" ; "payment_mode_id/id" ; "payment_order_id/id"
            "reference_type" ; "mandate_id/id" ; "thirdparty_invoice" ; "thirdparty_number" ; "not_in_mod347"
        ]

        let sql = """
            with
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            ),
			rel_fiscal_position as (
                select module, model, res_id as id, 'account.' || name as external_id
                from ir_model_data
                where model = 'account.fiscal.position'
            ),
            rel_account_payment_term as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'account.payment.term'
                and module not like '\_\_%'
            ),
            rel_crm_team as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'crm.team'
                and module not like '\_\_%'
            ),
            rel_res_partner as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.partner'
                and module not like '\_\_%'
            )
            select
                rrc.external_id as currency_external_id,
                rfp.external_id as fiscal_position_external_id,
                rapt.external_id as invoice_payment_term_external_id,
                rct.external_id as team_external_id,
                rrp.external_id as partner_external_id,
                rcrp.external_id as commercial_partner_external_id,
                rps.external_id as partner_shipping_external_id,
                am.*
            from account_move as am
            left join rel_res_currency as rrc on am.currency_id = rrc.id
            left join rel_fiscal_position as rfp on am.fiscal_position_id = rfp.id
            left join rel_account_payment_term as rapt on am.invoice_payment_term_id = rapt.id
            left join rel_crm_team as rct on am.team_id = rct.id
            left join rel_res_partner as rrp on am.partner_id = rrp.id
            left join rel_res_partner as rcrp on am.commercial_partner_id = rcrp.id
            left join rel_res_partner as rps on am.partner_shipping_id = rps.id
            order by am.id
        """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountMove.exportId
                reader.textOrNone "sequence_prefix" |> orEmptyString
                reader.intOrNone "sequence_number" |> orEmptyString
                ""               // message_main_attachment_id
                reader.textOrNone "access_token" |> orEmptyString
                reader.textOrNone "name" |> orEmptyString
                reader.dateOnly "date" |> Some |> dateOrEmptyString
                reader.textOrNone "ref" |> orEmptyString
                reader.textOrNone "narration" |> orEmptyString
                reader.text "state"
                reader.boolOrNone "posted_before" |> orEmptyString
                reader.text "move_type"
                reader.boolOrNone "to_check" |> orEmptyString
                reader.int "journal_id" |> Some |> AccountJournal.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.text "currency_external_id"

                match reader.textOrNone "partner_external_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "partner_id" |> ResPartner.exportId

                match reader.textOrNone "commercial_partner_external_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "commercial_partner_id" |> ResPartner.exportId

                reader.boolOrNone "is_move_sent" |> orEmptyString
                reader.intOrNone "partner_bank_id" |> ResPartnerBank.exportId
                reader.textOrNone "payment_reference" |> orEmptyString
                reader.intOrNone "payment_id" |> AccountPayment.exportId
                reader.intOrNone "statement_line_id" |> AccountBankStatementLine.exportId
                reader.doubleOrNone "amount_untaxed" |> formatDecimalOption
                reader.doubleOrNone "amount_tax" |> formatDecimalOption
                reader.doubleOrNone "amount_total" |> formatDecimalOption
                reader.doubleOrNone "amount_residual" |> formatDecimalOption
                reader.doubleOrNone "amount_untaxed_signed" |> formatDecimalOption
                reader.doubleOrNone "amount_tax_signed" |> formatDecimalOption
                reader.doubleOrNone "amount_total_signed" |> formatDecimalOption
                reader.doubleOrNone "amount_total_in_currency_signed" |> formatDecimalOption
                reader.doubleOrNone "amount_residual_signed" |> formatDecimalOption
                reader.textOrNone "payment_state" |> orEmptyString
                reader.intOrNone "tax_cash_basis_rec_id" |> AccountPartialReconcile.exportId
                reader.intOrNone "tax_cash_basis_origin_move_id" |> AccountMove.exportId
                reader.boolOrNone "always_tax_exigible" |> orEmptyString
                reader.boolOrNone "auto_post" |> orEmptyString
                reader.intOrNone "reversed_entry_id" |> AccountMove.exportId
                reader.textOrNone "fiscal_position_external_id" |> orEmptyString
                reader.intOrNone "invoice_user_id" |> ResUsers.exportId
                reader.dateOnlyOrNone "invoice_date" |> dateOrEmptyString
                reader.dateOnlyOrNone "invoice_date_due" |> dateOrEmptyString
                reader.textOrNone "invoice_origin" |> orEmptyString

                match reader.textOrNone "invoice_payment_term_external_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "invoice_payment_term_id" |> AccountPaymentTerm.exportId

                ""                     // invoice_incoterm_id
                reader.textOrNone "qr_code_method" |> orEmptyString
                reader.textOrNone "invoice_source_email" |> orEmptyString
                reader.textOrNone "invoice_partner_display_name" |> orEmptyString
                ""                    // invoice_cash_rounding_id
                reader.intOrNone "secure_sequence_number" |> orEmptyString
                reader.textOrNone "inalterable_hash" |> orEmptyString
                reader.textOrNone "edi_state" |> orEmptyString
                reader.intOrNone "stock_move_id" |> StockMove.exportId
                ""                    // campaign_id
                ""                    // source_id
                ""                    // medium_id
                reader.textOrNone "team_external_id" |> orEmptyString

                match reader.textOrNone "partner_shipping_external_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "partner_shipping_id" |> ResPartner.exportId

                reader.textOrNone "financial_type" |> orEmptyString
                reader.intOrNone "payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "payment_order_id" |> AccountPaymentOrder.exportId
                reader.textOrNone "reference_type" |> orEmptyString
                reader.intOrNone "mandate_id" |> AccountBankingMandate.exportId
                reader.boolOrNone "thirdparty_invoice" |> orEmptyString
                reader.textOrNone "thirdparty_number" |> orEmptyString
                reader.boolOrNone "not_in_mod347" |> orEmptyString

                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportMoveLine (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "move_id/id" ; "move_name" ; "date" ; "ref" ; "parent_state" ; "journal_id/id" ; "company_id/.id"
            "company_currency_id/id" ; "account_id/id" ; "sequence" ; "name" ; "quantity"
            "price_unit" ; "discount" ; "debit" ; "credit" ; "balance" ; "amount_currency" ; "price_subtotal"
            "price_total" ; "reconciled" ; "blocked" ; "date_maturity" ; "currency_id/id" ; "partner_id/id"
            "product_uom_id/id" ; "product_id/id" ; "reconcile_model_id/id" ; "payment_id/id" ; "statement_line_id/id"
            "statement_id/id" ; "group_tax_id/id" ; "tax_line_id/id" ; "tax_group_id/id" ; "tax_base_amount"
            "tax_repartition_line_id/id" ; "tax_audit" ; "tax_tag_invert" ; "amount_residual"
            "amount_residual_currency" ; "full_reconcile_id/id" ; "matching_number" ; "analytic_account_id/id"
            "display_type" ; "is_rounding_line" ; "exclude_from_invoice_tab"
            "is_anglo_saxon_line" ; "purchase_line_id/id" ; "payment_mode_id/id" ; "partner_bank_id/id"
        ]

        let sql = """
            with
			rel_res_partner as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.partner'
                and module not like '\_\_%'
            ),
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            ),
			rel_account_account as (
                select module, model, res_id as id, 'account.' || name as external_id
                from ir_model_data
                where model = 'account.account'
            ),
			rel_uom_uom as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'uom.uom'
            ),
			rel_account_tax as (
                select module, model, res_id as id, 'account.' || name as external_id
                from ir_model_data
                where model = 'account.tax'
            ),
			rel_account_tax_group as (
                select module, model, res_id as id, 'account.1_' || name as external_id
                from ir_model_data
                where model = 'account.tax.group'
            )
            select
                rrcc.external_id as company_currency_external_id,
                raa.external_id as account_external_id,
                rrc.external_id as currency_external_id,
                ruu.external_id as product_uom_external_id,
                rgt.external_id as group_tax_external_id,
                rtl.external_id as tax_line_external_id,
                ratg.external_id as tax_group_external_id,
                rrp.external_id as partner_external_id,
                aml.*
            from account_move_line as aml
            left join rel_res_currency as rrcc on aml.company_currency_id = rrcc.id
            left join rel_account_account as raa on aml.account_id = raa.id
            left join rel_res_currency as rrc on aml.currency_id = rrc.id
            left join rel_uom_uom as ruu on aml.product_uom_id = ruu.id
            left join rel_account_tax as rgt on aml.group_tax_id = rgt.id
            left join rel_account_tax as rtl on aml.tax_line_id = rtl.id
            left join rel_account_tax_group as ratg on aml.tax_group_id = ratg.id
            left join rel_res_partner as rrp on aml.partner_id = rrp.id
            order by aml.id
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> AccountMoveLine.exportId
                reader.int "move_id" |> Some |> AccountMove.exportId
                reader.textOrNone "move_name" |> orEmptyString
                reader.dateOnlyOrNone "date" |> dateOrEmptyString
                reader.textOrNone "ref" |> orEmptyString
                reader.textOrNone "parent_state" |> orEmptyString
                reader.intOrNone "journal_id" |> AccountJournal.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.text "company_currency_external_id"

                match reader.textOrNone "account_external_id" with
                | Some externalId ->
                    if externalId.StartsWith "account.1" then
                       if externalId = "account.1_account_chart_template_common_liquidity_transfer" then
                           "account.1_transfer_account_id"
                       else
                           externalId
                    else
                       reader.intOrNone "account_id" |> AccountAccount.exportId
                | None -> reader.intOrNone "account_id" |> AccountAccount.exportId

                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "name" |> orEmptyString
                reader.doubleOrNone "quantity" |> formatDecimalOption
                reader.doubleOrNone "price_unit" |> formatDecimalOption
                reader.doubleOrNone "discount" |> formatDecimalOption
                reader.doubleOrNone "debit" |> formatDecimalOption
                reader.doubleOrNone "credit" |> formatDecimalOption
                reader.doubleOrNone "balance" |> formatDecimalOption
                reader.doubleOrNone "amount_currency" |> formatDecimalOption
                reader.doubleOrNone "price_subtotal" |> formatDecimalOption
                reader.doubleOrNone "price_total" |> formatDecimalOption
                reader.boolOrNone "reconciled" |> orEmptyString            // 2 ?
                reader.boolOrNone "blocked" |> orEmptyString
                reader.dateOnlyOrNone "date_maturity" |> dateOrEmptyString
                reader.text "currency_external_id"

                match reader.textOrNone "partner_external_id" with
                | Some externalId ->
                    if externalId = "l10n_es_aeat.res_partner_aeat" then
                        externalId
                        // "res.partner.res_partner_aeat"
                    else
                        externalId
                | None -> reader.intOrNone "partner_id" |> ResPartner.exportId

                reader.textOrNone "product_uom_external_id" |> orEmptyString
                reader.intOrNone "product_id" |> ProductProduct.exportId
                reader.intOrNone "reconcile_model_id" |> AccountReconcileModel.exportId
                reader.intOrNone "payment_id" |> AccountPayment.exportId
                reader.intOrNone "statement_line_id" |> AccountBankStatementLine.exportId
                reader.intOrNone "statement_id" |> AccountBankStatement.exportId
                reader.textOrNone "group_tax_external_id" |> orEmptyString
                reader.textOrNone "tax_line_external_id" |> orEmptyString
                reader.textOrNone "tax_group_external_id" |> orEmptyString
                reader.doubleOrNone "tax_base_amount" |> formatDecimalOption
                reader.intOrNone "tax_repartition_line_id" |> AccountTaxRepartitionLine.exportId
                reader.textOrNone "tax_audit" |> orEmptyString
                reader.boolOrNone "tax_tag_invert" |> orEmptyString
                reader.doubleOrNone "amount_residual" |> formatDecimalOption
                reader.doubleOrNone "amount_residual_currency" |> formatDecimalOption
                reader.intOrNone "full_reconcile_id" |> AccountFullReconcile.exportId
                reader.textOrNone "matching_number" |> orEmptyString
                ""              // analytic_account_id
                reader.textOrNone "display_type" |> orEmptyString
                reader.boolOrNone "is_rounding_line" |> orEmptyString
                reader.boolOrNone "exclude_from_invoice_tab" |> orEmptyString
                reader.boolOrNone "is_anglo_saxon_line" |> orEmptyString
                reader.intOrNone "purchase_line_id" |> PurchaseOrderLine.exportId
                reader.intOrNone "payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "partner_bank_id" |> ResPartnerBank.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportTaxRepartitionLine (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "factor_percent" ; "repartition_type" ; "account_id/id" ; "invoice_tax_id/id" ; "refund_tax_id/id"
            "company_id/.id" ; "sequence" ; "use_in_tax_closing"
        ]

        let sql = """
            with
			rel_account_account as (
                select module, model, res_id as id, 'account.' || name as external_id
                from ir_model_data
                where model = 'account.account'
            ),
			rel_account_tax as (
                select module, model, res_id as id, 'account.' || name as external_id
                from ir_model_data
                where model = 'account.tax'
            )
            select
                raa.external_id as account_external_id,
                rit.external_id as invoice_tax_external_id,
                rrt.external_id as refund_tax_external_id,
                atrl.*
            from account_tax_repartition_line as atrl
            left join rel_account_account as raa on atrl.account_id = raa.id
            left join rel_account_tax rit on atrl.invoice_tax_id = rit.id
            left join rel_account_tax rrt on atrl.refund_tax_id = rrt.id
            order by atrl.id
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> AccountTaxRepartitionLine.exportId
                reader.double "factor_percent" |> Some |> formatDecimalOption
                reader.text "repartition_type"
                reader.textOrNone "account_external_id" |> orEmptyString
                reader.textOrNone "invoice_tax_external_id" |> orEmptyString
                reader.textOrNone "refund_tax_external_id" |> orEmptyString
                reader.intOrNone "company_id" |> orEmptyString
                reader.intOrNone "sequence" |> orEmptyString
                reader.boolOrNone "use_in_tax_closing" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportBankStatement (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "sequence_prefix" ; "sequence_number" ; "name" ; "reference"
            "date" ; "date_done" ; "balance_start" ; "balance_end_real" ; "state" ; "journal_id/id" ; "company_id/.id"
            "total_entry_encoding" ; "balance_end" ; "difference" ; "user_id/id" ; "cashbox_start_id/id"
            "cashbox_end_id/id" ; "previous_statement_id/id" ; "is_valid_balance_start" ; "skit_accounting_date"
        ]

        let sql = """
            select
                abs.*
            from account_bank_statement as abs
            order by abs.id
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> AccountBankStatement.exportId
                reader.textOrNone "sequence_prefix" |> orEmptyString
                reader.intOrNone "sequence_number" |> orEmptyString
                reader.textOrNone "name" |> orEmptyString
                reader.textOrNone "reference" |> orEmptyString
                reader.dateOnly "date" |> Some |> dateOrEmptyString
                reader.dateTimeOrNone "date_done" |> orEmptyString
                reader.doubleOrNone "balance_start" |> formatDecimalOption
                reader.doubleOrNone "balance_end_real" |> formatDecimalOption
                reader.text "state"
                reader.intOrNone "journal_id" |> AccountJournal.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.doubleOrNone "total_entry_encoding" |> formatDecimalOption
                reader.doubleOrNone "balance_end" |> formatDecimalOption
                reader.doubleOrNone "difference" |> formatDecimalOption
                reader.intOrNone "user_id" |> ResUsers.exportId
                reader.intOrNone "cashbox_start_id" |> orEmptyString
                reader.intOrNone "cashbox_end_id" |> orEmptyString
                reader.intOrNone "previous_statement_id" |> AccountBankStatement.exportId
                reader.boolOrNone "is_valid_balance_start" |> orEmptyString
                reader.dateOnlyOrNone "skit_accounting_date" |> dateOrEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportBankStatementLine (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "move_id/id" ; "statement_id/id" ; "sequence" ; "account_number" ; "partner_name"
            "transaction_type" ; "payment_ref" ; "amount" ; "amount_currency" ; "foreign_currency_id/id"
            "amount_residual" ; "currency_id/id" ; "partner_id/id" ; "is_reconciled"
            "unique_import_id/id" ; "journal_entry" ; "n43_line" ; "sale_date" ; "raw_data"
        ]

        let sql = """
            with
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            )
            select
                rfc.external_id as foreign_currency_external_id,
                rc.external_id as currency_external_id,
                absl.*
            from account_bank_statement_line as absl
            left join rel_res_currency as rfc on absl.foreign_currency_id = rfc.id
            left join rel_res_currency as rc on absl.currency_id = rc.id
            order by absl.id
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> AccountBankStatementLine.exportId
                reader.intOrNone "move_id" |> AccountMove.exportId
                reader.int "statement_id" |> Some |> AccountBankStatement.exportId
                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "account_number" |> orEmptyString
                reader.textOrNone "partner_name" |> orEmptyString
                reader.textOrNone "transaction_type" |> orEmptyString
                reader.text "payment_ref"
                reader.doubleOrNone "amount" |> formatDecimalOption
                reader.doubleOrNone "amount_currency" |> formatDecimalOption
                reader.textOrNone "foreign_currency_external_id" |> orEmptyString
                reader.doubleOrNone "amount_residual" |> formatDecimalOption
                reader.textOrNone "currency_external_id" |> orEmptyString
                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.boolOrNone "is_reconciled" |> orEmptyString
                reader.textOrNone "unique_import_id" |> orEmptyString
                reader.textOrNone "journal_entry" |> orEmptyString
                reader.textOrNone "n43_line" |> orEmptyString
                reader.dateOnlyOrNone "sale_date" |> dateOrEmptyString
                reader.textOrNone "raw_data" |> orEmptyString

                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportMove_old (modelName : string) =

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
