namespace Services.Exporting.Odoo

open System
open System.Globalization
open Motsoft.Util
open Model
open Model.Constants

type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker

type Service () =

    //------------------------------------------------------------------------------------------------------------------
    static let [<Literal>] COL_ACCOUNT = 1
    static let [<Literal>] COL_DEBIT = 4
    static let [<Literal>] COL_CREDIT = 5
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static let formatDecimalOption (valueOption : double option) =
        match valueOption with
        | Some value -> value.ToString("########0.00", CultureInfo.InvariantCulture)
        | None -> ""
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static let formatDecimal (value : double) =
        value.ToString("########0.00", CultureInfo.InvariantCulture)
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static let flattenData (joinData : (string list * string list list) list) =
        [
            for recordData, recordLines in joinData do
                recordData @ (recordLines |> List.head |> List.tail)
                for recordLine in (recordLines |> List.tail) do
                    [for _ in recordData -> ""] @ (recordLine |> List.tail)
        ]
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static let orEmptyString (optVal : 'a option) =
        match optVal with
        | Some value -> value |> string
        | None -> ""
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static let dateOrEmptyString (optVal : DateOnly option) =
        match optVal with
        | Some d -> d.ToString("yyyy-MM-dd")
        | None -> ""
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static let dateTimeOrEmptyString (optVal : DateTime option) =
        match optVal with
        | Some d -> d.ToString("yyyy-MM-dd HH:mm:ss")
        | None -> ""
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResBank (modelName : string) =

        let header = [ "id" ; "name" ; "bic" ; "country/id" ]

        let sql = """
            select id, name, bic
            from res_bank
            where active=true
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> Bank.exportId
                reader.text "name"
                reader.textOrNone "bic" |> orEmptyString
                "base.es"      // country/id
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResPartnerBank (modelName : string) =

        let header = [
            "id" ; "bank_id/id" ; "acc_number"; "sequence" ; "partner_id/id" ; "acc_holder_name" ; "description"
        ]

        let sql = $"""
            select rpb.id, rpb.acc_number, rpb.sequence, rpb.partner_id, rpb.bank_id,
                   rpb.acc_holder_name, rpb.description
            from res_partner_bank as rpb
            join res_partner as rp on rpb.partner_id = rp.id
            where rpb.company_id={ORIG_COMPANY_ID}
            and rp.active = true
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ResPartnerBank.exportId
                reader.intOrNone "bank_id" |> Bank.exportId
                reader.text "acc_number"
                reader.int "sequence" |> string
                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.textOrNone "acc_holder_name" |> orEmptyString
                reader.textOrNone "description" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountPaymentTerm (modelName : string) =

        //--------------------------------------------------------------------------------------------------------------
        let exportAccountPaymentTerm (modelName : string) =
            let header = [ "id" ; "name" ; "note" ; "sequence" ]

            let sql = """
                select id, name, note, sequence
                from account_payment_term
                """

            let readerFun (reader : RowReader) =
                [
                    reader.intOrNone "id" |> AccountPaymentTerm.exportId
                    reader.text "name"
                    $"""<p>{reader.textOrNone "note" |> Option.defaultValue (reader.text "name")}</p>"""
                    reader.int "sequence" |> string
                ]

            header::ISqlBroker.getExportData sql readerFun
            |> IExcelBroker.exportFile $"{modelName}.xlsx"
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let exportAccountPaymentTermLine (modelName : string) =

            let header = [ "id" ; "payment_id/id" ; "value" ; "value_amount" ; "nb_days"
                           "days_next_month" ; "delay_type" ]

            let sql = """
                select id, value, value_amount, days, day_of_the_month, option, payment_id, sequence
                from account_payment_term_line
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
    static member exportResUsers (modelName : string) =

        let header = [ "id" ; "login"; "name" ; "notification_type" ; "team_id/.id"
                       "working_year" ; "lowest_working_date" ; "action_id/id" ]

        let sql = """
            with
			rel_action_action as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'ir.actions.act_window'
                and module not like '\_\_%'
			)
            select res_users.id, login, name, notification_type, working_year, lowest_working_date,
                   raa.external_id as action_external_id
            from res_users
            join res_partner on res_users.partner_id = res_partner.id
            left join rel_action_action as raa on res_users.action_id = raa.id
            where res_users.active = true
            and res_users.company_id=""" + ORIG_COMPANY_ID

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ResUsers.exportId
                reader.text "login"
                reader.text "name"
                reader.text "notification_type"
                "1"
                // "sales_team.team_sales_department"     // sale_team_id
                reader.textOrNone "working_year" |> orEmptyString
                reader.dateOnlyOrNone "lowest_working_date" |> dateOrEmptyString
                reader.textOrNone "action_external_id" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResPartner (modelName : string) =

        //--------------------------------------------------------------------------------------------------------------
        let getDefaultAccountCode (propertyName : string) : string =
            let sql = $"""
                with rel_payable as (
                    select split_part(value_reference, ',', 2)::integer as account_id
                    from ir_property
                    where name = '{propertyName}'
                    and res_id is null
                )
                select code
                from account_account
                join rel_payable on rel_payable.account_id = account_account.id
                """

            (ISqlBroker.getExportData sql (fun reader -> [ reader.text "code" ])).Head.Head
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let defaultAccountReceivableCode = getDefaultAccountCode "property_account_receivable_id"
        let defaultAccountPayableCode = getDefaultAccountCode "property_account_payable_id"
        //--------------------------------------------------------------------------------------------------------------

        let header = [ "id" ; "name" ; "lang" ; "tz" ; "user_id/id" ; "parent_id/id"
                       "vat" ; "website" ; "comment" ; "type" ; "street" ; "street2" ; "zip" ; "city"
                       "state_id/id" ; "country_id" ; "email" ; "phone" ; "mobile" ; "is_company"
                       "customer" ; "supplier" ; "alternative_name" ; "comercial" ; "bank_name" ; "not_in_mod347"
                       "sale_journal_id/id" ; "purchase_journal_id/id" ; "aeat_anonymous_cash_customer"
                       "property_account_receivable_id" ; "property_account_payable_id"
                       "property_payment_term_id/id" ; "customer_payment_mode_id/id" ; "supplier_payment_mode_id/id"
                       "property_product_pricelist/id" ; "property_account_position_id/id" ]

        let sql = """
            with
            rel_payable as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as partner_id,
                       split_part(value_reference, ',', 2)::integer as account_id
                from ir_property
                where name = 'property_account_payable_id'
                and res_id is not null
            ),
            rel_receivable as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as partner_id,
                       split_part(value_reference, ',', 2)::integer as account_id
                from ir_property
                where name = 'property_account_receivable_id'
                and res_id is not null
            ),
            rel_payment_term as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as partner_id,
                       split_part(value_reference, ',', 2)::integer as payment_term_id
                from ir_property
                where name = 'property_payment_term_id'
                and res_id is not null
            ),
            rel_customer_payment_mode as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as partner_id,
                       split_part(value_reference, ',', 2)::integer as payment_mode_id
                from ir_property
                where name = 'customer_payment_mode_id'
                and res_id is not null
            ),
            rel_supplier_payment_mode_id as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as partner_id,
                       split_part(value_reference, ',', 2)::integer as payment_mode_id
                from ir_property
                where name = 'supplier_payment_mode_id'
                and res_id is not null
            ),
            rel_product_pricelist as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as partner_id,
                       split_part(value_reference, ',', 2)::integer as product_pricelist
                from ir_property
                where name = 'property_product_pricelist'
                and res_id is not null
            ),
            rel_account_position as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as partner_id,
                       split_part(value_reference, ',', 2)::integer as account_position
                from ir_property
                where name = 'property_account_position_id'
                and res_id is not null
            )
            select rp.id, rp.name, rp.lang, rp.tz, rp.user_id, rp.parent_id,
                   rp.vat, rp.website, rp.comment, rp.type, rp.street, rp.street2, rp.zip, rp.city,
                   rcs.code as state_id, rp.country_id, rp.email, rp.phone, rp.mobile, rp.is_company,
                   rp.not_in_mod347,
                   rp.sale_journal, rp.purchase_journal, rp.aeat_anonymous_cash_customer,
                   rp.customer, rp.supplier, rp.alternative_name, rp.comercial, rp.bank_name,
                   acc_rec.code as property_account_receivable_id, acc.code as property_account_payable_id,
                   apt.id as account_payment_term_id, rcpm.payment_mode_id as customer_payment_mode_id,
                   rspm.payment_mode_id as supplier_payment_mode_id,
                   rppl.product_pricelist as property_product_pricelist,
                   'account.' || imd.name AS fiscal_position_external_id
            from res_partner as rp
            left join rel_payable as pay on rp.id = pay.partner_id
            left join rel_receivable as rec on rp.id = rec.partner_id
            left join account_account as acc on pay.account_id = acc.id
            left join account_account as acc_rec on rec.account_id = acc_rec.id
            left join res_country_state as rcs on rp.state_id = rcs.id
            left join rel_payment_term as rp_term on rp.id = rp_term.partner_id
            left join account_payment_term as apt on rp_term.payment_term_id = apt.id
            left join rel_customer_payment_mode as rcpm on rp.id = rcpm.partner_id
            left join rel_supplier_payment_mode_id as rspm on rp.id = rspm.partner_id
            left join rel_product_pricelist as rppl on rp.id = rppl.partner_id
            left join rel_account_position as rap on rp.id = rap.partner_id
            left join account_fiscal_position as afp on rap.account_position = afp.id
            left join ir_model_data imd ON imd.model = 'account.fiscal.position' AND imd.res_id = afp.id
            where rp.customer is not null
            and rp.active = true
            or rp.name ilike 'Deysanka SL'
            order by rp.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ResPartner.exportId
                reader.text "name"
                reader.textOrNone "lang" |> orEmptyString
                reader.textOrNone "tz" |> orEmptyString
                reader.intOrNone "user_id" |> ResUsers.exportId
                reader.intOrNone "parent_id" |> ResPartner.exportId

                reader.textOrNone "vat" |> orEmptyString
                reader.textOrNone "website" |> orEmptyString
                reader.textOrNone "comment" |> orEmptyString
                reader.text "type"
                reader.textOrNone "street" |> orEmptyString
                reader.textOrNone "street2" |> orEmptyString
                reader.textOrNone "zip" |> orEmptyString
                reader.textOrNone "city" |> orEmptyString

                match reader.textOrNone "state_id" with
                | Some state_id -> $"base.state_es_{state_id}".ToLower()
                | None -> ""
                "ES"  // reader.intOrNone "country_id" |> withDefaultValue
                reader.textOrNone "email" |> orEmptyString
                reader.textOrNone "phone" |> orEmptyString
                reader.textOrNone "mobile" |> orEmptyString
                reader.bool "is_company" |> string

                reader.bool "customer" |> string
                reader.bool "supplier" |> string
                reader.textOrNone "alternative_name" |> orEmptyString
                reader.textOrNone "comercial" |> orEmptyString
                reader.textOrNone "bank_name" |> orEmptyString

                reader.boolOrNone "not_in_mod347" |> orEmptyString

                reader.intOrNone "sale_journal" |> AccountJournal.exportId
                reader.intOrNone "purchase_journal" |> AccountJournal.exportId
                reader.boolOrNone "aeat_anonymous_cash_customer" |> orEmptyString

                reader.textOrNone "property_account_receivable_id" |> Option.defaultValue defaultAccountReceivableCode
                reader.textOrNone "property_account_payable_id" |> Option.defaultValue defaultAccountPayableCode

                reader.intOrNone "account_payment_term_id" |> AccountPaymentTerm.exportId
                reader.intOrNone "customer_payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "supplier_payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "property_product_pricelist" |> ProductPriceList.exportId
                reader.stringOrNone "fiscal_position_external_id" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountAccount (modelName : string) =

        let header = [ "id" ; "code" ; "name"; "account_type_id"
                       "reconcile" ; "last_visible_year" ]

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
            with model_data as (
                select name, res_id as id
                from ir_model_data
                where model = 'account.account.type'
            )
            select aa.id, aa.code, aa.name, md.name as user_type_id, aa.reconcile, aa.last_visible_year
            from account_account as aa
            join account_account_type as aat on aa.user_type_id = aat.id
            join model_data as md on aa.user_type_id = md.id
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
                accountTypeMap[reader.text "user_type_id"]
                reader.boolOrNone "reconcile" |> orEmptyString
                reader.int "last_visible_year" |> string
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountJournalBase (modelName : string) =

        let header = [
            "id" ; "name" ; "code"; "type" ; "sequence" ; "sequence_id/id"
            "bank_journal_id/id" ; "bank_cash_move_label"
            "n43_date_type" ; "default_account_id" ; "refund_sequence" ; "refund_sequence_id/id"
        ]

        let sql = """
            select aj.id, aj.name, aj.code, aj.type, aj.sequence, aj.sequence_id,
                   aj.bank_journal_id, aj.bank_cash_move_label,
				   aj.sales_payment_mode_id, aj.buys_payment_mode_id,
				   n43_date_type, aa.code as account_id, aj.refund_sequence, aj.refund_sequence_id
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
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountJournalPaymentMode (modelName : string) =

        let header = [ "id" ; "sales_payment_mode_id/id" ; "buys_payment_mode_id/id" ]

        let sql = """
            select aj.id, aj.sales_payment_mode_id, aj.buys_payment_mode_id
            from account_journal as aj
            where aj.code <> 'STJ'
			and (aj.sales_payment_mode_id is not null or aj.buys_payment_mode_id is not null)
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountJournal.exportId
                reader.intOrNone "sales_payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "buys_payment_mode_id" |> AccountPaymentMode.exportId
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountBankingMandate (modelName : string) =

        let header = [
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
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductPriceList (modelName : string) =

        let header = [ "id" ; "name" ; "sequence"; "discount_policy" ; "active"]

        let sql = """
            select ppl.id, ppl.name, ppl.sequence, ppl.discount_policy, ppl.active
            from product_pricelist as ppl
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductPriceList.exportId
                reader.text "name"
                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "discount_policy" |> orEmptyString
                reader.boolOrNone "active" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductCategory (modelName : string) =

        let header = [ "id/.id" ; "id" ; "name" ; "complete_name"; "parent_id/.id"
                       "parent_path" ; "removal_strategy_id/id" ; "packaging_reserve_method"
                       "allow_negative_stock" ; "property_cost_method"
                       "property_account_income_categ_id" ; "property_account_expense_categ_id"]

        let sql = """
            with
            rel_account_expense as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as category_id,
                       split_part(value_reference, ',', 2)::integer as account_id
                from ir_property
                where name = 'property_account_expense_categ_id'
                and res_id is not null
            ),
            rel_account_income as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as category_id,
                       split_part(value_reference, ',', 2)::integer as account_id
                from ir_property
                where name = 'property_account_income_categ_id'
                and res_id is not null
            ),
            rel_cost_method as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as category_id,
                       value_text as property_cost_method
                from ir_property
                where name = 'property_cost_method'
                and res_id is not null
            ),
            rel_product_removal as (
                select res_id as category_id, imd.module || '.' || imd.name as external_id
                from ir_model_data as imd
                join product_removal
                on imd.res_id = product_removal.id
                where imd.model = 'product.removal'
            ),
            rel_product_category as (
                select res_id as category_id, imd.module || '.' || imd.name as external_id
                from ir_model_data as imd
                join product_removal
                on imd.res_id = product_removal.id
                where imd.model = 'product.category'
            )
            select rpr.external_id as removal_external_id, rpc.external_id as category_external_id,
                   rcm.property_cost_method, aai.code as property_account_income_categ_id,
                   aae.code as property_account_expense_categ_id, pc.*
            from product_category as pc
            left join rel_product_removal as rpr on pc.removal_strategy_id = rpr.category_id
            left join rel_product_category as rpc on pc.id = rpc.category_id
            left join rel_cost_method as rcm on pc.id = rcm.category_id
            left join rel_account_income as rai on pc.id = rai.category_id
            left join rel_account_expense as rae on pc.id = rai.category_id
            left join account_account as aai on rai.account_id = aai.id
            left join account_account as aae on rae.account_id = aae.id
            order by pc.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> string

                match reader.textOrNone "category_external_id" with
                | Some category_external_id when category_external_id.StartsWith "product." -> category_external_id
                | _ -> reader.intOrNone "id" |> ProductCategory.exportId

                reader.text "name"
                reader.text "complete_name"
                reader.intOrNone "parent_id" |> orEmptyString
                reader.text "parent_path"
                reader.text "removal_external_id"
                reader.text "packaging_reserve_method"
                reader.boolOrNone "allow_negative_stock" |> orEmptyString
                reader.textOrNone "property_cost_method" |> orEmptyString
                reader.textOrNone "property_account_income_categ_id" |> orEmptyString
                reader.textOrNone "property_account_expense_categ_id" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductTemplate (modelName : string) =

        let header = [
            "id" ; "name" ; "default_code" ; "sequence" ; "detailed_type" ; "categ_id/id" ; "list_price"
            "sale_ok" ; "purchase_ok" ; "active" ; "sale_delay"
            "description" ; "description_picking" ;  "description_pickingin" ; "description_pickingout"
            "description_purchase" ; "description_sale"
            "purchase_line_warn" ; "purchase_line_warn_msg" ; "sale_line_warn" ; "sale_line_warn_msg" ;
            "tracking" ; "use_expiration_date" ; "expiration_time" ; "use_time" ; "removal_time" ; "alert_time"
            "responsible_id/id" ; "service_type" ; "expense_policy" ; "purchase_method"
            "invoice_policy" ; "allow_negative_stock"
            "property_account_income_id" ; "property_account_expense_id"
            "barcode" ; "volume" ; "weight"
        ]

        let sql = """
			with
			rel_product_product as (
				select product_tmpl_id, barcode, volume, weight
				from product_product
			),
            rel_product_responsible as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as product_template_id,
                       split_part(value_reference, ',', 2)::integer as responsible_id
                from ir_property
                where name = 'responsible_id'
                and res_id is not null
            ),
			rel_res_users as (
				select module, model, res_id, module || '.' || name as external_id
				from ir_model_data
				where model = 'res.users'
				and module not like '\_\_%'
			),
			rel_product_category as (
				select module, model, res_id, module || '.' || name as external_id
				from ir_model_data
				where model = 'product.category'
				and module not like '\_\_%'
            ),
            rel_account_income as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as product_template_id,
                       split_part(value_reference, ',', 2)::integer as account_id
                from ir_property
                where name = 'property_account_income_id'
                and res_id is not null
            ),
            rel_account_expense as (
                select id, company_id,
                       split_part(res_id, ',', 2)::integer as product_template_id,
                       split_part(value_reference, ',', 2)::integer as account_id
                from ir_property
                where name = 'property_account_expense_id'
                and res_id is not null
            )
            select pt.id, pt.name, pt.default_code, pt.sequence, pt.detailed_type,
			       pt.categ_id as categ_id_id, rpc.external_id as categ_id,
                   pt.list_price, pt.sale_ok, pt.purchase_ok, pt.active, pt.sale_delay,
                   pt.description, pt.description_picking, pt.description_pickingin,
                   pt.description_pickingout, pt.description_purchase, pt.description_sale,
                   pt.purchase_line_warn, pt.purchase_line_warn_msg, pt.sale_line_warn, pt.sale_line_warn_msg,
				   pt.tracking, pt.use_expiration_date, pt.expiration_time,
				   pt.use_time, pt.removal_time, pt.alert_time,
				   rpr.responsible_id, rru.external_id as responsible_external_id,
                   pt.service_type, pt.expense_policy, pt.purchase_method,
                   pt.invoice_policy, pt.allow_negative_stock,
                   aai.code as property_account_income_id, aae.code as property_account_expense_id,
				   rpp.barcode, rpp.volume, rpp.weight
            from product_template as pt
            left join rel_account_income as rai on pt.id = rai.product_template_id
            left join rel_account_expense as rae on pt.id = rae.product_template_id
            left join account_account as aai on rai.account_id = aai.id
            left join account_account as aae on rae.account_id = aae.id
			left join rel_product_category as rpc on pt.categ_id = rpc.res_id
			left join rel_product_responsible as rpr on pt.id = rpr.product_template_id
			left join rel_res_users as rru on rpr.responsible_id = rru.res_id
			left join rel_product_product as rpp on pt.id = rpp.product_tmpl_id
            order by pt.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                reader.text "name"
                reader.textOrNone "default_code" |> orEmptyString
                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "detailed_type" |> orEmptyString

                match reader.textOrNone "categ_id" with
                | Some categ_id -> categ_id
                | None -> reader.intOrNone "categ_id_id" |> ProductCategory.exportId

                (reader.double "list_price") |> formatDecimal

                reader.bool "sale_ok" |> string
                reader.bool "purchase_ok" |> string
                reader.bool "active" |> string
                (reader.double "sale_delay").ToString("#####")

                reader.textOrNone "description" |> orEmptyString
                reader.textOrNone "description_picking" |> orEmptyString
                reader.textOrNone "description_pickingin" |> orEmptyString
                reader.textOrNone "description_pickingout" |> orEmptyString
                reader.textOrNone "description_purchase" |> orEmptyString
                reader.textOrNone "description_sale" |> orEmptyString

                reader.textOrNone "purchase_line_warn" |> orEmptyString
                reader.textOrNone "purchase_line_warn_msg" |> orEmptyString
                reader.textOrNone "sale_line_warn" |> orEmptyString
                reader.textOrNone "sale_line_warn_msg" |> orEmptyString

                reader.text "tracking"
                reader.boolOrNone "use_expiration_date" |> orEmptyString
                reader.intOrNone "expiration_time" |> orEmptyString
                reader.intOrNone "use_time" |> orEmptyString
                reader.intOrNone "removal_time" |> orEmptyString
                reader.intOrNone "alert_time" |> orEmptyString

                match reader.textOrNone "responsible_external_id" with
                | Some responsible_external_id -> responsible_external_id
                | None -> reader.intOrNone "responsible_id" |> ResUsers.exportId

                reader.textOrNone "service_type" |> orEmptyString
                reader.textOrNone "expense_policy" |> orEmptyString
                reader.textOrNone "purchase_method" |> orEmptyString

                reader.textOrNone "invoice_policy" |> orEmptyString
                reader.boolOrNone "allow_negative_stock" |> orEmptyString

                reader.textOrNone "property_account_income_id" |> orEmptyString
                reader.textOrNone "property_account_expense_id" |> orEmptyString

                reader.textOrNone "barcode" |> orEmptyString
                reader.doubleOrNone "volume" |> orEmptyString
                reader.doubleOrNone "weight" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductTaxes (modelName : string) =

        let header = [ "id" ; "tax_id/id" ]

        let sql = """
            with
			rel_taxes as (
				select module, model, res_id as tax_id, module || '.' || name as external_id
				from ir_model_data
				where model = 'account.tax'
			)
            select pt.id, rt.external_id as tax_id
            from product_template as pt
            left join product_taxes_rel as ptr on pt.id = ptr.prod_id
			left join rel_taxes as rt on ptr.tax_id = rt.tax_id
            order by pt.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                match reader.textOrNone "tax_id" with
                | Some tax_id -> tax_id.Replace("l10n_es.", "account.")
                | None -> ""
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductSupplierTaxes (modelName : string) =

        let header = [ "id" ; "tax_id/id" ]

        let sql = """
            with
			rel_taxes as (
				select module, model, res_id as tax_id, module || '.' || name as external_id
				from ir_model_data
				where model = 'account.tax'
			)
            select pt.id, rt.external_id as tax_id
            from product_template as pt
            left join product_supplier_taxes_rel as pstr on pt.id = pstr.prod_id
			left join rel_taxes as rt on pstr.tax_id = rt.tax_id
            order by pt.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                match reader.textOrNone "tax_id" with
                | Some tax_id -> tax_id.Replace("l10n_es.", "account.")
                | None -> ""
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductSupplierInfo (modelName : string) =

        let header = [
            "id" ; "partner_id/id" ; "product_name" ; "product_code" ; "sequence" ; "min_qty" ; "price"
            "company_id/.id" ; "currency_id/.id" ; "date_start" ; "date_end" ; "product_tmpl_id/id" ; "delay"
        ]

        let sql = """
            select ps.*
            from product_supplierinfo ps
            left join res_partner as rp on ps.name = rp.id
            order by ps.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductSupplierInfo.exportId
                reader.intOrNone "name" |> ResPartner.exportId
                reader.textOrNone "product_name" |> orEmptyString
                reader.textOrNone "product_code" |> orEmptyString
                reader.intOrNone "sequence" |> orEmptyString
                (reader.double "min_qty").ToString("###0.00", CultureInfo.InvariantCulture)
                (reader.double "price").ToString("###0.00", CultureInfo.InvariantCulture)
                reader.intOrNone "company_id" |> orEmptyString
                reader.intOrNone "currency_id" |> orEmptyString
                reader.dateOnlyOrNone "date_start" |> dateOrEmptyString
                reader.dateOnlyOrNone "date_end" |> dateOrEmptyString
                reader.intOrNone "product_tmpl_id" |> ProductTemplate.exportId
                reader.int "delay" |> string
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductPriceListItem (modelName : string) =

        let header = [ "id" ; "applied_on" ; "product_tmpl_id/id" ; "categ_id/id" ; "product_id/id" ; "base"
                       "pricelist_id/id" ; "compute_price" ; "fixed_price" ; "percent_price"
                       "date_start" ; "date_end" ]

        let sql = """
            select ppi.id, ppi.product_tmpl_id, ppi.categ_id, product_id, ppi.applied_on, ppi.base, ppi.pricelist_id,
                   ppi.compute_price, ppi.fixed_price, ppi.percent_price, ppi.company_id, ppi.date_start, ppi.date_end
            from product_pricelist_item as ppi
            order by ppi.create_date
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductPriceListItem.exportId
                reader.text "applied_on"
                reader.intOrNone "product_tmpl_id" |> ProductTemplate.exportId
                reader.intOrNone "categ_id" |> ProductCategory.exportId
                reader.intOrNone "product_id" |> ProductProduct.exportId
                reader.text "base"

                reader.intOrNone "pricelist_id" |> ProductPriceList.exportId
                reader.text "compute_price"
                reader.doubleOrNone "fixed_price" |> formatDecimalOption
                reader.doubleOrNone "percent_price" |> formatDecimalOption
                reader.dateTimeOrNone "date_start" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "date_end" |> dateTimeOrEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountPaymentMethod (modelName : string) =

        let header = [ "id" ; "name" ; "code" ; "payment_type" ; "bank_account_required"
                       "payment_order_only" ; "mandate_required" ; "pain_version"
                       "convert_to_ascii" ]

        let sql = """
            with model_data as (
                select name, res_id as id, module
                from ir_model_data
                where model = 'account.payment.method'
            )
            select md.name as id, md.module, apm.name, apm.code, apm.payment_type, apm.bank_account_required,
                   apm.payment_order_only, apm.mandate_required, apm.pain_version,
                   apm.convert_to_ascii
            from account_payment_method as apm
            join model_data as md on apm.id = md.id
            --where apm.id <> 3
            """

        let readerFun (reader : RowReader) =
            [
                reader.text "module" + "." + reader.text "id"
                reader.textOrNone "name" |> orEmptyString
                reader.textOrNone "code" |> orEmptyString
                reader.textOrNone "payment_type" |> orEmptyString
                reader.boolOrNone "bank_account_required" |> orEmptyString

                reader.boolOrNone "payment_order_only" |> orEmptyString
                reader.boolOrNone "mandate_required" |> orEmptyString
                reader.textOrNone "pain_version" |> orEmptyString

                reader.boolOrNone "convert_to_ascii" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountPaymentMode (modelName : string) =

        let exportIdPrefix = AccountJournal.exportId <| Some ""

        let header =
            [
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

            select apm.id, apm.name, apm.bank_account_link, apm.fixed_journal_id,
                   apm.initiating_party_identifier, apm.initiating_party_issuer,
                   apm.initiating_party_scheme, apm.sepa_creditor_identifier,
                   (md.module || '.' || md.name) as payment_method_id, apm.payment_type,
                   apm.payment_method_code, apm.payment_order_ok,
                   apm.default_payment_mode, apm.default_invoice, apm.default_target_move,
                   apm.default_date_type, apm.default_date_prefered, apm.group_lines,
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
                reader.textOrNone "payment_method_id" |> orEmptyString

                reader.boolOrNone "payment_order_ok" |> orEmptyString
                reader.textOrNone "default_payment_mode" |> orEmptyString

                reader.boolOrNone "default_invoice" |> orEmptyString
                reader.textOrNone "default_target_move" |> orEmptyString
                reader.textOrNone "default_date_type" |> orEmptyString
                reader.textOrNone "default_date_prefered" |> orEmptyString

                reader.boolOrNone "group_lines" |> orEmptyString
                reader.textOrNone "default_journal_ids" |> orEmptyString
                reader.textOrNone "variable_journal_ids" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountOpeningMove (modelName : string) =

        //--------------------------------------------------------------------------------------------------------------
        let header =
            [
                "id" ; "date" ; "name" ; "partner_id" ; "ref" ; "journal_id" ; "line_ids/account_id"
                "line_ids/partner_id/id" ; "line_ids/name" ; "line_ids/debit" ; "line_ids/credit"
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
            and am.state = 'posted'
            and aml.partner_id in (select partner_id from active_partners)
            and aa.code in (select * from account_list)
            order by aa.code, aml.partner_id
            """

        let detailsWithBalanceReaderFun (reader : RowReader) =
            [
                ""
                reader.text "account_id"
                reader.intOrNone "partner_id" |> ResPartner.exportId
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
                    reader.intOrNone "partner_id" |> ResPartner.exportId

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
    static member exportDefaultValues (modelName : string) =

        let header = [ "id" ; "field_id/id" ; "condition" ; "json_value" ]

        let data =
            [
                [ Some 1 |> DefaultValue.exportId ; "account.field_account_move__journal_id" ; "" ; "0" ]
            ]

        header::data
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountMove (modelName : string) =

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

        Service.exportAccountMoveBase modelName

        [
            ("message_main_attachment_id", IrAttachment.exportId)
            ("payment_id", AccountPayment.exportId)
            ("payment_order_id", AccountPaymentOrder.exportId)
            ("reversed_entry_id", AccountMove.exportId)
            ("statement_line_id", AccountBankStatementLine.exportId)
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
            "amount_total_signed" ; "amount_untaxed" ; "amount_untaxed_signed" ; "auto_post" ; "campaign_id"
            "commercial_partner_id/id" ; "company_id/.id" ; "currency_id/.id" ; "date"
            "financial_type" ; "fiscal_position_id/.id" ; "invoice_date" ; "invoice_date_due"
            "invoice_origin" ; "invoice_partner_display_name" ; "invoice_payment_term_id/id"
            "invoice_source_email" ; "invoice_user_id/id" ; "is_move_sent" ; "journal_id/id"
            "move_type" ; "name" ; "narration" ; "not_in_mod347" ; "partner_bank_id/id"
            "partner_id/id" ; "partner_shipping_id/id" ; "payment_mode_id/id"
            "payment_reference" ; "payment_state" ; "posted_before" ; "qr_code_method"
            "ref" ; "reference_type" ; "secure_sequence_number" ; "sequence_number"
            "sequence_prefix" ; "source_id" ; "state" ; "stock_move_id/id"
            "tax_cash_basis_origin_move_id/id" ; "tax_cash_basis_rec_id/id" ; "team_id/.id"
            "thirdparty_invoice" ; "thirdparty_number" ; "to_check"
        ]

        let header = [
            "id" ; "access_token" ; "always_tax_exigible" ; "amount_residual" ; "amount_residual_signed"
            "amount_tax" ; "amount_tax_signed" ; "amount_total" ; "amount_total_in_currency_signed"
            "amount_total_signed" ; "amount_untaxed" ; "amount_untaxed_signed" ; "auto_post" ; "campaign_id"
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
                reader.intOrNone "commercial_partner_id" |> ResPartner.exportId
                "1"          // company_id
                "126"        // currency_id
                reader.dateOnlyOrNone "date" |> dateOrEmptyString
                reader.textOrNone "financial_type" |> orEmptyString
                reader.intOrNone "fiscal_position_id" |> orEmptyString
                reader.dateOnlyOrNone "invoice_date" |> dateOrEmptyString
                reader.dateOnlyOrNone "invoice_date_due" |> dateOrEmptyString
                reader.textOrNone "invoice_origin" |> orEmptyString
                reader.textOrNone "invoice_partner_display_name" |> orEmptyString
                reader.intOrNone "invoice_payment_term_id" |> AccountPaymentTerm.exportId
                reader.textOrNone "invoice_source_email" |> orEmptyString
                reader.intOrNone "invoice_user_id" |> ResUsers.exportId
                "false"      // is_move_sent
                reader.intOrNone "journal_id" |> AccountJournal.exportId
                // reader.text "move_type"
                reader.text "name"
                // reader.textOrNone "narration" |> orEmptyString
                // reader.bool "not_in_mod347" |> string
                // reader.intOrNone "partner_bank_id" |> ResPartnerBank.exportId
                reader.intOrNone "partner_id" |> ResPartner.exportId
                // reader.intOrNone "partner_shipping_id" |> ResPartner.exportId
                // reader.intOrNone "payment_mode_id" |> AccountPaymentMode.exportId
                // reader.textOrNone "payment_reference" |> orEmptyString
                // reader.textOrNone "payment_state" |> orEmptyString
                // reader.boolOrNone "posted_before" |> orEmptyString
                // reader.textOrNone "qr_code_method" |> orEmptyString
                // reader.textOrNone "ref" |> orEmptyString
                // reader.textOrNone "reference_type" |> orEmptyString
                // reader.intOrNone "secure_sequence_number" |> orEmptyString
                // reader.int "sequence_number" |> string
                // reader.text "sequence_prefix" |> string
                // reader.intOrNone "source_id" |> orEmptyString
                // "draft"                                 //  reader.text "state" |> string
                // reader.intOrNone "stock_move_id" |> orEmptyString
                // reader.intOrNone "tax_cash_basis_origin_move_id" |> orEmptyString
                // reader.intOrNone "tax_cash_basis_rec_id" |> orEmptyString
                // reader.int "team_id" |> string
                // reader.bool "thirdparty_invoice" |> string
                // reader.textOrNone "thirdparty_number" |> orEmptyString
                // reader.boolOrNone "to_check" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> List.take 50
        |> IExcelBroker.exportFile $"{modelName}_base.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportIrAttachment (modelName : string) =

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
                    reader.int "res_id" |> Some |> modelFunMap[resModel]
                    reader.text "name"
                    reader.text "store_fname"
                    reader.text "mimetype"
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResGroupsUsersRel (modelName : string) =

        let sql = $"""
            select login
            from res_users
            where res_users.company_id={ORIG_COMPANY_ID}
            and res_users.active = true
            """

        let userReaderFun (reader : RowReader) =
            [
                reader.text "login"
            ]

        let header = [ "login" ; "category_name" ; "group_name" ]

        let groupReaderFun (reader : RowReader) =
            [
                reader.text "login"
                reader.textOrNone "category_name" |> orEmptyString
                reader.text "group_name"
            ]

        for row in ISqlBroker.getExportData sql userReaderFun do
            let login = row[0]

            let sqlGroups = $"""
                select ru.login, imc.name as category_name, rg.name as group_name
                from res_groups as rg
                join res_groups_users_rel as rgu on rg.id = rgu.gid
                join res_users as ru on ru.id = rgu.uid
                left join ir_module_category as imc on rg.category_id = imc.id
                where ru.login = '{login}'
                order by group_name
            """

            header::ISqlBroker.getExportData sqlGroups groupReaderFun
            |> IExcelBroker.exportFile $"{modelName}_{login}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportIrSequence (modelName : string) =

        let getSequenceNumberNextActual (sequenceId : int) =
            // Esta función sólo es válida para secuencias de tipo standard.

            let sequenceName = $"ir_sequence_{sequenceId:D3}"

            let sql = $"""
                select
                    last_value,
                    (select increment_by
                     from pg_sequences
                     where sequencename = '{sequenceName}'),
                    is_called
                from {sequenceName}
            """

            let readerFun (reader : RowReader) =
                [
                    reader.int "last_value"
                    reader.int "increment_by"
                    if reader.bool "is_called" then 1 else 0
                ]

            let sequenceData = ISqlBroker.getExportData sql readerFun
            let lastValue = sequenceData[0][0]
            let incrementBy = sequenceData[0][1]
            let isCalled = sequenceData[0][2]

            if isCalled = 1 then
                lastValue + incrementBy
            else
                lastValue

        let header = [
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
                | "standard" -> getSequenceNumberNextActual (reader.int "id") |> string
                | _ -> ""

                reader.int "padding" |> string
                reader.textOrNone "prefix" |> orEmptyString
                reader.textOrNone "suffix" |> orEmptyString
                reader.boolOrNone "use_date_range" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportIrSequenceDateRange (modelName : string) =

        let header = [ "id" ; "date_from" ; "date_to" ; "sequence_id/id" ; "number_next" ]

        let sql = """
            with
			rel_sequence as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'ir.sequence'
                and module not like '\_\_%'
			)
            select rs.external_id as sequence_external_id, irsdr.*
            from ir_sequence_date_range as irsdr
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
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportDeysankaResConfigSettings (modelName : string) =

        //--------------------------------------------------------------------------------------------------------------
        let getValueAsString (readerFun :RowReader -> string) (record_id : int) (fieldName : string) : string =
            let sql = $"""
                select {fieldName}
                from account_journal
                where id = {record_id}
            """

            ISqlBroker.getExportData sql readerFun
            |> List.head
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let getStringValue (record_id : int) (fieldName : string) : string =

            let readerFun (reader : RowReader) =
                reader.textOrNone fieldName |> orEmptyString

            getValueAsString readerFun record_id fieldName
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let getIntValueAsString (record_id : int) (fieldName : string) : string =

            let readerFun (reader : RowReader) =
                reader.intOrNone fieldName |> orEmptyString

            getValueAsString readerFun record_id fieldName
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let getIdFromData (data : string list list) (settingName : string) : int =
            data
            |> List.find (fun line -> line[0] = settingName)
            |> fun line -> split "_" line[1] |> Array.last |> int
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let resConfigSettingsMap =                   // Nombre de ir.config.parameter --> nombre en res.config.settings
            Map [
                "deysanka_account.bad_reconcile_exclude_accounts", "bad_reconcile_exclude_accounts"
                "deysanka_account.bank_batch_charges_prefix", "bank_batch_charges_prefix"
                "deysanka_account.bank_batch_credit_prefix", "bank_batch_credit_prefix"
                "deysanka_account.bank_charges_client_journal_id", "bank_charges_client_journal_id"
                "deysanka_account.bank_charges_client_payment_mode_id", "bank_charges_client_payment_mode_id"
                "deysanka_account.bank_charges_client_payment_term_id", "bank_charges_client_payment_term_id"
                "deysanka_account.bank_charges_client_product_id", "bank_charges_client_product_id"
                "deysanka_account.bank_charges_partner_id", "bank_charges_partner_id"
                "deysanka_account.bank_charges_product_id", "bank_charges_product_id"
                "deysanka_account.bank_charges_ret_journal_id", "bank_charges_ret_journal_id"
                "deysanka_account.cash_statement_dey_cash_journal_id", "cash_statement_dey_cash_journal_id"
                "deysanka_account.cash_statement_eb_cash_journal_id", "cash_statement_eb_cash_journal_id"
                "deysanka_account.closing_journal_id", "closing_journal_id"
                "deysanka_account.deysanka_checks_proxy_url", "deysanka_checks_proxy_url"
                "deysanka_account.monthly_sales_cash_journal_id", "monthly_sales_cash_journal_id"
                "deysanka_account.monthly_sales_journal_id", "monthly_sales_journal_id"
                "deysanka_account.monthly_sales_partner_id", "monthly_sales_partner_id"
                "deysanka_account.monthly_sales_payment_mode_id", "monthly_sales_payment_mode_id"
                "deysanka_account.monthly_sales_payment_term_id", "monthly_sales_payment_term_id"
                "deysanka_account.monthly_sales_product_ptc_id", "monthly_sales_product_ptc_id"
                "deysanka_account.monthly_sales_product_pte_id", "monthly_sales_product_pte_id"
                "deysanka_account.monthly_sales_product_ptt_id", "monthly_sales_product_ptt_id"
                "deysanka_account.monthly_sales_tpv_code", "monthly_sales_tpv_code"
                "deysanka_account.partner_deysanka_id", "partner_deysanka_id"
                "deysanka_account.tag_name_mensual", "tag_name_mensual"
                "deysanka_account.unpaid_inv_account_id", "unpaid_inv_account_id"
                "deysanka_account.unpaid_inv_payment_mode_by_customer", "unpaid_inv_payment_mode_by_customer"
                "deysanka_account.unpaid_inv_ref_prefix", "unpaid_inv_ref_prefix"
                "deysanka_account.web_sales_partner_id", "web_sales_partner_id"
                "deysanka_account.web_sales_tpv_code", "web_sales_tpv_code"
            ]
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let exportFunMap =
            Map [
                "bank_charges_client_journal_id", AccountJournal.exportId
                "bank_charges_client_payment_mode_id", AccountPaymentMode.exportId
                "bank_charges_client_payment_term_id", AccountPaymentTerm.exportId
                "bank_charges_client_product_id", ProductTemplate.exportId
                "bank_charges_partner_id", ResPartner.exportId
                "bank_charges_product_id", ProductTemplate.exportId
                "bank_charges_ret_journal_id", AccountJournal.exportId
                "cash_statement_dey_cash_journal_id", AccountJournal.exportId
                "cash_statement_eb_cash_journal_id", AccountJournal.exportId
                "closing_journal_id", AccountJournal.exportId
                "monthly_sales_cash_journal_id", AccountJournal.exportId
                "monthly_sales_journal_id", AccountJournal.exportId
                "monthly_sales_partner_id", ResPartner.exportId
                "monthly_sales_payment_mode_id", AccountPaymentMode.exportId
                "monthly_sales_payment_term_id", AccountPaymentTerm.exportId
                "monthly_sales_product_ptc_id", ProductTemplate.exportId
                "monthly_sales_product_pte_id", ProductTemplate.exportId
                "monthly_sales_product_ptt_id", ProductTemplate.exportId
                "partner_deysanka_id", ResPartner.exportId
                "unpaid_inv_account_id", AccountAccount.exportId
                "unpaid_inv_payment_mode_by_customer", AccountPaymentMode.exportId
                "web_sales_partner_id", ResPartner.exportId
            ]
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let getJournalConfigData (deyCashJournalId : int) (ebCashJournalId : int) =
            [
                [ "cash_statement_dey_bank_journal_id"
                  getIntValueAsString deyCashJournalId "bank_journal_id" |> Some |> AccountJournal.exportId ]
                [ "cash_statement_dey_cash_deposit_label"
                  getStringValue deyCashJournalId "bank_cash_move_label" ]
                [ "cash_statement_dey_sales_payment_mode_id"
                  getIntValueAsString deyCashJournalId "sales_payment_mode_id" |> Some |> AccountPaymentMode.exportId ]
                [ "cash_statement_dey_buys_payment_mode_id"
                  getIntValueAsString deyCashJournalId "buys_payment_mode_id" |> Some |> AccountPaymentMode.exportId ]
                [ "cash_statement_eb_bank_journal_id"
                  getIntValueAsString ebCashJournalId "bank_journal_id" |> Some |> AccountJournal.exportId ]
                [ "cash_statement_eb_cash_deposit_label"
                  getStringValue ebCashJournalId "bank_cash_move_label" ]
                [ "cash_statement_eb_sales_payment_mode_id"
                  getIntValueAsString ebCashJournalId "sales_payment_mode_id" |> Some |> AccountPaymentMode.exportId ]
                [ "cash_statement_eb_buys_payment_mode_id"
                  getIntValueAsString ebCashJournalId "buys_payment_mode_id" |> Some |> AccountPaymentMode.exportId ]
            ]
        //--------------------------------------------------------------------------------------------------------------

        let header = [ "name" ; "value" ]

        let sql = """
            select *
            from ir_config_parameter
            where key ilike 'deysanka_account%'
            order by key
        """

        let readerFun (reader : RowReader) =
            [
                let key = resConfigSettingsMap[reader.text "key"]
                key

                if exportFunMap.ContainsKey key
                then reader.text "value" |> Some |> exportFunMap[key]
                else reader.text "value"
            ]

        let configData = header::ISqlBroker.getExportData sql readerFun

        let deyCashJournalId = getIdFromData configData "cash_statement_dey_cash_journal_id"
        let ebCashJournalId = getIdFromData configData "cash_statement_eb_cash_journal_id"

        let journalConfigData = getJournalConfigData deyCashJournalId ebCashJournalId

        configData @ journalConfigData
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportStockWarehouse (modelName : string) =

        let header = [
            "id" ; "name" ; "active" ; "company_id/.id" ; "partner_id/id" ; "view_location_id/id"
            "lot_stock_id/id" ; "code" ; "reception_steps" ; "delivery_steps"
            "wh_input_stock_loc_id/id" ; "wh_qc_stock_loc_id/id" ; "wh_output_stock_loc_id/id"
            "wh_pack_stock_loc_id/id" ; "pick_type_id/id" ; "pack_type_id/id"
            "out_type_id/id" ; "in_type_id/id" ; "int_type_id/id" ; "return_type_id/id"
            "sequence" ; "buy_to_resupply"
        ]

        let sql = """
			with
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
			),
			rel_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
			)
			select
				view_loc.external_id as view_loc_external_id,
				lot_stock.external_id as lot_stock_external_id,
				input_stock_loc.external_id as input_stock_loc_external_id,
				qc_stock_loc.external_id as qc_stock_loc_external_id,
				output_stock_loc.external_id as output_stock_loc_external_id,
				pack_stock_loc.external_id as pack_stock_loc_external_id,
				pick_type.external_id as pick_type_external_id,
				pack_type.external_id as pack_type_external_id,
				out_type.external_id as out_type_external_id,
				in_type.external_id as in_type_external_id,
				int_type.external_id as int_type_external_id,
				return_type.external_id as return_type_external_id,
				sw.*
            from stock_warehouse as sw
			left join rel_location as view_loc on sw.view_location_id = view_loc.id
			left join rel_location as lot_stock on sw.lot_stock_id = lot_stock.id
			left join rel_location as input_stock_loc on sw.wh_input_stock_loc_id = input_stock_loc.id
			left join rel_location as qc_stock_loc on sw.wh_qc_stock_loc_id = qc_stock_loc.id
			left join rel_location as output_stock_loc on sw.wh_output_stock_loc_id = output_stock_loc.id
			left join rel_location as pack_stock_loc on sw.wh_pack_stock_loc_id = pack_stock_loc.id
			left join rel_picking_type as pick_type on sw.pick_type_id = pick_type.id
			left join rel_picking_type as pack_type on sw.pack_type_id = pack_type.id
			left join rel_picking_type as out_type on sw.out_type_id = out_type.id
			left join rel_picking_type as in_type on sw.in_type_id = in_type.id
			left join rel_picking_type as int_type on sw.int_type_id = int_type.id
			left join rel_picking_type as return_type on sw.return_type_id = return_type.id
            order by id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockWarehouse.exportId
                reader.text "name"
                reader.bool "active" |> string
                reader.int "company_id" |> string
                reader.intOrNone "partner_id" |> ResPartner.exportId

                match reader.textOrNone "view_loc_external_id" with
                | Some view_loc_external_id -> view_loc_external_id
                | None -> reader.intOrNone "view_location_id" |> StockLocation.exportId

                match reader.textOrNone "lot_stock_external_id" with
                | Some lot_stock_external_id -> lot_stock_external_id
                | None -> reader.intOrNone "lot_stock_id" |> StockLocation.exportId

                reader.textOrNone "code" |> orEmptyString
                reader.text "reception_steps"
                reader.text "delivery_steps"

                match reader.textOrNone "input_stock_loc_external_id" with
                | Some input_stock_loc_external_id -> input_stock_loc_external_id
                | None -> reader.intOrNone "wh_input_stock_loc_id" |> StockLocation.exportId

                match reader.textOrNone "qc_stock_loc_external_id" with
                | Some qc_stock_loc_external_id -> qc_stock_loc_external_id
                | None -> reader.intOrNone "wh_qc_stock_loc_id" |> StockLocation.exportId

                match reader.textOrNone "output_stock_loc_external_id" with
                | Some output_stock_loc_external_id -> output_stock_loc_external_id
                | None -> reader.intOrNone "wh_output_stock_loc_id" |> StockLocation.exportId

                match reader.textOrNone "pack_stock_loc_external_id" with
                | Some pack_stock_loc_external_id -> pack_stock_loc_external_id
                | None -> reader.intOrNone "wh_pack_stock_loc_id" |> StockLocation.exportId

                match reader.textOrNone "pick_type_external_id" with
                | Some pick_type_external_id -> pick_type_external_id
                | None -> reader.intOrNone "pick_type_id" |> StockPickingType.exportId

                match reader.textOrNone "pack_type_external_id" with
                | Some pack_type_external_id -> pack_type_external_id
                | None -> reader.intOrNone "pack_type_id" |> StockPickingType.exportId

                match reader.textOrNone "out_type_external_id" with
                | Some out_type_external_id -> out_type_external_id
                | None -> reader.intOrNone "out_type_id" |> StockPickingType.exportId

                match reader.textOrNone "in_type_external_id" with
                | Some in_type_external_id -> in_type_external_id
                | None ->reader.intOrNone "in_type_id" |> StockPickingType.exportId

                match reader.textOrNone "int_type_external_id" with
                | Some int_type_external_id -> int_type_external_id
                | None -> reader.intOrNone "int_type_id" |> StockPickingType.exportId

                match reader.textOrNone "return_type_external_id" with
                | Some return_type_external_id -> return_type_external_id
                | None -> reader.intOrNone "return_type_id" |> StockPickingType.exportId

                reader.int "sequence" |> string
                reader.bool "buy_to_resupply" |> string
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportStockLocation (modelName : string) =

        let header = [
            "id/.id" ; "id" ; "name" ; "complete_name" ; "active" ; "usage" ; "location_id/.id" ;
            "comment" ; "posx" ; "posy" ; "posz" ; "parent_path" ; "company_id/.id" ;
            "scrap_location" ; "return_location" ; "removal_strategy_id/id" ; "barcode" ;
            "cyclic_inventory_frequency" ; "last_inventory_date" ; "next_inventory_date" ;
            "storage_category_id" ; "valuation_in_account_id" ; "valuation_out_account_id" ;
            "allow_negative_stock"
        ]

        let sql = """
            with
            rel_removal_strategy as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'product.removal'
                and module not like '\_\_%'
            ),
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            )
            select rl.module as module, rl.external_id as location_external_id,
                   rrs.external_id as removal_external_id,
                   sl.*
            from stock_location as sl
            left join rel_location as rl on sl.id = rl.id
            left join rel_removal_strategy as rrs on sl.id = rrs.id
            order by id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> string

                match reader.textOrNone "location_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "id" |> Some |> StockLocation.exportId

                reader.text "name"
                reader.text "complete_name"
                reader.bool "active" |> string
                reader.text "usage"
                reader.intOrNone "location_id" |> orEmptyString
                reader.textOrNone "comment" |> orEmptyString
                reader.int "posx" |> string
                reader.int "posy" |> string
                reader.int "posz" |> string
                reader.text "parent_path"
                reader.intOrNone "company_id" |> orEmptyString
                reader.boolOrNone "scrap_location" |> orEmptyString
                reader.boolOrNone "return_location" |> orEmptyString
                reader.textOrNone "removal_external_id" |> orEmptyString
                reader.textOrNone "barcode" |> orEmptyString
                reader.intOrNone "cyclic_inventory_frequency" |> orEmptyString
                reader.dateOnlyOrNone "last_inventory_date" |> dateOrEmptyString
                reader.dateOnlyOrNone "next_inventory_date" |> dateOrEmptyString
                reader.intOrNone "storage_category_id" |> orEmptyString
                reader.intOrNone "valuation_in_account_id" |> AccountAccount.exportId
                reader.intOrNone "valuation_out_account_id" |> AccountAccount.exportId
                reader.boolOrNone "allow_negative_stock" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportStockPickingType (modelName : string) =

        let header = [
            "id" ; "name" ; "color" ; "sequence" ; "sequence_id/id" ; "sequence_code" ; "default_location_src_id/id"
            "default_location_dest_id/id" ; "code" ; "return_picking_type_id/id" ; "show_entire_packs"
            "warehouse_id/id" ; "active" ; "use_create_lots" ; "use_existing_lots" ; "print_label"
            "show_operations" ; "show_reserved" ; "reservation_method" ; "reservation_days_before"
            "reservation_days_before_priority" ; "barcode" ; "company_id"
        ]

        let sql = """
            with
			rel_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
			),
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            )
            select rls.external_id as def_loc_src_ext_id, rld.external_id as def_loc_dest_ext_id,
                   rpt.external_id as external_id,
                   spt.*
            from stock_picking_type as spt
            left join rel_location as rls on spt.default_location_src_id = rls.id
            left join rel_location as rld on spt.default_location_dest_id = rld.id
            left join rel_picking_type as rpt on spt.id = rpt.id
            order by spt.id
            """

        let readerFun (reader : RowReader) =
            [
                match reader.textOrNone "external_id" with
                | Some externalId -> externalId
                | None -> reader.int "id" |> Some |> StockPickingType.exportId

                reader.text "name"
                reader.int "color" |> string
                reader.int "sequence" |> string
                reader.int "sequence_id" |> Some |> IrSequence.exportId
                reader.text "sequence_code"

                match reader.textOrNone "def_loc_src_ext_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "default_location_src_id" |> StockLocation.exportId

                match reader.textOrNone "def_loc_dest_ext_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "default_location_dest_id" |> StockLocation.exportId

                reader.textOrNone "code" |> orEmptyString
                reader.intOrNone "return_picking_type_id" |> StockPickingType.exportId
                reader.boolOrNone "show_entire_packs" |> orEmptyString
                reader.int "warehouse_id" |> Some |> StockWarehouse.exportId
                reader.bool "active" |> string
                reader.bool "use_create_lots" |> string
                reader.bool "use_existing_lots" |> string
                reader.boolOrNone "print_label" |> orEmptyString
                reader.bool "show_operations" |> string
                reader.bool "show_reserved" |> string
                reader.text "reservation_method"
                reader.intOrNone "reservation_days_before" |> orEmptyString
                reader.intOrNone "reservation_days_before_priority" |> orEmptyString
                reader.textOrNone "barcode" |> orEmptyString
                reader.int "company_id" |> string
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProcurementGroup (modelName : string) =

        let header = [ "id" ; "partner_id/id" ; "name" ; "move_type" ; "sale_id/id" ]

        let sql = """
            select *
            from procurement_group
            order by create_date
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> ProcurementGroup.exportId
                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.text "name"
                reader.text "move_type"
                reader.intOrNone "sale_id" |> SaleOrder.exportId
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportStockPicking (modelName : string) =

        let header = [
            "id" ; "message_main_attachment_id/id" ; "name" ; "origin" ; "note" ; "backorder_id/id" ;
            "move_type" ; "state" ; "group_id/id" ; "priority" ; "scheduled_date" ; "date_deadline" ;
            "has_deadline_issue" ; "date" ; "date_done" ; "location_id/id" ; "location_dest_id/id" ;
            "picking_type_id/id" ; "partner_id/id" ; "company_id/.id" ; "user_id/id" ; "owner_id/id" ; "printed" ;
            "is_locked" ; "immediate_transfer" ; "sale_id"
        ]

        let sql = """
            with
			rel_stock_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            ),
			rel_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
            )
            select rsl.external_id as location_external_id,
                   rsld.external_id as location_dest_external_id,
                   rpt.external_id as picking_type_external_id,
                   sp.*
            from stock_picking as sp
            left join rel_stock_location as rsl on sp.location_id = rsl.id
            left join rel_stock_location as rsld on sp.location_dest_id = rsld.id
            left join rel_picking_type as rpt on sp.picking_type_id = rpt.id
            order by sp.create_date
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockPicking.exportId
                reader.intOrNone "message_main_attachment_id" |> IrAttachment.exportId
                reader.textOrNone "name" |> orEmptyString
                reader.textOrNone "origin" |> orEmptyString
                reader.textOrNone "note" |> orEmptyString
                reader.intOrNone "backorder_id" |> StockPicking.exportId
                reader.text "move_type"
                reader.textOrNone "state" |> orEmptyString
                reader.intOrNone "group_id" |> ProcurementGroup.exportId
                reader.textOrNone "priority" |> orEmptyString
                reader.dateTimeOrNone "scheduled_date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "date_deadline" |> dateTimeOrEmptyString
                reader.boolOrNone "has_deadline_issue" |> orEmptyString
                reader.dateTimeOrNone "date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "date_done" |> dateTimeOrEmptyString

                match reader.textOrNone "location_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "location_id" |> Some |> StockLocation.exportId

                match reader.textOrNone "location_dest_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "location_dest_id" |> Some |> StockLocation.exportId

                match reader.textOrNone "picking_type_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "picking_type_id" |> Some |> StockPickingType.exportId

                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.intOrNone "user_id" |> ResUsers.exportId
                reader.intOrNone "owner_id" |> ResPartner.exportId
                reader.boolOrNone "printed" |> orEmptyString
                reader.boolOrNone "is_locked" |> orEmptyString
                reader.boolOrNone "immediate_transfer" |> orEmptyString
                reader.intOrNone "sale_id" |> SaleOrder.exportId
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
