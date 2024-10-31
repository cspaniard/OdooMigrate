namespace Services.Exporting.Odoo

open System
open System.Globalization
open System.Runtime.Intrinsics.X86
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

        let header = [ "id" ; "bank_id/id" ; "acc_number"; "sequence"
                       "partner_id/id" ; "acc_holder_name" ; "description" ]

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

        let header = [ "id" ; "name" ; "note" ; "sequence"
                       "line_ids/value" ; "line_ids/value_amount" ; "line_ids/nb_days"
                       "line_ids/days_next_month" ; "line_ids/delay_type" ]

        let sql = $"""
            select id, name, note, sequence
            from account_payment_term
            """

        let termReaderFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountPaymentTerm.exportId
                reader.text "name"
                $"""<p>{reader.textOrNone "note" |> Option.defaultValue (reader.text "name")}</p>"""
                reader.int "sequence" |> string
            ]

        let sqlForLines = $"""
            select id, value, value_amount, days, day_of_the_month,
                   option, payment_id, sequence
            from {modelName}_line
            """

        let delayTypeMap = Map.ofList [
            "day_after_invoice_date", "days_after"
            "day_following_month", "days_end_of_month_on_the"
        ]

        let termLineReaderFun (reader : RowReader) =
            [
                reader.intOrNone "payment_id" |> AccountPaymentTerm.exportId

                let value = reader.text "value"
                if value = "balance" then "percent" else value

                reader.doubleOrNone "value_amount" |> Option.defaultValue 0.0 |> string
                reader.int "days" |> string

                let dayOfTheMonth = reader.intOrNone "day_of_the_month" |> Option.defaultValue 0 |> string
                dayOfTheMonth

                if dayOfTheMonth = "0"
                then delayTypeMap[reader.text "option"]
                else "days_end_of_month_on_the"
            ]

        let terms = ISqlBroker.getExportData sql termReaderFun
        let termLines =

            let updatePercentInRow (percentValue : string) (row : string list) =
                row
                |> List.mapi (fun i colVal -> if i = 2 then percentValue else colVal)

            let updatePercentInGroup = function
                | [singleRow] ->
                    [updatePercentInRow "100" singleRow]
                | firstRow :: secondRow :: _ ->
                    let value = decimal firstRow[2]
                    [
                        firstRow
                        secondRow |> updatePercentInRow (string (100m - value))
                    ]
                | [] -> []


            ISqlBroker.getExportData sqlForLines termLineReaderFun
            |> List.groupBy List.head
            |> List.collect (snd >> updatePercentInGroup)

        let joinData = [
            for term in terms -> (term, termLines |> List.filter (fun termLine -> termLine[0] = term[0]))
        ]

        header::(flattenData joinData)
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResUsers (modelName : string) =

        let header = [ "id" ; "login"; "name" ; "notification_type" ; "team_id/.id"
                       "working_year" ; "lowest_working_date" ]

        let sql = $"""
            select res_users.id, login, name, notification_type, working_year, lowest_working_date
            from res_users
            join res_partner on res_users.partner_id = res_partner.id
            where res_users.company_id={ORIG_COMPANY_ID}
            and res_users.active = true
            """

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
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResPartner (modelName : string) =

        let header = [ "id" ; "name" ; "lang" ; "tz" ; "user_id/id" ; "parent_id/id"
                       "vat" ; "website" ; "comment" ; "type" ; "street" ; "street2" ; "zip" ; "city"
                       "state_id/id" ; "country_id" ; "email" ; "phone" ; "mobile" ; "is_company" ; "partner_share"
                       "customer" ; "supplier"
                       "commercial_partner_id" ; "commercial_company_name" ; "not_in_mod347"
                       "sale_journal_id/id" ; "purchase_journal_id/id" ; "aeat_anonymous_cash_customer"
                       "aeat_partner_vat" ; "aeat_partner_name" ; "aeat_data_diff"
                       "property_account_receivable_id" ; "property_account_payable_id"
                       "property_payment_term_id/id" ; "customer_payment_mode_id/id" ; "supplier_payment_mode_id/id"
                       "property_product_pricelist/id" ; "posicion fiscal" ]

        let sql = $"""
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
                   rp.partner_share, rp.commercial_partner_id, rp.commercial_company_name, rp.not_in_mod347,
                   rp.sale_journal, rp.purchase_journal, rp.aeat_anonymous_cash_customer,
                   rp.aeat_partner_vat, rp.aeat_partner_name, rp.aeat_data_diff,
                   rp.customer, rp.supplier,
                   acc_rec.code as property_account_receivable_id, acc.code as property_account_payable_id,
                   apt.id as account_payment_term_id, rcpm.payment_mode_id as customer_payment_mode_id,
                   rspm.payment_mode_id as supplier_payment_mode_id,
                   rppl.product_pricelist as property_product_pricelist,
                   afp.name as property_account_position
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
                reader.bool "partner_share" |> string

                reader.bool "customer" |> string
                reader.bool "supplier" |> string

                reader.int "commercial_partner_id" |> string
                reader.textOrNone "commercial_company_name" |> orEmptyString
                reader.boolOrNone "not_in_mod347" |> orEmptyString

                reader.intOrNone "sale_journal" |> AccountJournal.exportId
                reader.intOrNone "purchase_journal" |> AccountJournal.exportId
                reader.boolOrNone "aeat_anonymous_cash_customer" |> orEmptyString

                reader.textOrNone "aeat_partner_vat" |> orEmptyString
                reader.textOrNone "aeat_partner_name" |> orEmptyString
                reader.boolOrNone "aeat_data_diff" |> orEmptyString

                reader.textOrNone "property_account_receivable_id" |> Option.defaultValue "430000"
                reader.textOrNone "property_account_payable_id" |> orEmptyString

                reader.intOrNone "account_payment_term_id" |> AccountPaymentTerm.exportId
                reader.intOrNone "customer_payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "supplier_payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "property_product_pricelist" |> ProductPriceList.exportId
                reader.stringOrNone "property_account_position" |> orEmptyString
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
                reader.intOrNone "id" |> ResPartnerBank.exportId
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
    static member exportAccountJournal (modelName : string) =

        let header = [ "id" ; "name" ; "code"; "type" ; "sequence"
                       "n43_date_type" ; "default_account_id" ; "refund_sequence" ]

        let sql = """
            select aj.id, aj.name, aj.code, aj.type, aj.sequence, n43_date_type,
                   aa.code as account_id, refund_sequence
            from account_journal as aj
            left join account_account as aa on aj.default_account_id = aa.id
            where aj.code <> 'STJ'
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountJournal.exportId
                reader.text "name"
                reader.text "code"
                reader.text "type"
                reader.int "sequence" |> string
                reader.text "n43_date_type"
                reader.textOrNone "account_id" |> orEmptyString
                reader.boolOrNone "refund_sequence" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountBankingMandate (modelName : string) =

        let header = [ "id" ; "format" ; "type"; "partner_bank_id/id" ; "signature_date"
                       "state" ; "recurrent_sequence_type" ; "scheme" ]

        let sql = $"""
            select abm.id, abm.format, abm.type, abm.partner_bank_id, abm.partner_id, abm.signature_date,
                   abm.last_debit_date, abm.state, abm.recurrent_sequence_type, abm.scheme
            from account_banking_mandate as abm
            join res_partner as rp on abm.partner_id = rp.id
            where abm.company_id={ORIG_COMPANY_ID}
            and abm.state = 'valid'
            and rp.active = true
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountBankingMandate.exportId
                reader.text "format"
                reader.text "type"
                reader.intOrNone "partner_bank_id" |> ResPartnerBank.exportId
                reader.dateOnlyOrNone "signature_date" |> dateOrEmptyString
                reader.textOrNone "state" |> orEmptyString
                reader.textOrNone "recurrent_sequence_type" |> orEmptyString
                reader.textOrNone "scheme" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductPriceList (modelName : string) =

        let header = [ "id" ; "name" ; "sequence"; "discount_policy" ]

        let sql = """
            select ppl.id, ppl.name, ppl.sequence, ppl.discount_policy
            from product_pricelist as ppl
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductPriceList.exportId
                reader.text "name"
                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "discount_policy" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductCategoryTranslation (modelName : string) =

        [
            [ "id" ; "name" ]
            [ "product.product_category_all" ; "Todos" ]
            [ "product.cat_expense" ; "Gastos" ]
            [ "product.product_category_1" ; "Vendible" ]
        ]
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductCategory (modelName : string) =

        let header = [ "id" ; "parent_path" ; "name" ; "complete_name"; "parent_id/.id" ; "allow_negative_stock" ]

        let sql = """
            select pg.id, pg.parent_path, pg.name, pg.complete_name, pg.parent_id, pg.allow_negative_stock
            from product_category as pg
            where pg.id > 3
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductCategory.exportId
                reader.text "parent_path"
                reader.text "name"
                reader.text "complete_name"
                reader.intOrNone "parent_id" |> orEmptyString
                reader.boolOrNone "allow_negative_stock" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductTemplate (modelName : string) =

        let header = [ "id" ; "name" ; "default_code" ; "sequence" ; "type" ; "categ_id" ; "list_price"
                       "sale_ok" ; "purchase_ok" ; "active" ; "sale_delay" ; "tracking"
                       "service_type" ; "sale_line_warn" ; "expense_policy" ; "purchase_method"
                       "invoice_policy" ; "purchase_line_warn_msg" ; "allow_negative_stock"
                       "property_account_income_id" ; "property_account_expense_id" ]

        let sql = $"""
            with
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
            select pt.id, pt.name, pt.default_code, pt.sequence, pt.type, pc.complete_name as categ_id,
                   pt.list_price, pt.sale_ok, pt.purchase_ok, pt.active, pt.sale_delay, pt.tracking,
                   pt.service_type, pt.sale_line_warn, pt.expense_policy, pt.purchase_method,
                   pt.invoice_policy, pt.purchase_line_warn_msg, pt.allow_negative_stock,
                   aai.code as property_account_income_id, aae.code as property_account_expense_id
            from product_template as pt
            left join product_category as pc on pt.categ_id = pc.id
            left join rel_account_income as rai on pt.id = rai.product_template_id
            left join rel_account_expense as rae on pt.id = rae.product_template_id
            left join account_account as aai on rai.account_id = aai.id
            left join account_account as aae on rae.account_id = aae.id
            where pt.company_id = {ORIG_COMPANY_ID}
            and pt.active = true
            order by pt.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                reader.text "name"
                reader.textOrNone "default_code" |> orEmptyString
                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "type" |> orEmptyString
                reader.textOrNone "categ_id" |> orEmptyString
                (reader.double "list_price") |> formatDecimal

                reader.bool "sale_ok" |> string
                reader.bool "purchase_ok" |> string
                reader.bool "active" |> string
                (reader.double "sale_delay").ToString("#####")
                reader.text "tracking"

                reader.textOrNone "service_type" |> orEmptyString
                reader.textOrNone "sale_line_warn" |> orEmptyString
                reader.textOrNone "expense_policy" |> orEmptyString
                reader.textOrNone "purchase_method" |> orEmptyString

                reader.textOrNone "invoice_policy" |> orEmptyString
                reader.textOrNone "purchase_line_warn_msg" |> orEmptyString
                reader.boolOrNone "allow_negative_stock" |> orEmptyString

                reader.textOrNone "property_account_income_id" |> orEmptyString
                reader.textOrNone "property_account_expense_id" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductTaxes (modelName : string) =

        let header = [ "id" ; "taxes_id" ]

        let sql = $"""
            select pt.id,
            case when at.name = 'IVA Exento Repercutido'
                then 'IVA Exento Repercutido Sujeto'
            else at.name
            end taxes_id

            from product_template as pt
            left join product_taxes_rel as ptr on pt.id = ptr.prod_id
            left join account_tax as at on ptr.tax_id = at.id
            where pt.company_id = {ORIG_COMPANY_ID}
            and pt.active = true
            order by pt.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                reader.textOrNone "taxes_id" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductSupplierTaxes (modelName : string) =

        let header = [ "id" ; "supplier_taxes_id" ]

        let sql = $"""
            select pt.id, at.name as taxes_id
            from product_template as pt
            left join product_supplier_taxes_rel as pstr on pt.id = pstr.prod_id
            left join account_tax as at on pstr.tax_id = at.id
            where pt.company_id = {ORIG_COMPANY_ID}
            and pt.active = true
            order by pt.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                reader.textOrNone "taxes_id" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductSupplierInfo (modelName : string) =

        let header = [ "id" ; "name/id" ; "price" ; "date_start" ; "date_end" ; "product_tmpl_id/id" ]

        let sql = $"""
            select psi.id, psi.name, psi.price, date_start, date_end, psi.product_tmpl_id
            from product_supplierinfo as psi
            join product_template as pt on psi.product_tmpl_id = pt.id
            where psi.company_id = {ORIG_COMPANY_ID}
            and pt.active = true
            order by psi.product_tmpl_id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductSupplierInfo.exportId
                reader.intOrNone "name" |> ResPartner.exportId
                (reader.double "price").ToString("###0.00", CultureInfo.InvariantCulture)
                reader.dateOnlyOrNone "date_start" |> dateOrEmptyString
                reader.dateOnlyOrNone "date_end" |> dateOrEmptyString
                reader.intOrNone "product_tmpl_id" |> ProductTemplate.exportId
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductPriceListItem (modelName : string) =

        let header = [ "id" ; "product_tmpl_id/id" ; "applied_on" ; "base" ; "pricelist_id/id"
                       "compute_price" ; "fixed_price" ; "percent_price" ]

        let sql = """
            select ppi.id, ppi.product_tmpl_id, ppi.applied_on, ppi.base, ppi.pricelist_id,
                   ppi.compute_price, ppi.fixed_price, ppi.percent_price, ppi.company_id
            from product_pricelist_item as ppi
            where ppi.product_tmpl_id is null
            union
            select ppi.id, ppi.product_tmpl_id, ppi.applied_on, ppi.base, ppi.pricelist_id,
                   ppi.compute_price, ppi.fixed_price, ppi.percent_price, ppi.company_id
            from product_pricelist_item as ppi
            left join product_template as pt on ppi.product_tmpl_id = pt.id
            where pt.active = true
            order by 2
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductPriceListItem.exportId
                match reader.intOrNone "product_tmpl_id" with
                | Some _ as idOption -> idOption |> ProductTemplate.exportId
                | None -> ""
                reader.text "applied_on"
                reader.text "base"
                match reader.intOrNone "pricelist_id" with
                | Some _ as idOption -> idOption |> ProductPriceList.exportId
                | None -> ""
                reader.text "compute_price"
                match reader.doubleOrNone "fixed_price" with
                | Some price -> price.ToString("###0.00", CultureInfo.InvariantCulture)
                | None -> ""
                match reader.doubleOrNone "percent_price" with
                | Some price -> price.ToString("###0.00", CultureInfo.InvariantCulture)
                | None -> ""
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
                "id" ; "name" ; "bank_account_link" ; "fixed_journal_id/id" ; "payment_method_id/id"
                "payment_order_ok" ; "default_payment_mode"
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
