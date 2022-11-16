namespace Services.Exporting.Odoo

open System
open System.Globalization
open Model
open Model.Constants

type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker

type Service () =

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

        let sql = """select id, name, bic
                     from res_bank
                     where active=true"""

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

        let sql = $"""select rpb.id, rpb.acc_number, rpb.sequence, rpb.partner_id, rpb.bank_id,
                             rpb.acc_holder_name, rpb.description
                      from res_partner_bank as rpb
                      join res_partner as rp on rpb.partner_id = rp.id
                      where rpb.company_id={ORIG_COMPANY_ID}
                      and rp.active = true"""

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
                       "line_ids/value" ; "line_ids/value_amount" ; "line_ids/days"
                       "line_ids/day_of_the_month" ; "line_ids/option" ; "line_ids/sequence" ]

        let sql = $"""select id, name, note, sequence
                      from account_payment_term
                      where company_id={ORIG_COMPANY_ID}"""

        let termReaderFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountPaymentTerm.exportId
                reader.text "name"
                $"""<p>{reader.textOrNone "note" |> Option.defaultValue (reader.text "name")}</p>"""
                reader.int "sequence" |> string
            ]

        let sqlForLines = $"""select id, value, value_amount, days, day_of_the_month,
                                     option, payment_id, sequence
                              from {modelName}_line"""

        let termLineReaderFun (reader : RowReader) =
            [
                reader.intOrNone "payment_id" |> AccountPaymentTerm.exportId
                reader.text "value"
                reader.doubleOrNone "value_amount" |> Option.defaultValue 0.0 |> string
                reader.int "days" |> string
                reader.intOrNone "day_of_the_month" |> Option.defaultValue 0 |> string
                reader.text "option"
                reader.int "sequence" |> string
            ]

        let terms = ISqlBroker.getExportData sql termReaderFun
        let termLines = ISqlBroker.getExportData sqlForLines termLineReaderFun

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

        let sql = $"""select res_users.id, login, name, notification_type, working_year, lowest_working_date
                      from res_users
                      join res_partner on res_users.partner_id = res_partner.id
                      where res_users.company_id={ORIG_COMPANY_ID}
                      and res_users.active = true"""

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
                       "commercial_partner_id" ; "commercial_company_name" ; "not_in_mod347"
                       "sale_journal_id/id" ; "purchase_journal_id/id" ; "aeat_anonymous_cash_customer"
                       "aeat_partner_vat" ; "aeat_partner_name" ; "aeat_data_diff"
                       "property_account_receivable_id" ; "property_account_payable_id"
                       "property_payment_term_id/id" ]

        let sql = $"""with
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
                      )
                      select rp.id, rp.name, rp.lang, rp.tz, rp.user_id, rp.parent_id,
                             rp.vat, rp.website, rp.comment, rp.type, rp.street, rp.street2, rp.zip, rp.city,
                             rcs.code as state_id, rp.country_id, rp.email, rp.phone, rp.mobile, rp.is_company,
                             rp.partner_share, rp.commercial_partner_id, rp.commercial_company_name, rp.not_in_mod347,
                             rp.sale_journal, rp.purchase_journal, rp.aeat_anonymous_cash_customer,
                             rp.aeat_partner_vat, rp.aeat_partner_name, rp.aeat_data_diff,
                             acc_rec.code as property_account_receivable_id, acc.code as property_account_payable_id,
                             apt.id as account_payment_term_id
                      from res_partner as rp
                      left join rel_payable as pay on rp.id = pay.partner_id
                      left join rel_receivable as rec on rp.id = rec.partner_id
                      left join account_account as acc on pay.account_id = acc.id
                      left join account_account as acc_rec on rec.account_id = acc_rec.id
                      left join res_country_state as rcs on rp.state_id = rcs.id
                      left join rel_payment_term as rp_term on rp.id = rp_term.partner_id
                      left join account_payment_term as apt on rp_term.payment_term_id = apt.id
                      where rp.company_id={ORIG_COMPANY_ID}
                      and rp.active = true
                      or rp.name ilike 'Deysanka SL'
                      order by rp.id"""

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

                (reader.intOrNone "account_payment_term_id" |> AccountPaymentTerm.exportId)
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountAccount (modelName : string) =

        let header = [ "id" ; "code" ; "name"; "user_type_id/id"
                       "reconcile" ; "last_visible_year" ]

        let sql = $"""with model_data as (
                          select name, res_id as id
                          from ir_model_data
                          where model = 'account.account.type'
                      )
                      select aa.id, aa.code, aa.name, md.name as user_type_id, aa.reconcile, aa.last_visible_year
                      from account_account as aa
                      join account_account_type as aat on aa.user_type_id = aat.id
                      join model_data as md on aa.user_type_id = md.id
                      where company_id={ORIG_COMPANY_ID}
                      and aa.create_date > '2019-09-05'
                      and not (aa.code like '41%%' or aa.code like '43%%')
                      order by aa.code"""

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ResPartnerBank.exportId
                reader.text "code"
                reader.text "name"
                "account." + reader.text "user_type_id"
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

        let sql = $"""select aj.id, aj.name, aj.code, aj.type, aj.sequence, n43_date_type,
                             case when aj.type in ('purchase', 'sale', 'bank', 'cash') then aa.code end account_id,
                             case when aj.type in ('purchase', 'sale', 'bank', 'cash') then true else false end refund_sequence
                      from account_journal as aj
                      left join account_account as aa on aj.default_credit_account_id = aa.id
                      where aj.company_id={ORIG_COMPANY_ID}
                      and aj.code <> 'STJ'"""

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

        let sql = $"""select abm.id, abm.format, abm.type, abm.partner_bank_id, abm.partner_id, abm.signature_date,
                             abm.last_debit_date, abm.state, abm.recurrent_sequence_type, abm.scheme
                      from account_banking_mandate as abm
                      join res_partner as rp on abm.partner_id = rp.id
                      where abm.company_id={ORIG_COMPANY_ID}
                      and abm.state = 'valid'
                      and rp.active = true"""

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountBankingMandate.exportId
                reader.text "format"
                reader.text "type"
                "__import__." + (reader.intOrNone "partner_bank_id" |> ResPartnerBank.exportId)
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

        let sql = $"""select ppl.id, ppl.name, ppl.sequence, ppl.discount_policy
                      from product_pricelist as ppl"""

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> AccountProductPriceList.exportId
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

        let sql = $"""select pg.id, pg.parent_path, pg.name, pg.complete_name, pg.parent_id, pg.allow_negative_stock
                      from product_category as pg
                      where pg.id > 3"""

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

        let sql = $"""with
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
                      join product_category as pc on pt.categ_id = pc.id
                      join rel_account_income as rai on pt.id = rai.product_template_id
                      join rel_account_expense as rae on pt.id = rae.product_template_id
                      join account_account as aai on rai.account_id = aai.id
                      join account_account as aae on rae.account_id = aae.id
                      where pt.company_id = {ORIG_COMPANY_ID}
                      and pt.active = true
                      order by pt.id"""

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                reader.text "name"
                reader.textOrNone "default_code" |> orEmptyString
                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "type" |> orEmptyString
                reader.textOrNone "categ_id" |> orEmptyString
                (reader.double "list_price").ToString("#####.00", CultureInfo.InvariantCulture)

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

        let sql = $"""select pt.id,
                      case when at.name = 'IVA Exento Repercutido'
                          then 'IVA Exento Repercutido Sujeto'
                      else at.name
                      end taxes_id

                      from product_template as pt
                      left join product_taxes_rel as ptr on pt.id = ptr.prod_id
                      left join account_tax as at on ptr.tax_id = at.id
                      where pt.company_id = {ORIG_COMPANY_ID}
                      and pt.active = true
                      order by pt.id"""

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                reader.textOrNone "taxes_id" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductSupplierTaxes(modelName : string) =

        let header = [ "id" ; "supplier_taxes_id" ]

        let sql = $"""select pt.id, at.name as taxes_id
                      from product_template as pt
                      left join product_supplier_taxes_rel as pstr on pt.id = pstr.prod_id
                      left join account_tax as at on pstr.tax_id = at.id
                      where pt.company_id = {ORIG_COMPANY_ID}
                      and pt.active = true
                      order by pt.id"""

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId
                reader.textOrNone "taxes_id" |> orEmptyString
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductSupplierInfo(modelName : string) =

        let header = [ "id" ; "name/id" ; "price" ; "date_start" ; "date_end" ; "product_tmpl_id/id" ]

        let sql = $"""select psi.id, psi.name, psi.price, date_start, date_end, psi.product_tmpl_id
                      from product_supplierinfo as psi
                      join product_template as pt on psi.product_tmpl_id = pt.id
                      where psi.company_id = {ORIG_COMPANY_ID}
                      and pt.active = true
                      order by psi.product_tmpl_id"""

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductSupplierInfo.exportId
                reader.intOrNone "name" |> ResPartner.exportId
                (reader.double "price").ToString("####.00", CultureInfo.InvariantCulture)
                reader.dateOnlyOrNone "date_start" |> dateOrEmptyString
                reader.dateOnlyOrNone "date_end" |> dateOrEmptyString
                reader.intOrNone "product_tmpl_id" |> ProductTemplate.exportId
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
