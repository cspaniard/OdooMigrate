namespace Services.Exporting.Odoo

open Model
open Model.Constants
open Services.Exporting.Odoo.ExportHelpers

type ExportRes () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportBank (modelName : string) =

        let header = addStampHeadersTo [ "id" ; "name" ; "bic" ; "country/id" ]

        let sql = """
            select *
            from res_bank
            where active=true
            order by create_date
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> Bank.exportId
                reader.text "name"
                reader.textOrNone "bic" |> orEmptyString
                "base.es"      // country/id
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPartnerBank (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "bank_id/id" ; "acc_number"; "sequence" ; "partner_id/id" ; "acc_holder_name" ; "description"
        ]

        let sql = $"""
            select rpb.*
            from res_partner_bank as rpb
            join res_partner as rp on rpb.partner_id = rp.id
            where rpb.company_id={ORIG_COMPANY_ID}
            and rp.active = true
            order by create_date
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
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportUsers (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "login"; "name" ; "notification_type" ; "team_id/.id"
            "working_year" ; "lowest_working_date" ; "action_id/id"
        ]

        let sql = """
            with
			rel_action_action as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'ir.actions.act_window'
                and module not like '\_\_%'
			)
            select res_users.*, res_partner.name as name,
                   raa.external_id as action_external_id
            from res_users
            join res_partner on res_users.partner_id = res_partner.id
            left join rel_action_action as raa on res_users.action_id = raa.id
            where res_users.active = true
            and res_users.company_id=""" + ORIG_COMPANY_ID + " order by res_users.id"

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> ResUsers.exportId
                reader.text "login"
                reader.text "name"
                reader.text "notification_type"
                "1"
                // "sales_team.team_sales_department"     // sale_team_id
                reader.textOrNone "working_year" |> orEmptyString
                reader.dateOnlyOrNone "lowest_working_date" |> dateOrEmptyString
                reader.textOrNone "action_external_id" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPartner (modelName : string) =

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

        let header = addStampHeadersTo [
            "id" ; "property_account_payable_code" ; "property_account_position_external_id/id"
            "property_account_receivable_code" ; "property_payment_term_id/id"
            "property_product_pricelist/id" ; "property_purchase_currency_id/.id"
            "property_stock_customer/id" ; "property_stock_supplier/id"
            "property_supplier_payment_term_id/id" ; "customer_payment_mode_id/id"
            "supplier_payment_mode_id/id" ; "name" ; "company_id/.id" ; "display_name"
            "date" ; "title" ; "parent_id/id" ; "ref" ; "lang" ; "tz" ; "user_id/id" ; "vat"
            "website" ; "comment" ; "credit_limit" ; "active" ; "employee" ; "function"
            "type" ; "street" ; "street2" ; "zip" ; "city" ; "state_id/id" ; "country_id/id"
            "partner_latitude" ; "partner_longitude" ; "email" ; "phone" ; "mobile"
            "is_company" ; "industry_id/id" ; "color" ; "partner_share"
            "commercial_partner_id/id" ; "commercial_company_name" ; "company_name"
            "email_normalized" ; "message_bounce" ; "signup_token" ; "signup_type"
            "signup_expiration" ; "team_id/.id" ; "phone_sanitized" ; "debit_limit"
            "last_time_entries_checked" ; "invoice_warn" ; "invoice_warn_msg" ; "supplier_rank"
            "customer_rank" ; "picking_warn" ; "picking_warn_msg" ; "sale_warn"
            "sale_warn_msg" ; "purchase_warn" ; "purchase_warn_msg"
            "aeat_anonymous_cash_customer" ; "aeat_identification_type"
            "aeat_identification" ; "comercial" ; "not_in_mod347"
            "aeat_partner_check_result" ; "aeat_partner_vat" ; "aeat_partner_name"
            "aeat_data_diff" ; "aeat_last_checked" ; "aeat_partner_type" ; "city_id/id" ; "zip_id/id"
            "sale_journal/id" ; "purchase_journal/id" ; "customer" ; "supplier" ; "bank_name"
            "alternative_name"
        ]

        let sql = """
            with
            rel_property as (
                select
                    name as property_name,
                    split_part(res_id, ',', 2)::integer as property_partner_id,
                    split_part(value_reference, ',', 1)::varchar as property_model_name,
                    split_part(value_reference, ',', 2)::integer as property_value_id
                from ir_property
                where res_id like 'res.partner,%'
            )
            select
                papay.property_value_id as property_account_payable_id,
                aapay.code as property_account_payable_code,
                papos.property_value_id as property_account_position_id,
                'account.' || afpextid.name as property_account_position_external_id,
                parec.property_value_id as property_account_receivable_id,
                aarec.code as property_account_receivable_code,
                ppterm.property_value_id as property_payment_term_id,
                ppplist.property_value_id as property_product_pricelist,
                ppcurr.property_value_id as property_purchase_currency_id,
                pscust.property_value_id as property_stock_customer,
                pssupp.property_value_id as property_stock_supplier,
                pspayterm.property_value_id as property_supplier_payment_term_id,
                cpmode.property_value_id as customer_payment_mode_id,
                spmode.property_value_id as supplier_payment_mode_id,
                rcstate.module || '.' || rcstate.name as state_external_id,
                rcstate.module || '.' || rcountry.name as country_external_id,
                rcstate.module || '.' || rpindustry.name as industry_external_id,
                rcstate.module || '.' || rcity.name as city_external_id,
                rcstate.module || '.' || rczip.name as zip_external_id,
                rp.*
            from res_partner as rp
            left join rel_property as papay
                on rp.id = papay.property_partner_id
                and papay.property_name = 'property_account_payable_id'
            left join rel_property as papos
                on rp.id = papos.property_partner_id
                and papos.property_name = 'property_account_position_id'
            left join rel_property as parec
                on rp.id = parec.property_partner_id
                and parec.property_name = 'property_account_receivable_id'
            left join rel_property as ppterm
                on rp.id = ppterm.property_partner_id
                and ppterm.property_name = 'property_payment_term_id'
            left join rel_property as ppplist
                on rp.id = ppplist.property_partner_id
                and ppplist.property_name = 'property_product_pricelist'
            left join rel_property as ppcurr
                on rp.id = ppcurr.property_partner_id
                and ppcurr.property_name = 'property_purchase_currency_id'
            left join rel_property as pscust
                on rp.id = pscust.property_partner_id
                and pscust.property_name = 'property_stock_customer'
            left join rel_property as pssupp
                on rp.id = pssupp.property_partner_id
                and pssupp.property_name = 'property_stock_supplier'
            left join rel_property as pspayterm
                on rp.id = pspayterm.property_partner_id
                and pspayterm.property_name = 'property_supplier_payment_term_id'
            left join rel_property as cpmode
                on rp.id = cpmode.property_partner_id
                and cpmode.property_name = 'customer_payment_mode_id'
            left join rel_property as spmode
                on rp.id = spmode.property_partner_id
                and spmode.property_name = 'supplier_payment_mode_id'
            left join ir_model_data afpextid
                ON afpextid.model = 'account.fiscal.position'
                and afpextid.res_id = papos.property_value_id
            left join account_account as aapay on papay.property_value_id = aapay.id
            left join account_account as aarec on parec.property_value_id = aarec.id
            left join ir_model_data rcstate on rcstate.model = 'res.country.state'
                and rp.state_id = rcstate.res_id
            left join ir_model_data rcountry on rcountry.model = 'res.country'
                and rp.country_id = rcountry.res_id
            left join ir_model_data rpindustry on rpindustry.model = 'res.partner.industry'
                and rp.country_id = rpindustry.res_id
            left join ir_model_data rcity on rcity.model = 'res.city'
                and rp.country_id = rcity.res_id
            left join ir_model_data rczip on rczip.model = 'res.city.zip'
                and rp.country_id = rczip.res_id
            where rp.customer is not null
            and rp.active = true
            or rp.name ilike 'Deysanka SL'
            order by rp.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ResPartner.exportId
                reader.textOrNone "property_account_payable_code" |> orEmptyString
                reader.textOrNone "property_account_position_external_id" |> orEmptyString
                reader.textOrNone "property_account_receivable_code" |> orEmptyString
                reader.intOrNone "property_payment_term_id" |> AccountPaymentTerm.exportId
                reader.intOrNone "property_product_pricelist" |> ProductPriceList.exportId
                reader.intOrNone "property_purchase_currency_id" |> orEmptyString
                reader.intOrNone "property_stock_customer" |> StockLocation.exportId
                reader.intOrNone "property_stock_supplier" |> StockLocation.exportId
                reader.intOrNone "property_supplier_payment_term_id" |> AccountPaymentTerm.exportId
                reader.intOrNone "customer_payment_mode_id" |> AccountPaymentMode.exportId
                reader.intOrNone "supplier_payment_mode_id" |> AccountPaymentMode.exportId
                reader.text "name" |> Some |> orEmptyString
                reader.intOrNone "company_id" |> orEmptyString
                reader.textOrNone "display_name" |> orEmptyString
                reader.dateOnlyOrNone "date" |> orEmptyString
                reader.intOrNone "title" |> orEmptyString
                reader.intOrNone "parent_id" |> ResPartner.exportId
                reader.textOrNone "ref" |> orEmptyString
                reader.textOrNone "lang" |> orEmptyString
                reader.textOrNone "tz" |> orEmptyString
                reader.intOrNone "user_id" |> ResUsers.exportId

                reader.textOrNone "vat" |> orEmptyString
                reader.textOrNone "website" |> orEmptyString
                reader.textOrNone "comment" |> orEmptyString
                reader.doubleOrNone "credit_limit" |> formatDecimalOption
                reader.bool "active" |> string
                reader.boolOrNone "employee" |> orEmptyString
                reader.textOrNone "function" |> orEmptyString
                reader.textOrNone "type" |> orEmptyString
                reader.textOrNone "street" |> orEmptyString
                reader.textOrNone "street2" |> orEmptyString
                reader.textOrNone "zip" |> orEmptyString
                reader.textOrNone "city" |> orEmptyString
                reader.textOrNone "state_external_id" |> orEmptyString
                reader.textOrNone "country_external_id" |> orEmptyString
                reader.doubleOrNone "partner_latitude" |> formatDecimalOption
                reader.doubleOrNone "partner_longitude" |> formatDecimalOption
                reader.textOrNone "email" |> orEmptyString
                reader.textOrNone "phone" |> orEmptyString
                reader.textOrNone "mobile" |> orEmptyString
                reader.bool "is_company" |> string
                reader.textOrNone "industry_external_id" |> orEmptyString
                reader.intOrNone "color" |> orEmptyString
                reader.bool "partner_share" |> string
                reader.intOrNone "commercial_partner_id" |> ResPartner.exportId
                reader.textOrNone "commercial_company_name" |> orEmptyString
                reader.textOrNone "company_name" |> orEmptyString
                reader.textOrNone "email_normalized" |> orEmptyString
                reader.intOrNone "message_bounce" |> orEmptyString
                reader.textOrNone "signup_token" |> orEmptyString
                reader.textOrNone "signup_type" |> orEmptyString
                reader.dateTimeOrNone "signup_expiration" |> dateTimeOrEmptyString
                reader.intOrNone "team_id" |> orEmptyString
                reader.textOrNone "phone_sanitized" |> orEmptyString
                reader.doubleOrNone "debit_limit" |> formatDecimalOption
                reader.dateTimeOrNone "last_time_entries_checked" |> dateTimeOrEmptyString
                reader.textOrNone "invoice_warn" |> orEmptyString
                reader.textOrNone "invoice_warn_msg" |> orEmptyString
                reader.intOrNone "supplier_rank" |> orEmptyString
                reader.intOrNone "customer_rank" |> orEmptyString
                reader.textOrNone "picking_warn" |> orEmptyString
                reader.textOrNone "picking_warn_msg" |> orEmptyString
                reader.textOrNone "sale_warn" |> orEmptyString
                reader.textOrNone "sale_warn_msg" |> orEmptyString
                reader.textOrNone "purchase_warn" |> orEmptyString
                reader.textOrNone "purchase_warn_msg" |> orEmptyString
                reader.boolOrNone "aeat_anonymous_cash_customer" |> orEmptyString
                reader.textOrNone "aeat_identification_type" |> orEmptyString
                reader.textOrNone "aeat_identification" |> orEmptyString
                reader.textOrNone "comercial" |> orEmptyString
                reader.boolOrNone "not_in_mod347" |> orEmptyString
                reader.textOrNone "aeat_partner_check_result" |> orEmptyString
                reader.textOrNone "aeat_partner_vat" |> orEmptyString
                reader.textOrNone "aeat_partner_name" |> orEmptyString
                reader.boolOrNone "aeat_data_diff" |> orEmptyString
                reader.dateTimeOrNone "aeat_last_checked" |> dateTimeOrEmptyString
                reader.textOrNone "aeat_partner_type" |> orEmptyString
                reader.textOrNone "city_external_id" |> orEmptyString
                reader.textOrNone "zip_external_id" |> orEmptyString
                reader.intOrNone "sale_journal" |> AccountJournal.exportId
                reader.intOrNone "purchase_journal" |> AccountJournal.exportId
                reader.boolOrNone "customer" |> orEmptyString
                reader.boolOrNone "supplier" |> orEmptyString
                reader.textOrNone "bank_name" |> orEmptyString
                reader.textOrNone "alternative_name" |> orEmptyString
                yield! readStampFields reader
            ]
        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportGroupsUsersRel (modelName : string) =

        // Estos ficheros generados tienen importador propio y no deben tener stamp fields.

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
