namespace Services.Exporting.Odoo

open System.Globalization
open Model
open Services.Exporting.Odoo.ExportHelpers

type ExportProduct () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportPriceList (modelName : string) =

        let header = addStampHeadersTo [ "id" ; "name" ; "sequence"; "discount_policy" ; "active"]

        let sql = """
            select ppl.*
            from product_pricelist as ppl
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductPriceList.exportId
                reader.text "name"
                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "discount_policy" |> orEmptyString
                reader.boolOrNone "active" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportCategory (modelName : string) =

        let header = addStampHeadersTo [
            "id/.id" ; "id" ; "name" ; "complete_name"; "parent_id/.id"
            "parent_path" ; "removal_strategy_id/id" ; "packaging_reserve_method"
            "allow_negative_stock" ; "property_cost_method"
            "property_account_income_categ_id/id" ; "property_account_expense_categ_id/id"
        ]

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
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportTemplate (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "default_code" ; "sequence" ; "detailed_type" ; "categ_id/id" ; "list_price"
            "sale_ok" ; "purchase_ok" ; "active" ; "sale_delay"
            "description" ; "description_picking" ;  "description_pickingin" ; "description_pickingout"
            "description_purchase" ; "description_sale"
            "purchase_line_warn" ; "purchase_line_warn_msg" ; "sale_line_warn" ; "sale_line_warn_msg" ;
            "tracking" ; "use_expiration_date" ; "expiration_time" ; "use_time" ; "removal_time" ; "alert_time"
            "responsible_id/id" ; "service_type" ; "expense_policy" ; "purchase_method"
            "invoice_policy" ; "allow_negative_stock"
            "property_account_income_id/id" ; "property_account_expense_id/id"
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
            select pt.*,
			       rpc.external_id as categ_external_id,
				   rpr.responsible_id, rru.external_id as responsible_external_id,
                   aai.code as property_account_income_id, aae.code as property_account_expense_id,
				   rpp.barcode, rpp.volume as rpp_volume, rpp.weight as rpp_weight
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

                match reader.textOrNone "categ_external_id" with
                | Some categ_external_id -> categ_external_id
                | None -> reader.intOrNone "categ_id" |> ProductCategory.exportId

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
                reader.doubleOrNone "rpp_volume" |> orEmptyString
                reader.doubleOrNone "rpp_weight" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportTaxes (modelName : string) =

        let header = addStampHeadersTo [ "id" ; "tax_id/id" ]

        let sql = """
            with
			rel_taxes as (
				select module, model, res_id as tax_id, module || '.' || name as external_id
				from ir_model_data
				where model = 'account.tax'
			)
            select pt.*,
                   rt.external_id as tax_external_id
            from product_template as pt
            left join product_taxes_rel as ptr on pt.id = ptr.prod_id
			left join rel_taxes as rt on ptr.tax_id = rt.tax_id
            order by pt.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId

                match reader.textOrNone "tax_external_id" with
                | Some tax_external_id -> tax_external_id.Replace("l10n_es.", "account.")
                | None -> ""

                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportSupplierTaxes (modelName : string) =

        let header = addStampHeadersTo [ "id" ; "tax_id/id" ]

        let sql = """
            with
			rel_taxes as (
				select module, model, res_id as tax_id, module || '.' || name as external_id
				from ir_model_data
				where model = 'account.tax'
			)
            select pt.*,
                   rt.external_id as tax_external_id
            from product_template as pt
            left join product_supplier_taxes_rel as pstr on pt.id = pstr.prod_id
			left join rel_taxes as rt on pstr.tax_id = rt.tax_id
            order by pt.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductTemplate.exportId

                match reader.textOrNone "tax_external_id" with
                | Some tax_external_id -> tax_external_id.Replace("l10n_es.", "account.")
                | None -> ""

                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportSupplierInfo (modelName : string) =

        let header = addStampHeadersTo [
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
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPriceListItem (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "applied_on" ; "product_tmpl_id/id" ; "categ_id/id" ; "product_id/id" ; "base"
            "pricelist_id/id" ; "compute_price" ; "fixed_price" ; "percent_price"
            "date_start" ; "date_end"
        ]

        let sql = """
            select ppi.*
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
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProduct (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "message_main_attachment_id/id" ; "default_code" ; "active" ; "product_tmpl_id/id"
            "barcode" ; "combination_indices" ; "volume" ; "weight" ; "can_image_variant_1024_be_zoomed"
        ]

        let sql = """
            select pp.*
            from product_product as pp
            order by pp.create_date
            """

        let readerFun (reader : RowReader) =
            [
                reader.intOrNone "id" |> ProductProduct.exportId
                ""                                                                 // message_main_attachment_id
                reader.textOrNone "default_code" |> orEmptyString
                reader.boolOrNone "active" |> orEmptyString
                reader.intOrNone "product_tmpl_id" |> ProductTemplate.exportId
                reader.textOrNone "barcode" |> orEmptyString
                reader.textOrNone "combination_indices" |> orEmptyString
                reader.doubleOrNone "volume" |> formatDecimalOption
                reader.doubleOrNone "weight" |> formatDecimalOption
                reader.boolOrNone "can_image_variant_1024_be_zoomed" |> orEmptyString

                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
