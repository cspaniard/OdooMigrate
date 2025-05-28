namespace Services.Exporting.Odoo

open Model
open Services.Exporting.Odoo.ExportHelpers

type ExportSale () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportOrder (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "access_token" ; "name" ; "origin" ; "client_order_ref" ; "reference"
            "state" ; "date_order" ; "validity_date" ; "require_signature" ; "require_payment"
            "user_id/id" ; "partner_id/id" ; "partner_invoice_id/id" ; "partner_shipping_id/id"
            "pricelist_id/id" ; "invoice_status" ; "note"
            "amount_untaxed" ; "amount_tax" ; "amount_total" ; "payment_term_id/id"
            "fiscal_position_id/id" ; "company_id/.id" ; "team_id/.id" ; "signed_by" ; "signed_on"
            "commitment_date" ; "show_update_pricelist" ; "sale_order_template_id/id" ; "incoterm/id"
            "picking_policy" ; "warehouse_id/id" ; "procurement_group_id/id" ; "effective_date"
            "payment_mode_id/id" ; "active" ; "project_id/id"
        ]

        let sql = """
            with
			rel_account_incoterms as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'account.incoterms'
            ),
			rel_fiscal_position as (
                select module, model, res_id as id, 'account.' || name as external_id
                from ir_model_data
                where model = 'account.fiscal.position'
            )
            select rfp.external_id as fiscal_position_external_id,
                   rai.external_id as incoterm_external_id,
                   so.*
            from sale_order as so
            left join rel_fiscal_position as rfp on so.fiscal_position_id = rfp.id
            left join rel_account_incoterms as rai on so.incoterm = rai.id
            order by so.create_date
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> SaleOrder.exportId
                reader.textOrNone "access_token" |> orEmptyString
                reader.text "name"
                reader.textOrNone "origin" |> orEmptyString
                reader.textOrNone "client_order_ref" |> orEmptyString
                reader.textOrNone "reference" |> orEmptyString
                reader.textOrNone "state" |> orEmptyString
                reader.dateTime "date_order" |> Some |> dateTimeOrEmptyString
                reader.dateOnlyOrNone "validity_date" |> dateOrEmptyString
                reader.boolOrNone "require_signature" |> orEmptyString
                reader.boolOrNone "require_payment" |> orEmptyString
                reader.intOrNone "user_id" |> ResUsers.exportId
                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.intOrNone "partner_invoice_id" |> ResPartner.exportId
                reader.intOrNone "partner_shipping_id" |> ResPartner.exportId
                reader.intOrNone "pricelist_id" |> ProductPriceList.exportId
                reader.textOrNone "invoice_status" |> orEmptyString
                reader.textOrNone "note" |> orEmptyString
                reader.decimalOrNone "amount_untaxed" |> orEmptyString
                reader.decimalOrNone "amount_tax" |> orEmptyString
                reader.decimalOrNone "amount_total" |> orEmptyString
                reader.intOrNone "payment_term_id" |> AccountPaymentTerm.exportId
                reader.textOrNone "fiscal_position_external_id" |> orEmptyString
                reader.int "company_id" |> string
                reader.intOrNone "team_id" |> orEmptyString
                reader.textOrNone "signed_by" |> orEmptyString
                reader.dateTimeOrNone "signed_on" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "commitment_date" |> dateTimeOrEmptyString
                reader.boolOrNone "show_update_pricelist" |> orEmptyString
                reader.intOrNone "sale_order_template_id" |> orEmptyString
                reader.textOrNone "incoterm_external_id" |> orEmptyString
                reader.text "picking_policy"
                reader.int "warehouse_id" |> Some |> StockWarehouse.exportId
                reader.intOrNone "procurement_group_id" |> ProcurementGroup.exportId
                reader.dateTimeOrNone "effective_date" |> dateTimeOrEmptyString
                reader.intOrNone "payment_mode_id" |> AccountPaymentMode.exportId
                reader.boolOrNone "active" |> orEmptyString
                reader.intOrNone "project_id" |> ProjectProject.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportOrderLine (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "order_id/id" ; "name" ; "sequence" ; "invoice_status" ; "price_unit" ; "price_subtotal" ;
            "price_tax" ; "price_total" ; "price_reduce" ; "price_reduce_taxinc" ; "price_reduce_taxexcl" ;
            "discount" ; "product_template_id/id" ; "product_uom_qty" ; "product_uom/.id"
            "qty_delivered_method" ; "qty_delivered" ; "qty_delivered_manual" ; "qty_to_invoice" ; "qty_invoiced" ;
            "untaxed_amount_invoiced" ; "untaxed_amount_to_invoice" ; "salesman_id/id"
            "company_id/.id" ; "order_partner_id/id" ; "is_expense" ; "is_downpayment" ; "state" ;
            "customer_lead" ; "display_type" ; "is_service"
        ]

        let sql = """
            select pt.id as product_template_id,
                   sol.*
            from sale_order_line as sol
            join product_product as pp on sol.product_id = pp.id
            join product_template as pt on pp.product_tmpl_id = pt.id
            order by sol.order_id, sol.sequence, sol.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> SaleOrderLine.exportId
                reader.int "order_id" |> Some |> SaleOrder.exportId
                reader.text "name"
                reader.int "sequence" |> string
                reader.textOrNone "invoice_status" |> orEmptyString
                reader.double "price_unit" |> formatDecimal
                reader.doubleOrNone "price_subtotal" |> formatDecimalOption
                reader.doubleOrNone "price_tax" |> formatDecimalOption
                reader.doubleOrNone "price_total" |> formatDecimalOption
                reader.doubleOrNone "price_reduce" |> formatDecimalOption
                reader.doubleOrNone "price_reduce_taxinc" |> formatDecimalOption
                reader.doubleOrNone "price_reduce_taxexcl" |> formatDecimalOption
                reader.doubleOrNone "discount" |> formatDecimalOption
                reader.int "product_template_id" |> Some |> ProductTemplate.exportId
                reader.double "product_uom_qty" |> formatDecimal
                reader.intOrNone "product_uom" |> orEmptyString
                reader.textOrNone "qty_delivered_method" |> orEmptyString
                reader.doubleOrNone "qty_delivered" |> formatDecimalOption
                reader.doubleOrNone "qty_delivered_manual" |> formatDecimalOption
                reader.doubleOrNone "qty_to_invoice" |> formatDecimalOption
                reader.doubleOrNone "qty_invoiced" |> formatDecimalOption
                reader.doubleOrNone "untaxed_amount_invoiced" |> formatDecimalOption
                reader.doubleOrNone "untaxed_amount_to_invoice" |> formatDecimalOption
                reader.intOrNone "salesman_id" |> ResUsers.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.intOrNone "order_partner_id" |> ResPartner.exportId
                reader.boolOrNone "is_expense" |> orEmptyString
                reader.boolOrNone "is_downpayment" |> orEmptyString
                reader.textOrNone "state" |> orEmptyString
                reader.double "customer_lead" |> string
                reader.textOrNone "display_type" |> orEmptyString
                reader.boolOrNone "is_service" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
