namespace Services.Exporting.Odoo

open Model
open Services.Exporting.Odoo.ExportHelpers

type ExportPurchase () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportOrder (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "access_token" ; "name" ; "priority" ; "origin" ; "partner_ref"
            "date_order" ; "date_approve" ; "partner_id/id" ; "dest_address_id/id"
            "currency_id/id" ; "state" ; "notes" ; "invoice_count" ; "invoice_status"
            "date_planned" ; "date_calendar_start" ; "amount_untaxed" ; "amount_tax"
            "amount_total" ; "fiscal_position_id/id" ; "payment_term_id/id" ; "incoterm_id/id"
            "user_id/id" ; "company_id/.id" ; "currency_rate" ; "mail_reminder_confirmed"
            "mail_reception_confirmed" ; "picking_type_id/id" ; "group_id/id" ; "effective_date"
            "supplier_partner_bank_id/id" ; "payment_mode_id/id"
        ]

        let sql = """
            with
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            ),
			rel_account_fiscal_position as (
                select module, model, res_id as id, 'account.' || name as external_id
                from ir_model_data
                where model = 'account.fiscal.position'
                and module not like '\_\_%'
            ),
			rel_account_payment_term as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'account.payment.term'
                and module not like '\_\_%'
            ),
			rel_stock_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
            )
            select
                   rrc.external_id as currency_external_id,
                   rafp.external_id as fiscal_position_external_id,
                   rapt.external_id as payment_term_external_id,
                   rspt.external_id as picking_type_external_id,
                   po.*
            from purchase_order as po
            left join rel_res_currency as rrc on po.currency_id = rrc.id
            left join rel_account_fiscal_position as rafp on po.fiscal_position_id = rafp.id
            left join rel_account_payment_term as rapt on po.payment_term_id = rapt.id
            left join rel_stock_picking_type as rspt on po.picking_type_id = rspt.id
            order by po.create_date
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> PurchaseOrder.exportId
                reader.textOrNone "access_token" |> orEmptyString
                reader.text "name"
                reader.textOrNone "priority" |> orEmptyString
                reader.textOrNone "origin" |> orEmptyString
                reader.textOrNone "partner_ref" |> orEmptyString
                reader.dateTime "date_order" |> Some |> dateTimeOrEmptyString
                reader.dateTimeOrNone "date_approve" |> dateTimeOrEmptyString
                reader.int "partner_id" |> Some |> ResPartner.exportId
                reader.intOrNone "dest_address_id" |> ResPartner.exportId
                reader.text "currency_external_id"
                reader.textOrNone "state" |> orEmptyString
                reader.textOrNone "notes" |> orEmptyString
                reader.intOrNone "invoice_count" |> orEmptyString
                reader.textOrNone "invoice_status" |> orEmptyString
                reader.dateTimeOrNone "date_planned" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "date_calendar_start" |> dateTimeOrEmptyString
                reader.double "amount_untaxed" |> formatDecimal
                reader.double "amount_tax" |> formatDecimal
                reader.double "amount_total" |> formatDecimal
                reader.textOrNone "fiscal_position_external_id" |> orEmptyString
                reader.intOrNone "payment_term_id" |> AccountPaymentTerm.exportId
                ""                // incoterm
                reader.intOrNone "user_id" |> ResUsers.exportId
                reader.int "company_id" |> string
                reader.doubleOrNone "currency_rate" |> formatDecimalOption
                reader.boolOrNone "mail_reminder_confirmed" |> orEmptyString
                reader.boolOrNone "mail_reception_confirmed" |> orEmptyString
                reader.text "picking_type_external_id"
                reader.intOrNone "group_id" |> ProcurementGroup.exportId
                reader.dateTimeOrNone "effective_date" |> dateTimeOrEmptyString
                reader.intOrNone "supplier_partner_bank_id" |> ResPartnerBank.exportId
                reader.intOrNone "payment_mode_id" |> AccountPaymentMode.exportId

                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportOrderLine (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "sequence" ; "product_qty" ; "product_uom_qty" ; "date_planned" ; "product_uom"
            "product_id/id" ; "price_unit" ; "price_subtotal" ; "price_total" ; "price_tax" ; "order_id/id"
            "account_analytic_id/id" ; "company_id/.id" ; "state" ; "qty_invoiced" ; "qty_received_method"
            "qty_received" ; "qty_received_manual" ; "qty_to_invoice" ; "partner_id/id" ; "currency_id/id"
            "product_packaging_id/id" ; "product_packaging_qty" ; "display_type" ; "orderpoint_id/id"
            "product_description_variants" ; "propagate_cancel" ; "sale_order_id/id" ; "sale_line_id/id"
        ]

        let sql = """
            with
			rel_uom_uom as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'uom.uom'
                and module not like '\_\_%'
            ),
			rel_res_currency as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'res.currency'
                and module not like '\_\_%'
            )
            select ruom.external_id as product_uom_external_id,
                   rrc.external_id as currency_external_id,
                   pol.*
            from purchase_order_line as pol
            left join rel_uom_uom as ruom on pol.product_uom = ruom.id
            left join rel_res_currency as rrc on pol.currency_id = rrc.id
            order by pol.create_date, pol.id
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> PurchaseOrderLine.exportId
                reader.text "name"
                reader.intOrNone "sequence" |> orEmptyString
                reader.doubleOrNone "product_qty" |> formatDecimalOption
                reader.doubleOrNone "product_uom_qty" |> formatDecimalOption
                reader.dateTimeOrNone "date_planned" |> dateTimeOrEmptyString

                match reader.textOrNone "product_uom_external_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "product_uom" |> UomUom.exportId

                reader.intOrNone "product_id" |> ProductProduct.exportId
                reader.double "price_unit" |> Some |> formatDecimalOption
                reader.doubleOrNone "price_subtotal" |> formatDecimalOption
                reader.doubleOrNone "price_total" |> formatDecimalOption
                reader.doubleOrNone "price_tax" |> formatDecimalOption
                reader.int "order_id" |> Some |> PurchaseOrder.exportId
                ""           // account_analytic_id
                reader.intOrNone "company_id" |> orEmptyString
                reader.textOrNone "state" |> orEmptyString
                reader.doubleOrNone "qty_invoiced" |> formatDecimalOption
                reader.textOrNone "qty_received_method" |> orEmptyString
                reader.doubleOrNone "qty_received" |> formatDecimalOption
                reader.doubleOrNone "qty_received_manual" |> formatDecimalOption
                reader.doubleOrNone "qty_to_invoice" |> formatDecimalOption
                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.textOrNone "currency_external_id" |> orEmptyString
                ""           // product_packaging_id
                reader.doubleOrNone "product_packaging_qty" |> formatDecimalOption
                reader.textOrNone "display_type" |> orEmptyString
                ""           // orderpoint_id
                reader.textOrNone "product_description_variants" |> orEmptyString
                reader.boolOrNone "propagate_cancel" |> orEmptyString
                reader.intOrNone "sale_order_id" |> SaleOrder.exportId
                reader.intOrNone "sale_line_id" |> SaleOrderLine.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
