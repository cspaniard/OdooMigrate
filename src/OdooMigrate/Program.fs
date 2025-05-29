
open System

type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker
type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IOdooExportService = DI.Services.ExportingDI.IOdooExportService
type IAccountExportService = DI.Services.ExportingDI.IExportAccountService
type IResExportService = DI.Services.ExportingDI.IExportResService
type IIrExportService = DI.Services.ExportingDI.IExportIrService
type IProductExportService = DI.Services.ExportingDI.IExportProductService
type IStockExportService = DI.Services.ExportingDI.IExportStockService
type ISaleExportService = DI.Services.ExportingDI.IExportSaleService
type IPurchaseExportService = DI.Services.ExportingDI.IExportPurchaseService
type ITableNamesExportService = DI.Services.ExportingDI.IExportTableNamesService

open Model.Constants

ISqlBroker.init CONNECTION_STRING_15
// IExcelBroker.init "/home/dsanroma/odoo_export"
IExcelBroker.init "/home/dsanroma/odoo_export/deysanka_15"

let exportList =
    [
        ("res_users", IResExportService.exportUsers)
        ("res_groups_users_rel", IResExportService.exportGroupsUsersRel)
        ("res_bank", IResExportService.exportBank)
        ("res_partner_bank", IResExportService.exportPartnerBank)
        ("res_partner", IResExportService.exportPartner)

        ("ir_sequence", IIrExportService.exportSequence)
        ("ir_sequence_date_range", IIrExportService.exportSequenceDateRange)
        ("ir_property_defaults", IIrExportService.exportPropertyDefaults)
        // ("ir_attachment", IIrExportService.exportAttachment)     // Hacer desde un shell o sólo parte
        // ("ir_default", IIrExportService.exportDefault)               // No entiendo este modelo

        ("stock_picking", IStockExportService.exportPicking)
        ("stock_lot", IStockExportService.exportProductionLot)
        ("stock_picking_type", IStockExportService.exportPickingType)
        ("stock_location", IStockExportService.exportLocation)
        ("stock_quant", IStockExportService.exportQuant)
        ("stock_route_product", IStockExportService.exportRouteProduct)
        // -------------------------------------------------------------------------------------------
        ("stock_warehouse", IStockExportService.exportWarehouse)
        ("stock_route", IStockExportService.exportLocationRoute)
        ("stock_rule", IStockExportService.exportRule)
        ("stock_valuation_layer", IStockExportService.exportValuationLayer)
        ("stock_putaway_rule", IStockExportService.exportPutawayRule)
        ("stock_wh_resupply_table", IStockExportService.exportWhResupplyTable)
        // -------------------------------------------------------------------------------------------
        ("stock_move", IStockExportService.exportMove)
        ("stock_move_line", IStockExportService.exportMoveLine)

        ("procurement_group", IOdooExportService.exportProcurementGroup)

        ("sale_order", ISaleExportService.exportOrder)
        ("sale_order_line", ISaleExportService.exportOrderLine)

        ("purchase_order", IPurchaseExportService.exportOrder)
        ("purchase_order_line", IPurchaseExportService.exportOrderLine)

        ("account_account", IAccountExportService.exportAccount)
        ("account_journal_base", IAccountExportService.exportJournalBase)
        ("account_journal_payment_mode", IAccountExportService.exportJournalPaymentMode)
        ("account_payment_mode", IAccountExportService.exportPaymentMode)
        ("account_payment_method", IAccountExportService.exportPaymentMethod)
        ("account_payment_term", IAccountExportService.exportPaymentTerm)
        ("account_banking_mandate", IAccountExportService.exportBankingMandate)
        ("account_opening_move_15", IAccountExportService.exportOpeningMove)
        // ("account_move", IAccountExportService.exportMove)            // Pendiente

        ("product_category", IProductExportService.exportCategory)
        ("product_template", IProductExportService.exportTemplate)
        ("product_taxes", IProductExportService.exportTaxes)
        ("product_supplier_taxes", IProductExportService.exportSupplierTaxes)
        ("product_supplierinfo", IProductExportService.exportSupplierInfo)
        ("product_pricelist", IProductExportService.exportPriceList)
        ("product_pricelist_item", IProductExportService.exportPriceListItem)
        ("product_product", IProductExportService.exportProduct)

        ("deysanka_res_config_settings", IOdooExportService.exportDeysankaResConfigSettings)
        ("tables_not_in_17", ITableNamesExportService.exportNotFoundIn17)
        ("tables_used_in_15", ITableNamesExportService.exportUsedTableNames15)
        ("tables_used_in_17", ITableNamesExportService.exportUsedTableNames17)

    ]

Console.ForegroundColor <- ConsoleColor.Yellow
Console.WriteLine "\nExportando datos/modelos:"
Console.ForegroundColor <- ConsoleColor.White

let stopwatch = System.Diagnostics.Stopwatch.StartNew()
exportList
|> List.iteri (fun i (modelName, exportFun) -> Console.WriteLine $"{i+1,3} - {modelName}"
                                               exportFun modelName)

stopwatch.Stop()

Console.WriteLine $"\nTiempo total de exportación: {stopwatch.Elapsed}"
