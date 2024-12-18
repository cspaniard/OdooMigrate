
open System

type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker
type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IOdooExportService = DI.Services.ExportingDI.IOdooExportService

open Model.Constants

ISqlBroker.init CONNECTION_STRING
// IExcelBroker.init "/home/dsanroma/odoo_export"
IExcelBroker.init "/home/dsanroma/odoo_export/deysanka_15"

let exportList =
    [
        // ("res_users", IOdooExportService.exportResUsers)
        // ("res_groups_users_rel", IOdooExportService.exportResGroupsUsersRel)
        // ("stock_picking", IOdooExportService.exportStockPicking)
        // ("procurement_group", IOdooExportService.exportProcurementGroup)
        // ("stock_lot", IOdooExportService.exportStockLot)
        // ("sale_order", IOdooExportService.exportSaleOrder)
        // ("sale_order_line", IOdooExportService.exportSaleOrderLine)
        ("ir_sequence", IOdooExportService.exportIrSequence)
        ("ir_sequence_date_range", IOdooExportService.exportIrSequenceDateRange)
        // ("account_account", IOdooExportService.exportAccountAccount)
        // ("account_journal_base", IOdooExportService.exportAccountJournalBase)
        // ("account_journal_payment_mode", IOdooExportService.exportAccountJournalPaymentMode)
        // ("account_payment_mode", IOdooExportService.exportAccountPaymentMode)
        // ("stock_picking_type", IOdooExportService.exportStockPickingType)
        // ("stock_location", IOdooExportService.exportStockLocation)
        // ("stock_warehouse", IOdooExportService.exportStockWarehouse)
        // ("deysanka_res_config_settings", IOdooExportService.exportDeysankaResConfigSettings)
        // ("product_category", IOdooExportService.exportProductCategory)
        // ("product_template", IOdooExportService.exportProductTemplate)
        // ("product_taxes", IOdooExportService.exportProductTaxes)
        // ("product_supplier_taxes", IOdooExportService.exportProductSupplierTaxes)
        // ("product_supplierinfo", IOdooExportService.exportProductSupplierInfo)
        // ("product_pricelist", IOdooExportService.exportProductPriceList)
        // ("product_pricelist_item", IOdooExportService.exportProductPriceListItem)
        // ("account_payment_method", IOdooExportService.exportAccountPaymentMethod)
        // ("res_bank", IOdooExportService.exportResBank)
        // ("res_partner_bank", IOdooExportService.exportResPartnerBank)
        // ("account_payment_term", IOdooExportService.exportAccountPaymentTerm)
        // ("res_partner", IOdooExportService.exportResPartner)
        // ("account_banking_mandate", IOdooExportService.exportAccountBankingMandate)

        // Dejar tranquilos por ahora.
        // ("account_opening_move_15", IOdooExportService.exportAccountOpeningMove)
        // ("ir_attachment", IOdooExportService.exportIrAttachment)
        // ("account_move", IOdooExportService.exportAccountMove)
        // ("ir_default", IOdooExportService.exportDefaultValues)
    ]

Console.ForegroundColor <- ConsoleColor.Yellow
Console.WriteLine "\nExportando datos/modelos:"
Console.ForegroundColor <- ConsoleColor.White

exportList
|> List.iteri (fun i (modelName, exportFun) -> Console.WriteLine $"{i+1,3} - {modelName}"
                                               exportFun modelName)
