
open System

type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker
type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IOdooExportService = DI.Services.ExportingDI.IOdooExportService

open Model.Constants

ISqlBroker.init CONNECTION_STRING
IExcelBroker.init "/home/dsanroma/odoo_export"

let exportList =
    [
        // ("res_bank", IOdooExportService.exportResBank)
        // ("res_partner_bank", IOdooExportService.exportResPartnerBank)
        // ("account_payment_term", IOdooExportService.exportAccountPaymentTerm)
        // ("res_users", IOdooExportService.exportResUsers)
        ("res_partner", IOdooExportService.exportResPartner)
        // ("account_account", IOdooExportService.exportAccountAccount)
        // ("account_journal", IOdooExportService.exportAccountJournal)
        // ("account_banking_mandate", IOdooExportService.exportAccountBankingMandate)
        // ("product_pricelist", IOdooExportService.exportProductPriceList)
        // ("product_category_translation", IOdooExportService.exportProductCategoryTranslation)
        // ("product_category", IOdooExportService.exportProductCategory)
        // ("product_template", IOdooExportService.exportProductTemplate)
        // ("product_taxes", IOdooExportService.exportProductTaxes)
        // ("product_supplier_taxes", IOdooExportService.exportProductSupplierTaxes)
        // ("product_supplierinfo", IOdooExportService.exportProductSupplierInfo)
        // ("product_pricelist_item", IOdooExportService.exportProductPriceListItem)
        // ("account_payment_method", IOdooExportService.exportAccountPaymentMethod)
        // ("account_payment_mode", IOdooExportService.exportAccountPaymentMode)
        // ("account_opening_move", IOdooExportService.exportAccountOpeningMove)
        // ("ir_default", IOdooExportService.exportDefaultValues)
    ]

Console.ForegroundColor <- ConsoleColor.Yellow
Console.WriteLine "\nExportando datos/modelos:"
Console.ForegroundColor <- ConsoleColor.White

exportList
|> List.iteri (fun i (modelName, exportFun) -> Console.WriteLine $"{i+1,3} - {modelName}"
                                               exportFun modelName)
