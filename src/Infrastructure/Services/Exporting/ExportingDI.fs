namespace DI.Services

module ExportingDI =

    type IOdooExportService = Services.Exporting.Odoo.Service
    type IExportAccountService = Services.Exporting.Odoo.ExportAccount
    type IExportResService = Services.Exporting.Odoo.ExportRes
    type IExportIrService = Services.Exporting.Odoo.ExportIr
    type IExportProductService = Services.Exporting.Odoo.ExportProduct
    type IExportStockService = Services.Exporting.Odoo.ExportStock
    type IExportSaleService = Services.Exporting.Odoo.ExportSale
    type IExportPurchaseService = Services.Exporting.Odoo.ExportPurchase
    type IExportTableNamesService = Services.Exporting.Odoo.ExportTableNames
