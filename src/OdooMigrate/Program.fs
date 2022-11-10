﻿
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
        ("account_payment_term", IOdooExportService.exportAccountPaymentTerm)
        // ("account_payment_term_line", IOdooExportService.exportAccountPaymentTermLine)
    ]

Console.ForegroundColor <- ConsoleColor.Yellow
Console.WriteLine "\nExportando datos/modelos:"
Console.ForegroundColor <- ConsoleColor.White

exportList
|> List.iteri (fun i (modelName, exportFun) -> Console.WriteLine $"{i+1,3} - {modelName}"
                                               exportFun modelName)
