namespace Services.Exporting.Odoo

open Model
open Model.Constants

type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker

type Service () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportResBank () =

        let header = [ "id" ; "name" ; "bic" ; "country/id" ]

        let sql = """Select id, name, bic
                     from res_bank
                     where active=true"""

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> Bank.exportId
                reader.text "name"
                reader.textOrNone "bic" |> Option.defaultValue ""
                "base.es"      // country/id
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile "res_bank.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResPartnerBank () =

        let header = [ "id" ; "bank_id/id" ; "acc_number"; "sequence"
                       "partner_id/id" ; "acc_holder_name" ; "description" ]

        let sql = $"""Select id, acc_number, sequence, partner_id, bank_id, acc_holder_name, description
                      from res_partner_bank
                      where company_id={ORIG_COMPANY_ID}"""

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> PartnerBank.exportId
                reader.intOrNone "bank_id" |> Bank.exportId
                reader.text "acc_number"
                reader.int "sequence" |> string
                reader.int "partner_id" |> Partner.exportId
                reader.textOrNone "acc_holder_name" |> Option.defaultValue ""
                reader.textOrNone "description" |> Option.defaultValue ""
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile "res_partner_bank.xlsx"
    //------------------------------------------------------------------------------------------------------------------
