namespace Services.Exporting.Odoo

open Model
open Model.Constants

type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker

type Service () =

    //------------------------------------------------------------------------------------------------------------------
    static let flattenData (joinData : (string list * string list list) list) =
        [
            for recordData, recordLines in joinData do
                recordData @ (recordLines |> List.head |> List.tail)
                for recordLine in (recordLines |> List.tail) do
                    [for _ in recordData -> ""] @ (recordLine |> List.tail)
        ]
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResBank (modelName : string) =

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
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResPartnerBank (modelName : string) =

        let header = [ "id" ; "bank_id/id" ; "acc_number"; "sequence"
                       "partner_id/id" ; "acc_holder_name" ; "description" ]

        let sql = $"""Select id, acc_number, sequence, partner_id, bank_id, acc_holder_name, description
                      from res_partner_bank
                      where company_id={ORIG_COMPANY_ID}"""

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> ResPartnerBank.exportId
                reader.intOrNone "bank_id" |> Bank.exportId
                reader.text "acc_number"
                reader.int "sequence" |> string
                reader.int "partner_id" |> ResPartner.exportId
                reader.textOrNone "acc_holder_name" |> Option.defaultValue ""
                reader.textOrNone "description" |> Option.defaultValue ""
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportAccountPaymentTerm (modelName : string) =

        let header = [ "id" ; "name" ; "note" ; "sequence"
                       "line_ids/value" ; "line_ids/value_amount" ; "line_ids/days"
                       "line_ids/day_of_the_month" ; "line_ids/option" ; "line_ids/sequence" ]

        let sql = $"""Select id, name, note, sequence
                      from account_payment_term
                      where company_id={ORIG_COMPANY_ID}"""

        let termReaderFun (reader : RowReader) =
            [
                reader.int "id" |> AccountPaymentTerm.exportId
                reader.text "name"
                $"""<p>{reader.textOrNone "note" |> Option.defaultValue (reader.text "name")}</p>"""
                reader.int "sequence" |> string
            ]

        let sqlForLines = $"""Select id, value, value_amount, days, day_of_the_month,
                                     option, payment_id, sequence
                              from {modelName}_line"""

        let termLineReaderFun (reader : RowReader) =
            [
                reader.int "payment_id" |> AccountPaymentTerm.exportId
                reader.text "value"
                reader.doubleOrNone "value_amount" |> Option.defaultValue 0.0 |> string
                reader.int "days" |> string
                reader.intOrNone "day_of_the_month" |> Option.defaultValue 0 |> string
                reader.text "option"
                reader.int "sequence" |> string
            ]

        let terms = ISqlBroker.getExportData sql termReaderFun
        let termLines = ISqlBroker.getExportData sqlForLines termLineReaderFun

        let joinData = [
            for term in terms -> (term, termLines |> List.filter (fun termLine -> termLine[0] = term[0]))
        ]

        header::(flattenData joinData)
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportResUsers (modelName : string) =

        let header = [ "id" ; "login"; "name" ; "notification_type" ; "sale_team_id/id"
                       "working_year" ; "lowest_working_date" ]

        let sql = $"""Select res_users.id, login, name, notification_type, working_year, lowest_working_date
                      from res_users
                      join res_partner on res_users.partner_id = res_partner.id
                      where res_users.company_id={ORIG_COMPANY_ID}
                        and res_users.active = true"""

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> ResUsers.exportId
                reader.text "login"
                reader.text "name"
                reader.text "notification_type"
                "sales_team.team_sales_department"     // sale_team_id
                reader.textOrNone "working_year" |> Option.defaultValue ""
                match reader.dateOnlyOrNone "lowest_working_date" with
                | Some d -> $"{d.Year}-{d.Month}-{d.Day}"
                | None -> ""
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
