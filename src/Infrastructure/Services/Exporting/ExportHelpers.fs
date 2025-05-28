module Services.Exporting.Odoo.ExportHelpers

open System
open System.Globalization
open Model

type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker

//------------------------------------------------------------------------------------------------------------------
let [<Literal>] COL_ACCOUNT = 1
let [<Literal>] COL_DEBIT = 4
let [<Literal>] COL_CREDIT = 5
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let formatDecimalOption (valueOption : double option) =
    match valueOption with
    | Some value -> value.ToString("########0.00", CultureInfo.InvariantCulture)
    | None -> ""
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let formatDecimal (value : double) =
    value.ToString("########0.00", CultureInfo.InvariantCulture)
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let flattenData (joinData : (string list * string list list) list) =
    [
        for recordData, recordLines in joinData do
            recordData @ (recordLines |> List.head |> List.tail)
            for recordLine in (recordLines |> List.tail) do
                [for _ in recordData -> ""] @ (recordLine |> List.tail)
    ]
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let orEmptyString (optVal : 'a option) =
    match optVal with
    | Some value -> value |> string
    | None -> ""
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let dateOrEmptyString (optVal : DateOnly option) =
    match optVal with
    | Some d -> d.ToString("yyyy-MM-dd")
    | None -> ""
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let dateTimeOrEmptyString (optVal : DateTime option) =
    match optVal with
    | Some d -> d.ToString("yyyy-MM-dd HH:mm:ss")
    | None -> ""
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let readStampFields (reader : RowReader) =
    [
        match reader.int "create_uid" with
        | 1 -> "base.user_root"
        | _ -> reader.int "create_uid" |> Some |> ResUsers.exportId

        reader.dateTimeOrNone "create_date" |> dateTimeOrEmptyString

        match reader.int "write_uid" with
        | 1 -> "base.user_root"
        | _ -> reader.int "write_uid" |> Some |> ResUsers.exportId

        reader.dateTimeOrNone "write_date" |> dateTimeOrEmptyString
    ]
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let getSequenceName (sequenceId : int) =
    $"ir_sequence_{sequenceId:D3}"
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let getDateRangeSequenceName (sequenceId : int) (sequenceDateRangeId : int) =
    $"ir_sequence_{sequenceId:D3}_{sequenceDateRangeId:D3}"
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let getSequenceNumberNextActual (sequenceName : string) =
    // Esta función sólo es válida para secuencias de tipo standard.

    let sql = $"""
        select
            last_value,
            (select increment_by
             from pg_sequences
             where sequencename = '{sequenceName}'),
            is_called
        from {sequenceName}
    """

    let readerFun (reader : RowReader) =
        [
            reader.int "last_value"
            reader.int "increment_by"
            if reader.bool "is_called" then 1 else 0
        ]

    let sequenceData = ISqlBroker.getExportData sql readerFun
    let lastValue = sequenceData[0][0]
    let incrementBy = sequenceData[0][1]
    let isCalled = sequenceData[0][2]

    if isCalled = 1 then
        lastValue + incrementBy
    else
        lastValue
//------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------
let addStampHeadersTo (fields : string list) =
    List.append fields ["create_uid/id" ; "create_date" ; "write_uid/id" ; "write_date"]
//------------------------------------------------------------------------------------------------------------------
