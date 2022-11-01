namespace Brokers.Storage.Excel

open System.IO
open ClosedXML.Excel

type Broker () =

    static let mutable _outputPath = ""

    //------------------------------------------------------------------------------------------------------------------
    static member init outputPath =
        _outputPath <- outputPath
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member OutputPath with get () = _outputPath
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportFile (fileName : string) (data : string list list) =

        let workbook = new XLWorkbook()
        let worksheet = workbook.Worksheets.Add()

        worksheet.Cell("A1").InsertData(data) |> ignore

        Path.Combine(Broker.OutputPath, fileName)
        |> workbook.SaveAs
    //------------------------------------------------------------------------------------------------------------------
