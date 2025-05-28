namespace Services.Exporting.Odoo

open Motsoft.Util
open Model
open ExportHelpers

type ISqlBroker = DI.Brokers.SqlDI.ISqlBroker
type IExcelBroker = DI.Brokers.StorageDI.IExcelBroker

type Service () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportProcurementGroup (modelName : string) =

        let header = addStampHeadersTo [ "id" ; "partner_id/id" ; "name" ; "move_type" ; "sale_id/id" ]

        let sql = """
            select *
            from procurement_group
            order by create_date
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> ProcurementGroup.exportId
                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.text "name"
                reader.text "move_type"
                reader.intOrNone "sale_id" |> SaleOrder.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportDeysankaResConfigSettings (modelName : string) =

        // Importador propio -> Sin stamp fields.

        //--------------------------------------------------------------------------------------------------------------
        let getValueAsString (readerFun :RowReader -> string) (record_id : int) (fieldName : string) : string =
            let sql = $"""
                select {fieldName}
                from account_journal
                where id = {record_id}
            """

            ISqlBroker.getExportData sql readerFun
            |> List.head
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let getStringValue (record_id : int) (fieldName : string) : string =

            let readerFun (reader : RowReader) =
                reader.textOrNone fieldName |> orEmptyString

            getValueAsString readerFun record_id fieldName
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let getIntValueAsString (record_id : int) (fieldName : string) : string =

            let readerFun (reader : RowReader) =
                reader.intOrNone fieldName |> orEmptyString

            getValueAsString readerFun record_id fieldName
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let getIdFromData (data : string list list) (settingName : string) : int =
            data
            |> List.find (fun line -> line[0] = settingName)
            |> fun line -> split "_" line[1] |> Array.last |> int
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let resConfigSettingsMap =                   // Nombre de ir.config.parameter --> nombre en res.config.settings
            Map [
                "deysanka_account.bad_reconcile_exclude_accounts", "bad_reconcile_exclude_accounts"
                "deysanka_account.bank_batch_charges_prefix", "bank_batch_charges_prefix"
                "deysanka_account.bank_batch_credit_prefix", "bank_batch_credit_prefix"
                "deysanka_account.bank_charges_client_journal_id", "bank_charges_client_journal_id"
                "deysanka_account.bank_charges_client_payment_mode_id", "bank_charges_client_payment_mode_id"
                "deysanka_account.bank_charges_client_payment_term_id", "bank_charges_client_payment_term_id"
                "deysanka_account.bank_charges_client_product_id", "bank_charges_client_product_id"
                "deysanka_account.bank_charges_partner_id", "bank_charges_partner_id"
                "deysanka_account.bank_charges_product_id", "bank_charges_product_id"
                "deysanka_account.bank_charges_ret_journal_id", "bank_charges_ret_journal_id"
                "deysanka_account.cash_statement_dey_cash_journal_id", "cash_statement_dey_cash_journal_id"
                "deysanka_account.cash_statement_eb_cash_journal_id", "cash_statement_eb_cash_journal_id"
                "deysanka_account.closing_journal_id", "closing_journal_id"
                "deysanka_account.deysanka_checks_proxy_url", "deysanka_checks_proxy_url"
                "deysanka_account.monthly_sales_cash_journal_id", "monthly_sales_cash_journal_id"
                "deysanka_account.monthly_sales_journal_id", "monthly_sales_journal_id"
                "deysanka_account.monthly_sales_partner_id", "monthly_sales_partner_id"
                "deysanka_account.monthly_sales_payment_mode_id", "monthly_sales_payment_mode_id"
                "deysanka_account.monthly_sales_payment_term_id", "monthly_sales_payment_term_id"
                "deysanka_account.monthly_sales_product_ptc_id", "monthly_sales_product_ptc_id"
                "deysanka_account.monthly_sales_product_pte_id", "monthly_sales_product_pte_id"
                "deysanka_account.monthly_sales_product_ptt_id", "monthly_sales_product_ptt_id"
                "deysanka_account.monthly_sales_tpv_code", "monthly_sales_tpv_code"
                "deysanka_account.partner_deysanka_id", "partner_deysanka_id"
                "deysanka_account.tag_name_mensual", "tag_name_mensual"
                "deysanka_account.unpaid_inv_account_id", "unpaid_inv_account_id"
                "deysanka_account.unpaid_inv_payment_mode_by_customer", "unpaid_inv_payment_mode_by_customer"
                "deysanka_account.unpaid_inv_ref_prefix", "unpaid_inv_ref_prefix"
                "deysanka_account.web_sales_partner_id", "web_sales_partner_id"
                "deysanka_account.web_sales_tpv_code", "web_sales_tpv_code"
            ]
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let exportFunMap =
            Map [
                "bank_charges_client_journal_id", AccountJournal.exportId
                "bank_charges_client_payment_mode_id", AccountPaymentMode.exportId
                "bank_charges_client_payment_term_id", AccountPaymentTerm.exportId
                "bank_charges_client_product_id", ProductTemplate.exportId
                "bank_charges_partner_id", ResPartner.exportId
                "bank_charges_product_id", ProductTemplate.exportId
                "bank_charges_ret_journal_id", AccountJournal.exportId
                "cash_statement_dey_cash_journal_id", AccountJournal.exportId
                "cash_statement_eb_cash_journal_id", AccountJournal.exportId
                "closing_journal_id", AccountJournal.exportId
                "monthly_sales_cash_journal_id", AccountJournal.exportId
                "monthly_sales_journal_id", AccountJournal.exportId
                "monthly_sales_partner_id", ResPartner.exportId
                "monthly_sales_payment_mode_id", AccountPaymentMode.exportId
                "monthly_sales_payment_term_id", AccountPaymentTerm.exportId
                "monthly_sales_product_ptc_id", ProductTemplate.exportId
                "monthly_sales_product_pte_id", ProductTemplate.exportId
                "monthly_sales_product_ptt_id", ProductTemplate.exportId
                "partner_deysanka_id", ResPartner.exportId
                "unpaid_inv_account_id", AccountAccount.exportId
                "unpaid_inv_payment_mode_by_customer", AccountPaymentMode.exportId
                "web_sales_partner_id", ResPartner.exportId
            ]
        //--------------------------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------
        let getJournalConfigData (deyCashJournalId : int) (ebCashJournalId : int) =
            [
                [ "cash_statement_dey_bank_journal_id"
                  getIntValueAsString deyCashJournalId "bank_journal_id" |> Some |> AccountJournal.exportId ]
                [ "cash_statement_dey_cash_deposit_label"
                  getStringValue deyCashJournalId "bank_cash_move_label" ]
                [ "cash_statement_dey_sales_payment_mode_id"
                  getIntValueAsString deyCashJournalId "sales_payment_mode_id" |> Some |> AccountPaymentMode.exportId ]
                [ "cash_statement_dey_buys_payment_mode_id"
                  getIntValueAsString deyCashJournalId "buys_payment_mode_id" |> Some |> AccountPaymentMode.exportId ]
                [ "cash_statement_eb_bank_journal_id"
                  getIntValueAsString ebCashJournalId "bank_journal_id" |> Some |> AccountJournal.exportId ]
                [ "cash_statement_eb_cash_deposit_label"
                  getStringValue ebCashJournalId "bank_cash_move_label" ]
                [ "cash_statement_eb_sales_payment_mode_id"
                  getIntValueAsString ebCashJournalId "sales_payment_mode_id" |> Some |> AccountPaymentMode.exportId ]
                [ "cash_statement_eb_buys_payment_mode_id"
                  getIntValueAsString ebCashJournalId "buys_payment_mode_id" |> Some |> AccountPaymentMode.exportId ]
            ]
        //--------------------------------------------------------------------------------------------------------------

        let header = [ "name" ; "value" ]

        let sql = """
            select *
            from ir_config_parameter
            where key ilike 'deysanka_account%'
            order by key
        """

        let readerFun (reader : RowReader) =
            [
                let key = resConfigSettingsMap[reader.text "key"]
                key

                if exportFunMap.ContainsKey key
                then reader.text "value" |> Some |> exportFunMap[key]
                else reader.text "value"
            ]

        let configData = header::ISqlBroker.getExportData sql readerFun

        let deyCashJournalId = getIdFromData configData "cash_statement_dey_cash_journal_id"
        let ebCashJournalId = getIdFromData configData "cash_statement_eb_cash_journal_id"

        let journalConfigData = getJournalConfigData deyCashJournalId ebCashJournalId

        configData @ journalConfigData
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
