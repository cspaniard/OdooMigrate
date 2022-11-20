namespace Model

module Constants =
    let [<Literal>] DEST_COMPANY_ID = "1"
    let [<Literal>] ORIG_COMPANY_ID = "2"
    let [<Literal>] CONNECTION_STRING = "Host=localhost; Database=zzz_gestion1; Username=dsanroma; Password=pepe;"

module Helpers =
    let exportId (modelName : string) (idOption : 'a option) =
        match idOption with
        | Some id -> $"__export__.{modelName}_{id}"
        | None -> ""

open Helpers

type Bank = Bank with
    static member exportId idOption = exportId "res_bank" idOption

type ResUsers = ResUsers with
    static member exportId idOption = exportId "res_users" idOption

type ResPartner = ResPartner with
    static member exportId idOption = exportId "res_partner" idOption

type ResPartnerBank = ResPartnerBank with
    static member exportId idOption = exportId "res_partner_bank" idOption

type AccountPaymentTerm = AccountPaymentTerm with
    static member exportId idOption = exportId "account_payment_term" idOption

type AccountJournal = AccountJournal with
    static member exportId idOption = exportId "account_journal" idOption

type AccountBankingMandate = AccountBankingMandate with
    static member exportId idOption = exportId "account_banking_mandate" idOption

type ProductPriceList = AccountProductPriceList with
    static member exportId idOption = exportId "product_pricelist" idOption

type ProductCategory = ProductCategory with
    static member exportId idOption = exportId "product_category" idOption

type ProductTemplate = ProductTemplate with
    static member exportId idOption = exportId "product_template" idOption

type ProductSupplierInfo = ProductSupplierInfo with
    static member exportId idOption = exportId "product_supplierinfo" idOption

type ProductPriceListItem = ProductPriceListItem with
    static member exportId idOption = exportId "product_pricelist_item" idOption

type AccountPaymentMode = AccountPaymentMode with
    static member exportId idOption = exportId "account_payment_mode" idOption

type AccountOpeningMove = AccountOpeningMove with
    static member exportId idOption = exportId "account_opening_move" idOption

type AccountMoveLine = AccountMoveLine with
    static member exportId idOption = exportId "account_move_line" idOption
