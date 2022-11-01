namespace Model

module Constants =
    let [<Literal>] DEST_COMPANY_ID = "1"
    let [<Literal>] ORIG_COMPANY_ID = "2"
    let [<Literal>] CONNECTION_STRING = "Host=localhost; Database=zzz_gestion1; Username=dsanroma; Password=pepe;"

type Bank = Bank with
    static member exportId idOption =
        match idOption with
        | Some id -> $"__export__res_bank_{id}"
        | None -> ""

type Partner = Partner with
    static member exportId id = $"__export__res_partner_{id}"

type PartnerBank = PartnerBank with
    static member exportId id = $"__export__res_partner_bank_{id}"
