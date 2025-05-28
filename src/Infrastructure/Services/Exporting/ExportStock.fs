namespace Services.Exporting.Odoo

open Motsoft.Util
open Model
open Services.Exporting.Odoo.ExportHelpers

type ExportStock () =

    //------------------------------------------------------------------------------------------------------------------
    static member exportWarehouse (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "active" ; "company_id/.id" ; "partner_id/id" ; "view_location_id/id"
            "lot_stock_id/id" ; "code" ; "reception_steps" ; "delivery_steps"
            "wh_input_stock_loc_id/id" ; "wh_qc_stock_loc_id/id" ; "wh_output_stock_loc_id/id"
            "wh_pack_stock_loc_id/id" ; "mto_pull_id/id" ; "pick_type_id/id" ; "pack_type_id/id"
            "out_type_id/id" ; "in_type_id/id" ; "int_type_id/id" ; "return_type_id/id"
            "crossdock_route_id/id" ; "reception_route_id/id" ; "delivery_route_id/id"
            "sequence" ; "buy_to_resupply" ; "buy_pull_id/id"
        ]

        let sql = """
			with
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
			),
			rel_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
			)
			select
				view_loc.external_id as view_loc_external_id,
				lot_stock.external_id as lot_stock_external_id,
				input_stock_loc.external_id as input_stock_loc_external_id,
				qc_stock_loc.external_id as qc_stock_loc_external_id,
				output_stock_loc.external_id as output_stock_loc_external_id,
				pack_stock_loc.external_id as pack_stock_loc_external_id,
				pick_type.external_id as pick_type_external_id,
				pack_type.external_id as pack_type_external_id,
				out_type.external_id as out_type_external_id,
				in_type.external_id as in_type_external_id,
				int_type.external_id as int_type_external_id,
				return_type.external_id as return_type_external_id,
				sw.*
            from stock_warehouse as sw
			left join rel_location as view_loc on sw.view_location_id = view_loc.id
			left join rel_location as lot_stock on sw.lot_stock_id = lot_stock.id
			left join rel_location as input_stock_loc on sw.wh_input_stock_loc_id = input_stock_loc.id
			left join rel_location as qc_stock_loc on sw.wh_qc_stock_loc_id = qc_stock_loc.id
			left join rel_location as output_stock_loc on sw.wh_output_stock_loc_id = output_stock_loc.id
			left join rel_location as pack_stock_loc on sw.wh_pack_stock_loc_id = pack_stock_loc.id
			left join rel_picking_type as pick_type on sw.pick_type_id = pick_type.id
			left join rel_picking_type as pack_type on sw.pack_type_id = pack_type.id
			left join rel_picking_type as out_type on sw.out_type_id = out_type.id
			left join rel_picking_type as in_type on sw.in_type_id = in_type.id
			left join rel_picking_type as int_type on sw.int_type_id = int_type.id
			left join rel_picking_type as return_type on sw.return_type_id = return_type.id
            order by id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockWarehouse.exportId
                reader.text "name"
                reader.bool "active" |> string
                reader.int "company_id" |> string
                reader.intOrNone "partner_id" |> ResPartner.exportId

                match reader.textOrNone "view_loc_external_id" with
                | Some view_loc_external_id -> view_loc_external_id
                | None -> reader.intOrNone "view_location_id" |> StockLocation.exportId

                match reader.textOrNone "lot_stock_external_id" with
                | Some lot_stock_external_id -> lot_stock_external_id
                | None -> reader.intOrNone "lot_stock_id" |> StockLocation.exportId

                reader.text "code"
                reader.text "reception_steps"
                reader.text "delivery_steps"

                match reader.textOrNone "input_stock_loc_external_id" with
                | Some input_stock_loc_external_id -> input_stock_loc_external_id
                | None -> reader.intOrNone "wh_input_stock_loc_id" |> StockLocation.exportId

                match reader.textOrNone "qc_stock_loc_external_id" with
                | Some qc_stock_loc_external_id -> qc_stock_loc_external_id
                | None -> reader.intOrNone "wh_qc_stock_loc_id" |> StockLocation.exportId

                match reader.textOrNone "output_stock_loc_external_id" with
                | Some output_stock_loc_external_id -> output_stock_loc_external_id
                | None -> reader.intOrNone "wh_output_stock_loc_id" |> StockLocation.exportId

                match reader.textOrNone "pack_stock_loc_external_id" with
                | Some pack_stock_loc_external_id -> pack_stock_loc_external_id
                | None -> reader.intOrNone "wh_pack_stock_loc_id" |> StockLocation.exportId

                reader.intOrNone "mto_pull_id" |> StockRule.exportId

                match reader.textOrNone "pick_type_external_id" with
                | Some pick_type_external_id -> pick_type_external_id
                | None -> reader.intOrNone "pick_type_id" |> StockPickingType.exportId

                match reader.textOrNone "pack_type_external_id" with
                | Some pack_type_external_id -> pack_type_external_id
                | None -> reader.intOrNone "pack_type_id" |> StockPickingType.exportId

                match reader.textOrNone "out_type_external_id" with
                | Some out_type_external_id -> out_type_external_id
                | None -> reader.intOrNone "out_type_id" |> StockPickingType.exportId

                match reader.textOrNone "in_type_external_id" with
                | Some in_type_external_id -> in_type_external_id
                | None ->reader.intOrNone "in_type_id" |> StockPickingType.exportId

                match reader.textOrNone "int_type_external_id" with
                | Some int_type_external_id -> int_type_external_id
                | None -> reader.intOrNone "int_type_id" |> StockPickingType.exportId

                match reader.textOrNone "return_type_external_id" with
                | Some return_type_external_id -> return_type_external_id
                | None -> reader.intOrNone "return_type_id" |> StockPickingType.exportId

                reader.intOrNone "crossdock_route_id" |> StockRoute.exportId
                reader.intOrNone "reception_route_id" |> StockRoute.exportId
                reader.intOrNone "delivery_route_id" |> StockRoute.exportId

                reader.int "sequence" |> string
                reader.bool "buy_to_resupply" |> string
                reader.intOrNone "buy_pull_id" |> StockRule.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportLocationRoute (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "active" ; "sequence" ; "product_selectable" ; "product_categ_selectable"
            "warehouse_selectable" ; "packaging_selectable" ; "supplied_wh_id/id" ; "supplier_wh_id/id"
            "company_id/.id" ; "sale_selectable" ; "warehouse_ids"
        ]

        let sql = """
            select slr.*,
                   STRING_AGG(srw.warehouse_id::text, ',') AS warehouse_ids
            from stock_location_route as slr
            left join stock_route_warehouse as srw on slr.id = srw.route_id
            group by slr.id
            order by slr.id
            """

        let getExternalWarehouseIds (warehouseIds : string) =
            warehouseIds
            |> split ","
            |> Array.map (Some >> StockWarehouse.exportId)
            |> join ","

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockRoute.exportId
                reader.text "name"
                reader.boolOrNone "active" |> orEmptyString
                reader.intOrNone "sequence" |> orEmptyString
                reader.boolOrNone "product_selectable" |> orEmptyString
                reader.boolOrNone "product_categ_selectable" |> orEmptyString
                reader.boolOrNone "warehouse_selectable" |> orEmptyString
                reader.boolOrNone "packaging_selectable" |> orEmptyString
                reader.intOrNone "supplied_wh_id" |> StockWarehouse.exportId
                reader.intOrNone "supplier_wh_id" |> StockWarehouse.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.boolOrNone "sale_selectable" |> orEmptyString
                reader.stringOrNone "warehouse_ids" |> Option.map getExternalWarehouseIds |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportRule (modelName : string) =
        let header = addStampHeadersTo [
            "id" ; "name" ; "active" ; "group_propagation_option" ; "group_id/id" ; "action" ; "sequence"
            "company_id/.id" ; "location_dest_id/id" ; "location_src_id/id" ; "route_id/id" ; "procure_method"
            "route_sequence" ; "picking_type_id/id" ; "delay" ; "partner_address_id/id" ; "propagate_cancel"
            "propagate_carrier" ; "warehouse_id/id" ; "propagate_warehouse_id/id" ; "auto"
        ]

        let sql = """
			with
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
			),
			rel_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
			)
			select location_dest.external_id as location_dest_external_id,
			       location_src.external_id as location_src_external_id,
			       picking_type.external_id as picking_type_external_id,
			       sr.*
			from stock_rule as sr
			left join rel_location as location_dest on sr.location_id = location_dest.id
			left join rel_location as location_src on sr.location_src_id = location_src.id
			left join rel_picking_type as picking_type on sr.picking_type_id = picking_type.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockRule.exportId
                reader.text "name"
                reader.boolOrNone "active" |> orEmptyString
                reader.textOrNone "group_propagation_option" |> orEmptyString
                reader.intOrNone "group_id" |> ProcurementGroup.exportId
                reader.text "action"
                reader.intOrNone "sequence" |> orEmptyString
                reader.intOrNone "company_id" |> orEmptyString

                match reader.textOrNone "location_dest_external_id" with
                | Some location_dest_external_id -> location_dest_external_id
                | None -> reader.intOrNone "location_id" |> StockLocation.exportId

                match reader.textOrNone "location_src_external_id" with
                | Some location_src_external_id -> location_src_external_id
                | None -> reader.intOrNone "location_src_id" |> StockLocation.exportId

                reader.intOrNone "route_id" |> StockRoute.exportId
                reader.text "procure_method"
                reader.intOrNone "route_sequence" |> orEmptyString

                match reader.textOrNone "picking_type_external_id" with
                | Some picking_type_external_id -> picking_type_external_id
                | None -> reader.intOrNone "picking_type_id" |> StockPickingType.exportId

                reader.intOrNone "delay" |> orEmptyString
                reader.intOrNone "partner_address_id" |> ResPartner.exportId
                reader.boolOrNone "propagate_cancel" |> orEmptyString
                reader.boolOrNone "propagate_carrier" |> orEmptyString
                reader.intOrNone "warehouse_id" |> StockWarehouse.exportId
                reader.intOrNone "propagate_warehouse_id" |> StockWarehouse.exportId
                reader.text "auto"

                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportLocation (modelName : string) =

        let header = addStampHeadersTo [
            "id/.id" ; "id" ; "name" ; "complete_name" ; "active" ; "usage" ; "location_id/.id" ;
            "comment" ; "posx" ; "posy" ; "posz" ; "parent_path" ; "company_id/.id" ;
            "scrap_location" ; "return_location" ; "removal_strategy_id/id" ; "barcode" ;
            "cyclic_inventory_frequency" ; "last_inventory_date" ; "next_inventory_date" ;
            "storage_category_id/id" ; "valuation_in_account_id/id" ; "valuation_out_account_id/id" ;
            "allow_negative_stock"
        ]

        let sql = """
            with
            rel_removal_strategy as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'product.removal'
                and module not like '\_\_%'
            ),
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            )
            select rl.module as module, rl.external_id as location_external_id,
                   rrs.external_id as removal_external_id,
                   sl.*
            from stock_location as sl
            left join rel_location as rl on sl.id = rl.id
            left join rel_removal_strategy as rrs on sl.id = rrs.id
            order by id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> string

                match reader.textOrNone "location_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "id" |> Some |> StockLocation.exportId

                reader.text "name"
                reader.text "complete_name"
                reader.bool "active" |> string
                reader.text "usage"
                reader.intOrNone "location_id" |> orEmptyString
                reader.textOrNone "comment" |> orEmptyString
                reader.int "posx" |> string
                reader.int "posy" |> string
                reader.int "posz" |> string
                reader.text "parent_path"
                reader.intOrNone "company_id" |> orEmptyString
                reader.boolOrNone "scrap_location" |> orEmptyString
                reader.boolOrNone "return_location" |> orEmptyString
                reader.textOrNone "removal_external_id" |> orEmptyString
                reader.textOrNone "barcode" |> orEmptyString
                reader.intOrNone "cyclic_inventory_frequency" |> orEmptyString
                reader.dateOnlyOrNone "last_inventory_date" |> dateOrEmptyString
                reader.dateOnlyOrNone "next_inventory_date" |> dateOrEmptyString
                reader.intOrNone "storage_category_id" |> orEmptyString
                reader.intOrNone "valuation_in_account_id" |> AccountAccount.exportId
                reader.intOrNone "valuation_out_account_id" |> AccountAccount.exportId
                reader.boolOrNone "allow_negative_stock" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPickingType (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "color" ; "sequence" ; "sequence_id/id" ; "sequence_code" ; "default_location_src_id/id"
            "default_location_dest_id/id" ; "code" ; "return_picking_type_id/id" ; "show_entire_packs"
            "warehouse_id/id" ; "active" ; "use_create_lots" ; "use_existing_lots" ; "print_label"
            "show_operations" ; "show_reserved" ; "reservation_method" ; "reservation_days_before"
            "reservation_days_before_priority" ; "barcode" ; "company_id/id"
        ]

        let sql = """
            with
			rel_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
			),
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            )
            select rls.external_id as def_loc_src_ext_id, rld.external_id as def_loc_dest_ext_id,
                   rpt.external_id as external_id,
                   spt.*
            from stock_picking_type as spt
            left join rel_location as rls on spt.default_location_src_id = rls.id
            left join rel_location as rld on spt.default_location_dest_id = rld.id
            left join rel_picking_type as rpt on spt.id = rpt.id
            order by spt.id
            """

        let readerFun (reader : RowReader) =
            [
                match reader.textOrNone "external_id" with
                | Some externalId -> externalId
                | None -> reader.int "id" |> Some |> StockPickingType.exportId

                reader.text "name"
                reader.int "color" |> string
                reader.int "sequence" |> string
                reader.int "sequence_id" |> Some |> IrSequence.exportId
                reader.text "sequence_code"

                match reader.textOrNone "def_loc_src_ext_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "default_location_src_id" |> StockLocation.exportId

                match reader.textOrNone "def_loc_dest_ext_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "default_location_dest_id" |> StockLocation.exportId

                reader.textOrNone "code" |> orEmptyString
                reader.intOrNone "return_picking_type_id" |> StockPickingType.exportId
                reader.boolOrNone "show_entire_packs" |> orEmptyString
                reader.int "warehouse_id" |> Some |> StockWarehouse.exportId
                reader.bool "active" |> string
                reader.bool "use_create_lots" |> string
                reader.bool "use_existing_lots" |> string
                reader.boolOrNone "print_label" |> orEmptyString
                reader.bool "show_operations" |> string
                reader.bool "show_reserved" |> string
                reader.text "reservation_method"
                reader.intOrNone "reservation_days_before" |> orEmptyString
                reader.intOrNone "reservation_days_before_priority" |> orEmptyString
                reader.textOrNone "barcode" |> orEmptyString
                reader.int "company_id" |> string
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPicking (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "message_main_attachment_id/id" ; "name" ; "origin" ; "note" ; "backorder_id/id" ;
            "move_type" ; "state" ; "group_id/id" ; "priority" ; "scheduled_date" ; "date_deadline" ;
            "has_deadline_issue" ; "date" ; "date_done" ; "location_id/id" ; "location_dest_id/id" ;
            "picking_type_id/id" ; "partner_id/id" ; "company_id/.id" ; "user_id/id" ; "owner_id/id" ; "printed" ;
            "is_locked" ; "immediate_transfer" ; "sale_id/id"
        ]

        let sql = """
            with
			rel_stock_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            ),
			rel_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
            )
            select rsl.external_id as location_external_id,
                   rsld.external_id as location_dest_external_id,
                   rpt.external_id as picking_type_external_id,
                   sp.*
            from stock_picking as sp
            left join rel_stock_location as rsl on sp.location_id = rsl.id
            left join rel_stock_location as rsld on sp.location_dest_id = rsld.id
            left join rel_picking_type as rpt on sp.picking_type_id = rpt.id
            order by sp.create_date
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockPicking.exportId
                reader.intOrNone "message_main_attachment_id" |> IrAttachment.exportId
                reader.textOrNone "name" |> orEmptyString
                reader.textOrNone "origin" |> orEmptyString
                reader.textOrNone "note" |> orEmptyString
                reader.intOrNone "backorder_id" |> StockPicking.exportId
                reader.text "move_type"
                reader.textOrNone "state" |> orEmptyString
                reader.intOrNone "group_id" |> ProcurementGroup.exportId
                reader.textOrNone "priority" |> orEmptyString
                reader.dateTimeOrNone "scheduled_date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "date_deadline" |> dateTimeOrEmptyString
                reader.boolOrNone "has_deadline_issue" |> orEmptyString
                reader.dateTimeOrNone "date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "date_done" |> dateTimeOrEmptyString

                match reader.textOrNone "location_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "location_id" |> Some |> StockLocation.exportId

                match reader.textOrNone "location_dest_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "location_dest_id" |> Some |> StockLocation.exportId

                match reader.textOrNone "picking_type_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "picking_type_id" |> Some |> StockPickingType.exportId

                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.intOrNone "company_id" |> orEmptyString
                reader.intOrNone "user_id" |> ResUsers.exportId
                reader.intOrNone "owner_id" |> ResPartner.exportId
                reader.boolOrNone "printed" |> orEmptyString
                reader.boolOrNone "is_locked" |> orEmptyString
                reader.boolOrNone "immediate_transfer" |> orEmptyString
                reader.intOrNone "sale_id" |> SaleOrder.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportMove (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "name" ; "sequence" ; "priority" ; "date" ; "date_deadline" ; "company_id/.id"
            "product_id/id" ; "description_picking" ; "product_qty" ; "product_uom_qty" ; "product_uom/id"
            "location_id/id" ; "location_dest_id/id" ; "partner_id/id" ; "picking_id/id" ; "state"
            "price_unit" ; "origin" ; "procure_method" ; "scrapped" ; "group_id/id" ; "rule_id/id"
            "propagate_cancel" ; "delay_alert_date" ; "picking_type_id/id" ; "is_inventory"
            "origin_returned_move_id/id" ; "restrict_partner_id/id" ; "warehouse_id/id" ; "additional" ; "reference"
            "package_level_id/id" ; "next_serial" ; "next_serial_count" ; "orderpoint_id/id" ; "reservation_date"
            "product_packaging_id/id" ; "to_refund" ; "analytic_account_line_id/id" ; "sale_line_id/id"
            "purchase_line_id/id" ; "created_purchase_line_id/id"
        ]

        let sql = """
            with
			rel_picking_type as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.picking.type'
                and module not like '\_\_%'
            ),
			rel_stock_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            ),
			rel_uom_uom as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'uom.uom'
                and module not like '\_\_%'
            )
            select
                   ruom.external_id as uom_external_id,
                   rsl.external_id  as location_external_id,
                   rsld.external_id as location_dest_external_id,
                   rspt.external_id as picking_type_external_id,
                   sm.*
            from stock_move as sm
            left join rel_uom_uom as ruom on sm.product_uom = ruom.id
            left join rel_stock_location as rsl on sm.location_id = rsl.id
            left join rel_stock_location as rsld on sm.location_dest_id = rsld.id
            left join rel_picking_type as rspt on sm.picking_type_id = rspt.id
            order by sm.create_date
        """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockMove.exportId
                reader.text "name"
                reader.intOrNone "sequence" |> orEmptyString
                reader.textOrNone "priority" |> orEmptyString
                reader.dateTimeOrNone "date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "date_deadline" |> dateTimeOrEmptyString
                reader.int "company_id" |> string
                reader.int "product_id" |> Some |> ProductProduct.exportId
                reader.textOrNone "description_picking" |> orEmptyString
                reader.doubleOrNone "product_qty" |> formatDecimalOption
                reader.doubleOrNone "product_uom_qty" |> formatDecimalOption

                match reader.textOrNone "uom_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "product_uom" |> Some |> UomUom.exportId

                match reader.textOrNone "location_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "location_id" |> Some |> StockLocation.exportId

                match reader.textOrNone "location_dest_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "location_dest_id" |> Some |> StockLocation.exportId

                reader.intOrNone "partner_id" |> ResPartner.exportId
                reader.intOrNone "picking_id" |> StockPicking.exportId
                reader.textOrNone "state" |> orEmptyString
                reader.doubleOrNone "price_unit" |> formatDecimalOption
                reader.textOrNone "origin" |> orEmptyString
                reader.text "procure_method"
                reader.boolOrNone "scrapped" |> orEmptyString
                reader.intOrNone "group_id" |> ProcurementGroup.exportId
                reader.intOrNone "rule_id" |> StockRule.exportId
                reader.boolOrNone "propagate_cancel" |> orEmptyString
                reader.dateTimeOrNone "delay_alert_date" |> dateTimeOrEmptyString

                match reader.textOrNone "picking_type_external_id" with
                | Some externalId -> externalId
                | None -> reader.intOrNone "picking_type_id" |> StockPickingType.exportId

                reader.boolOrNone "is_inventory" |> orEmptyString
                reader.intOrNone "origin_returned_move_id" |> StockMove.exportId
                reader.intOrNone "restrict_partner_id" |> ResPartner.exportId
                reader.intOrNone "warehouse_id" |> StockWarehouse.exportId
                reader.boolOrNone "additional" |> orEmptyString
                reader.textOrNone "reference" |> orEmptyString
                ""      // package_level_id
                ""      // next_serial
                reader.intOrNone "next_serial_count" |> orEmptyString
                ""      // orderpoint_id
                reader.dateOnlyOrNone "reservation_date" |> dateOrEmptyString
                ""      // product_packaging_id
                reader.boolOrNone "to_refund" |> orEmptyString
                ""      // analytic_account_line_id
                reader.intOrNone "sale_line_id" |> SaleOrderLine.exportId
                reader.intOrNone "purchase_line_id" |> PurchaseOrderLine.exportId
                reader.intOrNone "created_purchase_line_id" |> PurchaseOrderLine.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportMoveLine (modelName : string) =

        failwith "No está terminado."

        let header = addStampHeadersTo [
            "id" ;
        ]

        let sql = """
            with
			rel_stock_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            )
            select rsl.external_id as location_external_id,
                   rsld.external_id as location_dest_external_id,
                   sm.*
            from stock_move as sm
            left join rel_stock_location as rsl on sm.location_id = rsl.id
            left join rel_stock_location as rsld on sm.location_dest_id = rsld.id
            order by sm.create_date
            """

        let readerFun (reader : RowReader) =
            [
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportProductionLot (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "message_main_attachment_id/id" ; "name" ; "ref" ; "product_id/id" ; "product_uom_name"
            "note" ; "company_id/.id" ; "expiration_date" ; "use_date" ; "removal_date"
            "alert_date" ; "product_expiry_reminded" ; "mostrar"
        ]

        let sql = """
            select uom.name as product_uom_name, spl.*
            from stock_production_lot as spl
            join uom_uom as uom on spl.product_uom_id = uom.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockProductionLot.exportId
                reader.intOrNone "message_main_attachment_id" |> IrAttachment.exportId
                reader.text "name"
                reader.textOrNone "ref" |> orEmptyString
                reader.intOrNone "product_id" |> ProductTemplate.exportId
                reader.text "product_uom_name"
                reader.textOrNone "note" |> orEmptyString
                reader.intOrNone "company_id" |> orEmptyString
                reader.dateTimeOrNone "expiration_date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "use_date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "removal_date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "alert_date" |> dateTimeOrEmptyString
                reader.boolOrNone "product_expiry_reminded" |> orEmptyString
                reader.boolOrNone "mostrar" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportQuant (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "product_id/id" ; "company_id/.id" ; "location_id/id" ; "lot_id/id" ; "package_id/id"
            "owner_id/id"; "quantity" ; "reserved_quantity" ; "in_date" ; "inventory_quantity"
            "inventory_diff_quantity" ; "inventory_date" ; "inventory_quantity_set" ; "user_id/id"
            "accounting_date" ; "removal_date"
        ]

        let sql = """
            with
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            )
            select rl.module as module, rl.external_id as location_external_id,
                   sq.*
            from stock_quant as sq
            left join stock_location as sl on sq.location_id = sl.id
            left join rel_location as rl on sl.id = rl.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockQuant.exportId
                reader.intOrNone "product_id" |> ProductProduct.exportId
                reader.intOrNone "company_id" |> orEmptyString

                match reader.textOrNone "location_external_id" with
                | Some externalId -> externalId
                | None -> reader.int "location_id" |> Some |> StockLocation.exportId

                // reader.intOrNone "location_id" |> StockLocation.exportId

                reader.intOrNone "lot_id" |> StockProductionLot.exportId
                ""                                                             // package_id/id
                reader.intOrNone "owner_id" |> ResPartner.exportId
                reader.doubleOrNone "quantity" |> formatDecimalOption
                reader.double "reserved_quantity" |> formatDecimal
                reader.dateTimeOrNone "in_date" |> dateTimeOrEmptyString
                reader.doubleOrNone "inventory_quantity" |> formatDecimalOption
                reader.doubleOrNone "inventory_diff_quantity" |> formatDecimalOption
                reader.dateTimeOrNone "inventory_date" |> dateTimeOrEmptyString
                reader.boolOrNone "inventory_quantity_set" |> orEmptyString
                reader.intOrNone "user_id" |> ResUsers.exportId
                reader.dateTimeOrNone "accounting_date" |> dateTimeOrEmptyString
                reader.dateTimeOrNone "removal_date" |> dateTimeOrEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportRouteProduct (modelName : string) =

        let header = [ "route_id/id" ; "product_id/id" ]

        let sql = """
            select *
            from stock_route_product
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "route_id" |> Some |> StockRoute.exportId
                reader.int "product_id" |> Some |> ProductTemplate.exportId
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportPutawayRule (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "product_id/id" ; "category_id/id" ; "location_in_id/id" ; "location_out_id/id"
            "sequence" ; "company_id/.id" ; "storage_category_id/id" ; "active"
        ]

        let sql = """
            with
            rel_location as (
                select module, model, res_id as id, module || '.' || name as external_id
                from ir_model_data
                where model = 'stock.location'
                and module not like '\_\_%'
            )
            select rli.external_id as location_in_external_id,
                   rlo.external_id as location_out_external_id,
                   spr.*
            from stock_putaway_rule as spr
            left join rel_location as rli on spr.location_in_id = rli.id
            left join rel_location as rlo on spr.location_in_id = rlo.id
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockPutawayRule.exportId
                reader.intOrNone "product_id" |> ProductProduct.exportId
                reader.intOrNone "category_id" |> ProductCategory.exportId

                match reader.textOrNone "location_in_external_id" with
                | Some location_in_external_id -> location_in_external_id
                | None -> reader.intOrNone "location_in_id" |> StockLocation.exportId

                match reader.textOrNone "location_out_external_id" with
                | Some location_out_external_id -> location_out_external_id
                | None -> reader.intOrNone "location_out_id" |> StockLocation.exportId

                reader.intOrNone "sequence" |> orEmptyString
                reader.int "company_id" |> Some |> orEmptyString
                reader.intOrNone "storage_category_id" |> orEmptyString
                reader.boolOrNone "active" |> orEmptyString
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportValuationLayer (modelName : string) =

        let header = addStampHeadersTo [
            "id" ; "company_id/.id" ; "product_id/id" ; "quantity" ; "unit_cost" ; "value" ; "remaining_qty"
            "remaining_value" ; "description" ; "stock_valuation_layer_id/id" ; "stock_move_id/id"
            "account_move_id/id"
        ]

        let sql = """
            select *
            from stock_valuation_layer
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "id" |> Some |> StockValuationLayer.exportId
                reader.int "company_id" |> Some |> orEmptyString
                reader.int "product_id" |> Some |> ProductProduct.exportId
                reader.doubleOrNone "quantity" |> formatDecimalOption
                reader.doubleOrNone "unit_cost" |> formatDecimalOption
                reader.doubleOrNone "value" |> formatDecimalOption
                reader.doubleOrNone "remaining_qty" |> formatDecimalOption
                reader.doubleOrNone "remaining_value" |> formatDecimalOption
                reader.textOrNone "description" |> orEmptyString
                reader.intOrNone "stock_valuation_layer_id" |> StockValuationLayer.exportId
                reader.intOrNone "stock_move_id" |> StockMove.exportId
                reader.intOrNone "account_move_id" |> AccountMove.exportId
                yield! readStampFields reader
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------

    //------------------------------------------------------------------------------------------------------------------
    static member exportWhResupplyTable (modelName : string) =

        let header = [ "supplied_wh_id" ; "supplier_wh_id" ]

        let sql = """
            select *
            from stock_wh_resupply_table
            """

        let readerFun (reader : RowReader) =
            [
                reader.int "supplied_wh_id" |> Some |> StockWarehouse.exportId
                reader.int "supplier_wh_id" |> Some |> StockWarehouse.exportId
            ]

        header::ISqlBroker.getExportData sql readerFun
        |> IExcelBroker.exportFile $"{modelName}.xlsx"
    //------------------------------------------------------------------------------------------------------------------
