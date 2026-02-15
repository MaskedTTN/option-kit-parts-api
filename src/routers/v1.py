from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional

from models import (
    Vehicle,
    MainGroupDefinition,
    SubGroupDefinition,
    VehicleMainGroup,
    VehicleSubGroup,
    Diagram,
    Part,
    PartWithContext,
    DiagramWithParts,
    VehicleSubGroupWithDiagrams,
    VehicleMainGroupWithSubGroups,
    PartSearchResult,
    MainGroupNested,
    VehicleOrder,
)

from services.db import get_db

router = APIRouter()


@router.get("/vehicles", response_model=List[Vehicle])
def get_vehicles():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vehicles ORDER BY created_at DESC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


@router.get("/vehicles/{vid}", response_model=Vehicle)
def get_vehicle(vid: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vehicles WHERE vid = ?", (vid,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        return dict(row)


@router.get("/vehicles/{vid}/complete", response_model=List[MainGroupNested])
def get_vehicle_complete_structure(vid: str, vehicleOrder: VehicleOrder):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vehicles WHERE vid = ?", (vid,))
        vehicle = cursor.fetchone()
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")

        result = []
        cursor.execute("""
            SELECT vmg.id as vmg_id, vmg.mg_number, mgd.mg_name, vmg.url
            FROM vehicle_main_groups vmg
            JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
            WHERE vmg.vid = ?
            ORDER BY CAST(vmg.mg_number AS INTEGER)
        """, (vid,))
        main_groups = cursor.fetchall()

        for mg_row in main_groups:
            mg_dict = dict(mg_row)
            mg_id = mg_dict['vmg_id']

            cursor.execute("""
                SELECT vsg.id as vsg_id, sgd.sg_number, sgd.sg_name
                FROM vehicle_subgroups vsg
                JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
                WHERE vsg.vehicle_mg_id = ?
                ORDER BY CAST(sgd.sg_number AS INTEGER)
            """, (mg_id,))

            subgroups = cursor.fetchall()
            sg_list = []

            for sg_row in subgroups:
                sg_dict = dict(sg_row)
                vsg_id = sg_dict['vsg_id']

                cursor.execute("""
                    SELECT id, diagram_id, title, url
                    FROM diagrams
                    WHERE vehicle_subgroup_id = ?
                    ORDER BY title
                """, (vsg_id,))
                diagrams = cursor.fetchall()
                diag_list = []

                for diag_row in diagrams:
                    diag_dict = dict(diag_row)
                    diag_db_id = diag_dict['id']

                    cursor.execute("""
                        SELECT id, position, description, part_number, quantity,
                               supplement, from_date, up_to_date, price, notes,
                               option_requirements, option_codes
                        FROM parts
                        WHERE diagram_id = ?
                        ORDER BY CAST(position AS INTEGER)
                    """, (diag_db_id,))

                    parts = []
                    part_rows = cursor.fetchall()

                    for part_row in part_rows:
                        part_dict = dict(part_row)
                        if part_dict.get('option_codes'):
                            part_option_codes = {}
                            for code in part_dict['option_codes'].split(' '):
                                code_split = code.strip().split('=')
                                if len(code_split) == 2:
                                    part_option_codes[code_split[0]] = code_split[1]
                            vehicleOrder_codes = [code.code for code in vehicleOrder.order_codes]
                            addPart = True
                            for code, val in part_option_codes.items():
                                if code in vehicleOrder_codes:
                                    if val == "Yes":
                                        continue
                                    elif val == "No":
                                        addPart = False
                                        break
                            if not addPart:
                                continue
                        parts.append(part_dict)

                    diag_list.append({
                        'id': diag_dict['id'],
                        'diagram_id': diag_dict['diagram_id'],
                        'title': diag_dict['title'],
                        'url': diag_dict['url'],
                        'parts': parts
                    })

                sg_list.append({
                    'sg_number': sg_dict['sg_number'],
                    'sg_name': sg_dict['sg_name'],
                    'diagrams': diag_list
                })

            result.append({
                'mg_number': mg_dict['mg_number'],
                'mg_name': mg_dict['mg_name'],
                'url': mg_dict['url'],
                'subgroups': sg_list
            })

        return result


@router.get("/vehicles/{vid}/complete/summary")
def get_vehicle_complete_summary(vid: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vehicles WHERE vid = ?", (vid,))
        vehicle = cursor.fetchone()
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")

        cursor.execute("SELECT COUNT(*) as count FROM vehicle_main_groups WHERE vid = ?", (vid,))
        mg_count = cursor.fetchone()['count']

        cursor.execute("""
            SELECT COUNT(*) as count
            FROM vehicle_subgroups vsg
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            WHERE vmg.vid = ?
        """, (vid,))
        sg_count = cursor.fetchone()['count']

        cursor.execute("""
            SELECT COUNT(*) as count
            FROM diagrams d
            JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            WHERE vmg.vid = ?
        """, (vid,))
        diag_count = cursor.fetchone()['count']

        cursor.execute("""
            SELECT COUNT(*) as count
            FROM parts p
            JOIN diagrams d ON p.diagram_id = d.id
            JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            WHERE vmg.vid = ?
        """, (vid,))
        parts_count = cursor.fetchone()['count']

        cursor.execute("""
            SELECT COUNT(DISTINCT p.part_number) as count
            FROM parts p
            JOIN diagrams d ON p.diagram_id = d.id
            JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            WHERE vmg.vid = ? AND p.part_number != ''
        """, (vid,))
        unique_parts = cursor.fetchone()['count']

        return {
            "vid": vid,
            "vehicle": dict(vehicle),
            "main_groups": mg_count,
            "subgroups": sg_count,
            "diagrams": diag_count,
            "total_parts": parts_count,
            "unique_part_numbers": unique_parts
        }


@router.get("/main-groups/definitions", response_model=List[MainGroupDefinition])
def get_main_group_definitions():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT mg_number, mg_name, description FROM main_group_definitions ORDER BY CAST(mg_number AS INTEGER)")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


@router.get("/main-groups/definitions/{mg_number}", response_model=MainGroupDefinition)
def get_main_group_definition(mg_number: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT mg_number, mg_name, description FROM main_group_definitions WHERE mg_number = ?", (mg_number,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Main group definition not found")
        return dict(row)


@router.get("/subgroups/definitions", response_model=List[SubGroupDefinition])
def get_subgroup_definitions(mg_number: Optional[str] = None):
    with get_db() as conn:
        cursor = conn.cursor()
        if mg_number:
            cursor.execute("SELECT id, mg_number, sg_number, sg_name FROM subgroup_definitions WHERE mg_number = ? ORDER BY CAST(sg_number AS INTEGER)", (mg_number,))
        else:
            cursor.execute("SELECT id, mg_number, sg_number, sg_name FROM subgroup_definitions ORDER BY CAST(mg_number AS INTEGER), CAST(sg_number AS INTEGER)")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


@router.get("/vehicles/{vid}/main-groups", response_model=List[VehicleMainGroup])
def get_vehicle_main_groups(vid: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT vmg.id, vmg.vid, vmg.mg_number, vmg.url, mgd.mg_name
            FROM vehicle_main_groups vmg
            JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
            WHERE vmg.vid = ?
            ORDER BY CAST(vmg.mg_number AS INTEGER)
        """, (vid,))
        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No main groups found for this vehicle")
        return [dict(row) for row in rows]


@router.get("/vehicles/{vid}/main-groups/{mg_number}", response_model=VehicleMainGroup)
def get_vehicle_main_group(vid: str, mg_number: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT vmg.id, vmg.vid, vmg.mg_number, vmg.url, mgd.mg_name
            FROM vehicle_main_groups vmg
            JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
            WHERE vmg.vid = ? AND vmg.mg_number = ?
        """, (vid, mg_number))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Main group not found for this vehicle")
        return dict(row)


@router.get("/vehicles/{vid}/main-groups/{mg_number}/full", response_model=VehicleMainGroupWithSubGroups)
def get_vehicle_main_group_full(vid: str, mg_number: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT vmg.id, vmg.vid, vmg.mg_number, vmg.url, mgd.mg_name
            FROM vehicle_main_groups vmg
            JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
            WHERE vmg.vid = ? AND vmg.mg_number = ?
        """, (vid, mg_number))
        mg_row = cursor.fetchone()
        if not mg_row:
            raise HTTPException(status_code=404, detail="Main group not found for this vehicle")
        mg = dict(mg_row)
        cursor.execute("""
            SELECT vsg.id, vsg.vehicle_mg_id, sgd.sg_number, sgd.sg_name
            FROM vehicle_subgroups vsg
            JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
            WHERE vsg.vehicle_mg_id = ?
            ORDER BY CAST(sgd.sg_number AS INTEGER)
        """, (mg['id'],))
        sg_rows = cursor.fetchall()
        mg['subgroups'] = [dict(row) for row in sg_rows]
        return mg


@router.get("/vehicles/{vid}/main-groups/{mg_number}/subgroups", response_model=List[VehicleSubGroup])
def get_vehicle_subgroups(vid: str, mg_number: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT vsg.id, vsg.vehicle_mg_id, sgd.sg_number, sgd.sg_name
            FROM vehicle_subgroups vsg
            JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            WHERE vmg.vid = ? AND vmg.mg_number = ?
            ORDER BY CAST(sgd.sg_number AS INTEGER)
        """, (vid, mg_number))
        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No subgroups found")
        return [dict(row) for row in rows]


@router.get("/vehicle-subgroups/{vsg_id}", response_model=VehicleSubGroup)
def get_vehicle_subgroup(vsg_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT vsg.id, vsg.vehicle_mg_id, sgd.sg_number, sgd.sg_name
            FROM vehicle_subgroups vsg
            JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
            WHERE vsg.id = ?
        """, (vsg_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Vehicle subgroup not found")
        return dict(row)


@router.get("/vehicle-subgroups/{vsg_id}/full", response_model=VehicleSubGroupWithDiagrams)
def get_vehicle_subgroup_full(vsg_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT vsg.id, vsg.vehicle_mg_id, sgd.sg_number, sgd.sg_name
            FROM vehicle_subgroups vsg
            JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
            WHERE vsg.id = ?
        """, (vsg_id,))
        sg_row = cursor.fetchone()
        if not sg_row:
            raise HTTPException(status_code=404, detail="Vehicle subgroup not found")
        sg = dict(sg_row)
        cursor.execute("SELECT id, diagram_id, title, url FROM diagrams WHERE vehicle_subgroup_id = ?", (vsg_id,))
        diag_rows = cursor.fetchall()
        sg['diagrams'] = [dict(row) for row in diag_rows]
        return sg


@router.get("/vehicle-subgroups/{vsg_id}/diagrams", response_model=List[Diagram])
def get_diagrams(vsg_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, diagram_id, title, url FROM diagrams WHERE vehicle_subgroup_id = ?", (vsg_id,))
        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No diagrams found")
        return [dict(row) for row in rows]


@router.get("/diagrams/{diagram_db_id}", response_model=Diagram)
def get_diagram(diagram_db_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM diagrams WHERE id = ?", (diagram_db_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Diagram not found")
        return dict(row)


@router.get("/diagrams/{diagram_db_id}/full", response_model=DiagramWithParts)
def get_diagram_full(diagram_db_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM diagrams WHERE id = ?", (diagram_db_id,))
        diag_row = cursor.fetchone()
        if not diag_row:
            raise HTTPException(status_code=404, detail="Diagram not found")
        diag = dict(diag_row)
        cursor.execute("""
            SELECT id, position, description, part_number, quantity,
                   supplement, from_date, up_to_date, price, notes,
                   option_requirements, option_codes
            FROM parts 
            WHERE diagram_id = ?
            ORDER BY CAST(position AS INTEGER)
        """, (diagram_db_id,))
        part_rows = cursor.fetchall()
        diag['parts'] = [dict(row) for row in part_rows]
        return diag


@router.get("/diagrams/{diagram_db_id}/parts", response_model=List[Part])
def get_parts(diagram_db_id: int):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, position, description, part_number, quantity,
                   supplement, from_date, up_to_date, price, notes,
                   option_requirements, option_codes
            FROM parts 
            WHERE diagram_id = ?
            ORDER BY CAST(position AS INTEGER)
        """, (diagram_db_id,))
        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No parts found")
        return [dict(row) for row in rows]


@router.get("/parts/search", response_model=PartSearchResult)
def search_parts(
    q: str = Query(..., min_length=3, description="Search query"),
    vid: Optional[str] = Query(None, description="Filter by vehicle VID"),
    limit: int = Query(50, ge=1, le=500, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    with get_db() as conn:
        cursor = conn.cursor()
        search_term = f"%{q}%"
        if vid:
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM parts p
                JOIN diagrams d ON p.diagram_id = d.id
                JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
                JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
                WHERE (p.description LIKE ? OR p.part_number LIKE ?)
                AND vmg.vid = ?
            """, (search_term, search_term, vid))
            total = cursor.fetchone()['total']
            cursor.execute("""
                SELECT 
                    p.id, p.position, p.description, p.part_number, p.quantity,
                    p.supplement, p.from_date, p.up_to_date, p.price, p.notes,
                    p.option_requirements, p.option_codes,
                    d.title as diagram_title, d.diagram_id,
                    sgd.sg_name, mgd.mg_name, vmg.vid as vehicle_vid
                FROM parts p
                JOIN diagrams d ON p.diagram_id = d.id
                JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
                JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
                JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
                JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
                WHERE (p.description LIKE ? OR p.part_number LIKE ?)
                AND vmg.vid = ?
                LIMIT ? OFFSET ?
            """, (search_term, search_term, vid, limit, offset))
        else:
            cursor.execute("SELECT COUNT(*) as total FROM parts p WHERE p.description LIKE ? OR p.part_number LIKE ?", (search_term, search_term))
            total = cursor.fetchone()['total']
            cursor.execute("""
                SELECT 
                    p.id, p.position, p.description, p.part_number, p.quantity,
                    p.supplement, p.from_date, p.up_to_date, p.price, p.notes,
                    p.option_requirements, p.option_codes,
                    d.title as diagram_title, d.diagram_id,
                    sgd.sg_name, mgd.mg_name, vmg.vid as vehicle_vid
                FROM parts p
                JOIN diagrams d ON p.diagram_id = d.id
                JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
                JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
                JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
                JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
                WHERE p.description LIKE ? OR p.part_number LIKE ?
                LIMIT ? OFFSET ?
            """, (search_term, search_term, limit, offset))
        rows = cursor.fetchall()
        return {"total": total, "parts": [dict(row) for row in rows]}


@router.get("/parts/{part_number}", response_model=List[PartWithContext])
def get_part_by_number(part_number: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                p.id, p.position, p.description, p.part_number, p.quantity,
                p.supplement, p.from_date, p.up_to_date, p.price, p.notes,
                p.option_requirements, p.option_codes,
                d.title as diagram_title, d.diagram_id,
                sgd.sg_name, mgd.mg_name,
                vmg.vid as vehicle_vid
            FROM parts p
            JOIN diagrams d ON p.diagram_id = d.id
            JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
            JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
            WHERE p.part_number = ?
        """, (part_number,))
        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="Part not found")
        return [dict(row) for row in rows]


@router.get("/options/{option_code}", response_model=List[PartWithContext])
def get_parts_by_option(option_code: str, vid: Optional[str] = None):
    with get_db() as conn:
        cursor = conn.cursor()
        if vid:
            cursor.execute("""
                SELECT 
                    p.id, p.position, p.description, p.part_number, p.quantity,
                    p.supplement, p.from_date, p.up_to_date, p.price, p.notes,
                    p.option_requirements, p.option_codes,
                    d.title as diagram_title, d.diagram_id,
                    sgd.sg_name, mgd.mg_name, vmg.vid as vehicle_vid
                FROM parts p
                JOIN diagrams d ON p.diagram_id = d.id
                JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
                JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
                JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
                JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
                WHERE p.option_codes LIKE ? AND vmg.vid = ?
            """, (f"%{option_code}%", vid))
        else:
            cursor.execute("""
                SELECT 
                    p.id, p.position, p.description, p.part_number, p.quantity,
                    p.supplement, p.from_date, p.up_to_date, p.price, p.notes,
                    p.option_requirements, p.option_codes,
                    d.title as diagram_title, d.diagram_id,
                    sgd.sg_name, mgd.mg_name, vmg.vid as vehicle_vid
                FROM parts p
                JOIN diagrams d ON p.diagram_id = d.id
                JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
                JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
                JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
                JOIN main_group_definitions mgd ON vmg.mg_number = mgd.mg_number
                WHERE p.option_codes LIKE ?
            """, (f"%{option_code}%",))
        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No parts found for this option")
        return [dict(row) for row in rows]


@router.get("/main-groups/{mg_number}/vehicles", response_model=List[Vehicle])
def get_vehicles_with_main_group(mg_number: str):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT v.*
            FROM vehicles v
            JOIN vehicle_main_groups vmg ON v.vid = vmg.vid
            WHERE vmg.mg_number = ?
            ORDER BY v.created_at DESC
        """, (mg_number,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


@router.get("/subgroups/{sg_number}/vehicles", response_model=List[Vehicle])
def get_vehicles_with_subgroup(sg_number: str, mg_number: Optional[str] = None):
    with get_db() as conn:
        cursor = conn.cursor()
        if mg_number:
            cursor.execute("""
                SELECT DISTINCT v.*
                FROM vehicles v
                JOIN vehicle_main_groups vmg ON v.vid = vmg.vid
                JOIN vehicle_subgroups vsg ON vmg.id = vsg.vehicle_mg_id
                JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
                WHERE sgd.sg_number = ? AND sgd.mg_number = ?
                ORDER BY v.created_at DESC
            """, (sg_number, mg_number))
        else:
            cursor.execute("""
                SELECT DISTINCT v.*
                FROM vehicles v
                JOIN vehicle_main_groups vmg ON v.vid = vmg.vid
                JOIN vehicle_subgroups vsg ON vmg.id = vsg.vehicle_mg_id
                JOIN subgroup_definitions sgd ON vsg.sg_definition_id = sgd.id
                WHERE sgd.sg_number = ?
                ORDER BY v.created_at DESC
            """, (sg_number,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]


@router.get("/stats")
def get_statistics():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM vehicles")
        vehicles = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM main_group_definitions")
        mg_definitions = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM subgroup_definitions")
        sg_definitions = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM vehicle_main_groups")
        vehicle_mgs = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM vehicle_subgroups")
        vehicle_sgs = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM diagrams")
        diagrams = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM parts")
        parts = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(DISTINCT part_number) as count FROM parts WHERE part_number != ''")
        unique_parts = cursor.fetchone()['count']
        return {
            "vehicles": vehicles,
            "main_group_definitions": mg_definitions,
            "subgroup_definitions": sg_definitions,
            "vehicle_main_groups": vehicle_mgs,
            "vehicle_subgroups": vehicle_sgs,
            "diagrams": diagrams,
            "parts": parts,
            "unique_part_numbers": unique_parts
        }
