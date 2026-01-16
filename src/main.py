# parts_api.py

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import sqlite3
from contextlib import contextmanager
import uvicorn

# ============================================================================
# PYDANTIC MODELS (API Response Models)
# ============================================================================

class Vehicle(BaseModel):
    vid: str
    series: str
    body: str
    model: str
    market: str
    prod_month: str
    engine: str
    steering: str
    created_at: str

class MainGroupDefinition(BaseModel):
    mg_number: str
    mg_name: str
    description: Optional[str] = None

class SubGroupDefinition(BaseModel):
    id: int
    mg_number: str
    sg_number: str
    sg_name: str

class VehicleMainGroup(BaseModel):
    id: int
    vid: str
    mg_number: str
    mg_name: str
    url: str

class VehicleSubGroup(BaseModel):
    id: int
    vehicle_mg_id: int
    sg_number: str
    sg_name: str

class Diagram(BaseModel):
    id: int
    diagram_id: str
    title: str
    url: str

class Part(BaseModel):
    id: int
    position: str
    description: str
    part_number: str
    quantity: str
    supplement: str
    from_date: str
    up_to_date: str
    price: str
    notes: str
    option_requirements: Optional[str]
    option_codes: Optional[str]

class PartWithContext(Part):
    diagram_title: Optional[str] = None
    diagram_id: Optional[str] = None
    sg_name: Optional[str] = None
    mg_name: Optional[str] = None
    vehicle_vid: Optional[str] = None

class DiagramWithParts(Diagram):
    parts: List[Part] = []

class VehicleSubGroupWithDiagrams(VehicleSubGroup):
    diagrams: List[Diagram] = []

class VehicleMainGroupWithSubGroups(VehicleMainGroup):
    subgroups: List[VehicleSubGroup] = []

class PartSearchResult(BaseModel):
    total: int
    parts: List[PartWithContext]



class PartNested(BaseModel):
    id: int
    position: str
    description: str
    part_number: str
    quantity: str
    supplement: str
    from_date: str
    up_to_date: str
    price: str
    notes: str
    option_requirements: Optional[str]
    option_codes: Optional[str]

class DiagramNested(BaseModel):
    id: int
    diagram_id: str
    title: str
    url: str
    parts: List[PartNested] = []

class SubGroupNested(BaseModel):
    sg_number: str
    sg_name: str
    diagrams: List[DiagramNested] = []

class MainGroupNested(BaseModel):
    mg_number: str
    mg_name: str
    url: str
    subgroups: List[SubGroupNested] = []

class OptionCode(BaseModel):
    code: str
    description: Optional[str] = None

class VehicleOrder(BaseModel):
    vid: str
    order_codes: List[OptionCode]

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

DB_PATH = "bmw_parts.db"

@contextmanager
def get_db():
    """Context manager for database connections"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(
    title="BMW Parts API",
    description="RESTful API for BMW Parts Catalog (Normalized Schema)",
    version="2.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/")
def root():
    """API root endpoint"""
    return {
        "message": "BMW Parts API (Normalized Schema)",
        "version": "2.0.0",
        "endpoints": {
            "vehicles": "/vehicles",
            "vehicle_main_groups": "/vehicles/{vid}/main-groups",
            "vehicle_subgroups": "/vehicles/{vid}/main-groups/{mg_number}/subgroups",
            "diagrams": "/vehicles/{vid}/subgroups/{vsg_id}/diagrams",
            "parts": "/diagrams/{diagram_id}/parts",
            "search": "/parts/search?q={query}",
            "part_lookup": "/parts/{part_number}",
            "mg_definitions": "/main-groups/definitions",
            "sg_definitions": "/subgroups/definitions"
        }
    }

# ============================================================================
# VEHICLE ENDPOINTS
# ============================================================================

@app.get("/vehicles", response_model=List[Vehicle])
def get_vehicles():
    """Get all vehicles in the database"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vehicles ORDER BY created_at DESC")
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

@app.get("/vehicles/{vid}", response_model=Vehicle)
def get_vehicle(vid: str):
    """Get a specific vehicle by VID"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vehicles WHERE vid = ?", (vid,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        return dict(row)
    
@app.get("/vehicles/{vid}/complete", response_model=List[MainGroupNested])
def get_vehicle_complete_structure(vid: str, vehicleOrder: VehicleOrder):
    """
    Get the complete hierarchical structure for a vehicle:
    Main Groups -> Sub Groups -> Diagrams -> Parts
    
    Returns everything organized in a nested structure
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verify vehicle exists
        cursor.execute("SELECT * FROM vehicles WHERE vid = ?", (vid,))
        vehicle = cursor.fetchone()
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        
        result = []
        
        # Get all main groups for this vehicle
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
            
            # Get subgroups for this main group
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
                
                # Get diagrams for this subgroup
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
                    
                    # Get parts for this diagram
                    cursor.execute("""
                        SELECT id, position, description, part_number, quantity,
                               supplement, from_date, up_to_date, price, notes,
                               option_requirements, option_codes
                        FROM parts
                        WHERE diagram_id = ?
                        ORDER BY CAST(position AS INTEGER)
                    """, (diag_db_id,))
                    
                    #parts = [dict(part_row) for part_row in cursor.fetchall()]
                    parts = []
                    part_rows = cursor.fetchall()

                    for part_row in part_rows:
                        part_dict = dict(part_row)
                        #if not part_dict['option_requirements'] is None:
                        #    part_dict['option_requirements'] = part_dict['option_requirements'].split(',')
                        if not part_dict['option_codes'] is None:
                            print("part has option codes")
                            #part_dict['option_codes'] = part_dict['option_codes'].split(',')
                            print(f"Part {part_dict['description']} {part_dict['part_number']} option codes: {part_dict['option_codes']}")
                            #need part option codes as dict
                            part_option_codes = {}
                            for code in part_dict['option_codes'].split(' '):
                                code_split = code.strip().split('=')
                                if len(code_split) == 2:
                                    part_option_codes[code_split[0]] = code_split[1]
                            #part_option_codes = [code.strip() for code in part_dict['option_codes'].split(' ')]
                            print(f"Parsed option codes: {part_option_codes}") #Parsed option codes: ['S601A=Yes', 'S609A=No', 'S6VAA=No']
                            vehicleOrder_codes = [code.code for code in vehicleOrder.order_codes]
                            print(f"Vehicle order option codes: {vehicleOrder_codes}")
                            #partcodes = {
                            #   
                            #}
                            #Vehicle order option codes: ['S4M5A=Yes', 'S610A=Yes']
                            addPart = True
                            for code, val in part_option_codes.items():
                                if code in vehicleOrder_codes:
                                    if val == "Yes":
                                        print(f"Part requires option {code}=Yes and vehicle has it. Keeping part.")
                                        continue
                                    elif val == "No":
                                        print(f"Part requires option {code}=No but vehicle has it. Skipping part.")
                                        addPart = False
                                        break
                                
                            if not addPart:
                                continue  # Skip adding this part
                                    
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

@app.get("/vehicles/{vid}/complete/summary")
def get_vehicle_complete_summary(vid: str):
    """
    Get a summary count of the complete structure
    """
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Verify vehicle exists
        cursor.execute("SELECT * FROM vehicles WHERE vid = ?", (vid,))
        vehicle = cursor.fetchone()
        if not vehicle:
            raise HTTPException(status_code=404, detail="Vehicle not found")
        
        # Count main groups
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM vehicle_main_groups
            WHERE vid = ?
        """, (vid,))
        mg_count = cursor.fetchone()['count']
        
        # Count subgroups
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM vehicle_subgroups vsg
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            WHERE vmg.vid = ?
        """, (vid,))
        sg_count = cursor.fetchone()['count']
        
        # Count diagrams
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM diagrams d
            JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            WHERE vmg.vid = ?
        """, (vid,))
        diag_count = cursor.fetchone()['count']
        
        # Count parts
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM parts p
            JOIN diagrams d ON p.diagram_id = d.id
            JOIN vehicle_subgroups vsg ON d.vehicle_subgroup_id = vsg.id
            JOIN vehicle_main_groups vmg ON vsg.vehicle_mg_id = vmg.id
            WHERE vmg.vid = ?
        """, (vid,))
        parts_count = cursor.fetchone()['count']
        
        # Count unique part numbers
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

# ============================================================================
# MAIN GROUP DEFINITION ENDPOINTS
# ============================================================================

@app.get("/main-groups/definitions", response_model=List[MainGroupDefinition])
def get_main_group_definitions():
    """Get all main group definitions (shared categories)"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT mg_number, mg_name, description 
            FROM main_group_definitions 
            ORDER BY CAST(mg_number AS INTEGER)
        """)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

@app.get("/main-groups/definitions/{mg_number}", response_model=MainGroupDefinition)
def get_main_group_definition(mg_number: str):
    """Get a specific main group definition"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT mg_number, mg_name, description 
            FROM main_group_definitions 
            WHERE mg_number = ?
        """, (mg_number,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Main group definition not found")
        return dict(row)

# ============================================================================
# SUBGROUP DEFINITION ENDPOINTS
# ============================================================================

@app.get("/subgroups/definitions", response_model=List[SubGroupDefinition])
def get_subgroup_definitions(mg_number: Optional[str] = None):
    """Get all subgroup definitions, optionally filtered by main group"""
    with get_db() as conn:
        cursor = conn.cursor()
        if mg_number:
            cursor.execute("""
                SELECT id, mg_number, sg_number, sg_name 
                FROM subgroup_definitions 
                WHERE mg_number = ?
                ORDER BY CAST(sg_number AS INTEGER)
            """, (mg_number,))
        else:
            cursor.execute("""
                SELECT id, mg_number, sg_number, sg_name 
                FROM subgroup_definitions 
                ORDER BY CAST(mg_number AS INTEGER), CAST(sg_number AS INTEGER)
            """)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

# ============================================================================
# VEHICLE MAIN GROUP ENDPOINTS
# ============================================================================

@app.get("/vehicles/{vid}/main-groups", response_model=List[VehicleMainGroup])
def get_vehicle_main_groups(vid: str):
    """Get all main groups for a specific vehicle"""
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

@app.get("/vehicles/{vid}/main-groups/{mg_number}", response_model=VehicleMainGroup)
def get_vehicle_main_group(vid: str, mg_number: str):
    """Get a specific main group for a vehicle"""
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

@app.get("/vehicles/{vid}/main-groups/{mg_number}/full", response_model=VehicleMainGroupWithSubGroups)
def get_vehicle_main_group_full(vid: str, mg_number: str):
    """Get main group with all its subgroups for a specific vehicle"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get vehicle main group
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
        
        # Get subgroups
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

# ============================================================================
# VEHICLE SUBGROUP ENDPOINTS
# ============================================================================

@app.get("/vehicles/{vid}/main-groups/{mg_number}/subgroups", response_model=List[VehicleSubGroup])
def get_vehicle_subgroups(vid: str, mg_number: str):
    """Get all subgroups for a vehicle's main group"""
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

@app.get("/vehicle-subgroups/{vsg_id}", response_model=VehicleSubGroup)
def get_vehicle_subgroup(vsg_id: int):
    """Get a specific vehicle subgroup by ID"""
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

@app.get("/vehicle-subgroups/{vsg_id}/full", response_model=VehicleSubGroupWithDiagrams)
def get_vehicle_subgroup_full(vsg_id: int):
    """Get vehicle subgroup with all its diagrams"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get vehicle subgroup
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
        
        # Get diagrams
        cursor.execute("""
            SELECT id, diagram_id, title, url 
            FROM diagrams 
            WHERE vehicle_subgroup_id = ?
        """, (vsg_id,))
        diag_rows = cursor.fetchall()
        
        sg['diagrams'] = [dict(row) for row in diag_rows]
        return sg

# ============================================================================
# DIAGRAM ENDPOINTS
# ============================================================================

@app.get("/vehicle-subgroups/{vsg_id}/diagrams", response_model=List[Diagram])
def get_diagrams(vsg_id: int):
    """Get all diagrams for a vehicle subgroup"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, diagram_id, title, url 
            FROM diagrams 
            WHERE vehicle_subgroup_id = ?
        """, (vsg_id,))
        rows = cursor.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="No diagrams found")
        return [dict(row) for row in rows]

@app.get("/diagrams/{diagram_db_id}", response_model=Diagram)
def get_diagram(diagram_db_id: int):
    """Get a specific diagram by database ID"""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM diagrams WHERE id = ?", (diagram_db_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Diagram not found")
        return dict(row)

@app.get("/diagrams/{diagram_db_id}/full", response_model=DiagramWithParts)
def get_diagram_full(diagram_db_id: int):
    """Get diagram with all its parts"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Get diagram
        cursor.execute("SELECT * FROM diagrams WHERE id = ?", (diagram_db_id,))
        diag_row = cursor.fetchone()
        if not diag_row:
            raise HTTPException(status_code=404, detail="Diagram not found")
        
        diag = dict(diag_row)
        
        # Get parts
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

# ============================================================================
# PARTS ENDPOINTS
# ============================================================================

@app.get("/diagrams/{diagram_db_id}/parts", response_model=List[Part])
def get_parts(diagram_db_id: int):
    """Get all parts for a diagram"""
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

@app.get("/parts/{part_number}", response_model=List[PartWithContext])
def get_part_by_number(part_number: str):
    """Get all occurrences of a part number with full context"""
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

@app.get("/parts/search", response_model=PartSearchResult)
def search_parts(
    q: str = Query(..., min_length=3, description="Search query"),
    vid: Optional[str] = Query(None, description="Filter by vehicle VID"),
    limit: int = Query(50, ge=1, le=500, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """Search parts by description or part number, optionally filtered by vehicle"""
    with get_db() as conn:
        cursor = conn.cursor()
        
        search_term = f"%{q}%"
        
        if vid:
            # Search within specific vehicle
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
            # Search across all vehicles
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM parts p
                WHERE p.description LIKE ? OR p.part_number LIKE ?
            """, (search_term, search_term))
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
        
        return {
            "total": total,
            "parts": [dict(row) for row in rows]
        }

@app.get("/options/{option_code}", response_model=List[PartWithContext])
def get_parts_by_option(option_code: str, vid: Optional[str] = None):
    """Get all parts that require a specific option code"""
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

# ============================================================================
# CROSS-VEHICLE QUERIES
# ============================================================================

@app.get("/main-groups/{mg_number}/vehicles", response_model=List[Vehicle])
def get_vehicles_with_main_group(mg_number: str):
    """Get all vehicles that have a specific main group"""
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

@app.get("/subgroups/{sg_number}/vehicles", response_model=List[Vehicle])
def get_vehicles_with_subgroup(sg_number: str, mg_number: Optional[str] = None):
    """Get all vehicles that have a specific subgroup"""
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

# ============================================================================
# STATISTICS ENDPOINT
# ============================================================================

@app.get("/stats")
def get_statistics():
    """Get database statistics"""
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

# ============================================================================
# RUN SERVER
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )