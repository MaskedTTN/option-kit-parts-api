from pydantic import BaseModel
from typing import List, Optional


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
