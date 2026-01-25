from __future__ import annotations

from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import List, Optional, Dict, Any
from uuid import uuid4
import random

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

app = FastAPI(title="Mock WMS API", version="0.1.0")


# ---------- Helpers ----------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def ensure_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ---------- Status Enums ----------
class IBStatus(str, Enum):
    NEW = "NEW"
    PROCESSING = "PROCESSING"
    FINISHED = "FINISHED"
    CANCELLED = "CANCELLED"


class OBStatus(str, Enum):
    NEW = "NEW"
    READYTOPICK = "READYTOPICK"
    PICKING = "PICKING"
    PICKED = "PICKED"
    PACKING = "PACKING"
    PACKED = "PACKED"
    CANCELLED = "CANCELLED"


# ---------- Models ----------
class IBLine(BaseModel):
    line_id: str = Field(default_factory=lambda: str(uuid4()))
    product_id: int
    sku: str
    qty_unit_id: int
    expected_qty: int
    actual_qty: int = 0


class IBReceipt(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    po_code: str
    po_date: str  # keep simple string YYYY-MM-DD for mock
    status: IBStatus = IBStatus.NEW
    note: Optional[str] = None

    processed_by: Optional[str] = None
    contact_name: Optional[str] = None
    contact_phone: Optional[str] = None

    client_id: int
    warehouse_id: int

    created_by: str
    created_at: str
    updated_by: str
    updated_at: str
    finished_at: Optional[str] = None

    lines: List[IBLine]


class OBLine(BaseModel):
    line_id: str = Field(default_factory=lambda: str(uuid4()))
    product_id: int
    sku: str
    qty: int


class OBOrder(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    so_code: str
    expected_delivery_date: str
    actual_delivery_date: Optional[str] = None

    customer_id: int
    shipping_address_id: int

    total_amount: float = 0
    actual_amount: float = 0
    note: Optional[str] = None

    client_id: int
    warehouse_id: int
    status: OBStatus = OBStatus.NEW

    total_cod_amount: float = 0
    total_weight: float = 0
    total_volume: float = 0

    created_by: str
    created_at: str
    updated_by: str
    updated_at: str

    lines: List[OBLine]


# ---------- In-memory "DB" ----------
DB: Dict[str, List[Dict[str, Any]]] = {
    "ib": [],
    "ob": [],
}


def seed_data():
    random.seed(7)
    base_time = now_utc() - timedelta(hours=6)

    # Seed IB
    for i in range(3000):
        t = base_time + timedelta(minutes=i * 5)
        receipt = IBReceipt(
            po_code=f"PO{20250000 + i}",
            po_date=(t.date().isoformat()),
            status=random.choice([IBStatus.NEW, IBStatus.PROCESSING]),
            note=random.choice([None, "urgent", "check qty"]),
            processed_by=random.choice([None, "wms_user_a", "wms_user_b"]),
            contact_name=random.choice([None, "NCC A", "NCC B"]),
            contact_phone=random.choice([None, "0900000001", "0900000002"]),
            client_id=1,
            warehouse_id=101,
            created_by="system",
            created_at=iso(t),
            updated_by="system",
            updated_at=iso(t),
            lines=[
                IBLine(product_id=1001, sku="SKU-1001", qty_unit_id=1, expected_qty=random.randint(5, 30)),
                IBLine(product_id=1002, sku="SKU-1002", qty_unit_id=1, expected_qty=random.randint(5, 30)),
            ],
        )
        DB["ib"].append(receipt.model_dump())

    # Seed OB
    for i in range(3000):
        t = base_time + timedelta(minutes=i * 4)
        order = OBOrder(
            so_code=f"SO{20250000 + i}",
            expected_delivery_date=(t.date().isoformat()),
            customer_id=random.randint(2000, 2010),
            shipping_address_id=random.randint(3000, 3010),
            total_amount=float(random.randint(200000, 2000000)),
            actual_amount=0,
            note=random.choice([None, "fragile", "COD"]),
            client_id=1,
            warehouse_id=101,
            status=random.choice([OBStatus.NEW, OBStatus.READYTOPICK]),
            total_cod_amount=0,
            total_weight=round(random.uniform(0.5, 30.0), 2),
            total_volume=round(random.uniform(0.01, 1.5), 3),
            created_by="system",
            created_at=iso(t),
            updated_by="system",
            updated_at=iso(t),
            lines=[
                OBLine(product_id=1001, sku="SKU-1001", qty=random.randint(1, 5)),
                OBLine(product_id=1003, sku="SKU-1003", qty=random.randint(1, 5)),
            ],
        )
        DB["ob"].append(order.model_dump())


seed_data()


# ---------- Status transitions ----------
def ib_next_status(s: IBStatus) -> IBStatus:
    if s == IBStatus.CANCELLED:
        return IBStatus.CANCELLED
    if s == IBStatus.NEW:
        return IBStatus.PROCESSING
    if s == IBStatus.PROCESSING:
        return IBStatus.FINISHED
    return IBStatus.FINISHED


def ob_next_status(s: OBStatus) -> OBStatus:
    if s == OBStatus.CANCELLED:
        return OBStatus.CANCELLED
    order = [
        OBStatus.NEW,
        OBStatus.READYTOPICK,
        OBStatus.PICKING,
        OBStatus.PICKED,
        OBStatus.PACKING,
        OBStatus.PACKED,
    ]
    idx = order.index(s)
    return order[min(idx + 1, len(order) - 1)]




# ---------- API ----------
@app.get("/health")
def health():
    return {"status": "ok", "time_utc": iso(now_utc())}


@app.get("/ib/receipts")
def get_ib_receipts(
    updated_after: Optional[datetime] = Query(default=None, description="ISO8601 datetime with timezone"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    items = sorted(DB["ib"], key=lambda x: (x["updated_at"], x["id"]))
    updated_after = ensure_utc(updated_after)
    
    if updated_after:
        items = [x for x in items if datetime.fromisoformat(x["updated_at"]) > updated_after]
        

    return {
        "data": items[offset : offset + limit],
        "meta": {"limit": limit, "offset": offset, "count": len(items)},
    }


@app.get("/ob/orders")
def get_ob_orders(
    updated_after: Optional[datetime] = Query(default=None, description="ISO8601 datetime with timezone"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    items = sorted(DB["ob"], key=lambda x: (x["updated_at"], x["id"]))
    updated_after = ensure_utc(updated_after)
    
    if updated_after:
        items = [x for x in items if datetime.fromisoformat(x["updated_at"]) > updated_after]

    return {
        "data": items[offset : offset + limit],
        "meta": {"limit": limit, "offset": offset, "count": len(items)},
    }


@app.post("/simulate/tick")
def simulate_tick(
    n_changes: int = Query(default=10, ge=1, le=200),
):
    """
    Make the data change over time:
    - advance statuses
    - update quantities
    - bump updated_at
    """
    t = now_utc()
    cancel_prob = 0.05
    # choose random IB receipts to mutate
    for x in random.sample(DB["ib"], k=min(n_changes, len(DB["ib"]))):
        if x["status"] in (IBStatus.CANCELLED.value, IBStatus.FINISHED.value):
            continue

        if (x["status"] == IBStatus.NEW.value) and (random.random() < cancel_prob):
            x["status"] = IBStatus.CANCELLED.value
            x["updated_at"] = iso(t)
            x["updated_by"] = "simulator"
            continue
        
        old = x["status"]
        new = ib_next_status(IBStatus(old))
        x["status"] = new.value
        x["updated_at"] = iso(t)
        x["updated_by"] = "simulator"

        # simulate receiving: actual_qty increases during PROCESSING
        if new == IBStatus.PROCESSING:
            for ln in x["lines"]:
                if ln["actual_qty"] < ln["expected_qty"]:
                    ln["actual_qty"] = min(ln["expected_qty"], ln["actual_qty"] + random.randint(1, 5))
        if new == IBStatus.FINISHED:
            for ln in x["lines"]:
                ln["actual_qty"] = ln["expected_qty"]
            x["finished_at"] = iso(t)

    # choose random OB orders to mutate
    for x in random.sample(DB["ob"], k=min(n_changes, len(DB["ob"]))):
        if x["status"] in (OBStatus.CANCELLED.value, OBStatus.PACKED.value):
            continue
        
        if random.random() < cancel_prob:
            x["status"] = OBStatus.CANCELLED.value
            x["updated_at"] = iso(t)
            x["updated_by"] = "simulator"
            continue
        old = x["status"]
        new = ob_next_status(OBStatus(old))
        x["status"] = new.value
        x["updated_at"] = iso(t)
        x["updated_by"] = "simulator"

        if new == OBStatus.PACKED:
            x["actual_amount"] = x["total_amount"]
            x["actual_delivery_date"] = t.date().isoformat()

    return {"ok": True, "changed": n_changes, "time_utc": iso(t)}
