#!/usr/bin/env python3
import sys, json, re
from pathlib import Path
from typing import Dict, Any, List, Tuple

###############################################################################
# Utilities
###############################################################################

def load_structured(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"structuredData.json not found: {path}")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def flatten_text(extract_json: Dict[str, Any]) -> str:
    """
    Adobe Extract structuredData.json has a tree with content blocks.
    We just create a plain text body to run label/regex extraction on.
    """
    chunks: List[str] = []

    def walk(node):
        if isinstance(node, dict):
            # Common text holders: "Text", "text", "content"
            for key in ("Text", "text", "content"):
                v = node.get(key)
                if isinstance(v, str):
                    chunks.append(v)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for it in node:
                walk(it)

    walk(extract_json)
    # Deduplicate consecutive whitespace and normalize quotes
    text = "\n".join(chunks)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r?\n\s*\n+", "\n", text)
    return text

def get_after_label(text: str, labels: List[str], max_chars=120, sep_regex=r"[:\-–]\s*") -> str:
    """
    Find the first occurrence of any label and capture the short value that follows.
    Example: label 'Shipment No' in 'Shipment No: 12345' -> '12345'
    """
    for label in labels:
        # Word-boundary label tolerant to spaces/colon variants
        pattern = rf"{re.escape(label)}{sep_regex}(.{{1,{max_chars}}})"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            # Cut off at common hard breaks or next label hint
            val = re.split(r"\s{2,}|\n|  +|  |\t|\r", val)[0].strip()
            return val
    return ""

def get_first_match(text: str, patterns: List[str]) -> str:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""

def clean_scalar(x: Any) -> Any:
    if isinstance(x, str):
        # remove leading ".:" or stray punctuation, extra spaces
        x = re.sub(r'^\s*([.:;\-_/\\]+)\s*', '', x).strip()
        # collapse multiple inner spaces
        x = re.sub(r'\s{2,}', ' ', x)
        # common junk endings
        x = re.sub(r'[.:;\-_/\\]+$', '', x).strip()
    return x

def parse_weight(s: str) -> str:
    """
    Extract a numeric weight (kg) from a messy string like 'Gross Weight (kg): 123,45'
    Returns normalized string (e.g., '123.45') or ''.
    """
    if not s:
        return ""
    m = re.search(r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?)\s*(?:kg|kilo|kilograms?)?", s, flags=re.IGNORECASE)
    if not m:
        return ""
    val = m.group(1).replace(" ", "")
    # normalize decimal separator
    if val.count(",") == 1 and val.count(".") == 0:
        val = val.replace(",", ".")
    else:
        val = val.replace(",", "")
    return val

def postprocess_cleanup(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Final polishing of values: remove artifacts, parse incoterms if embedded in another field, etc.
    """
    # Clean common scalar fields
    paths = [
        "refs.shipment_no", "refs.order_no_internal", "refs.customer_po",
        "refs.delivery_no", "refs.customer_no", "refs.loading_date",
        "refs.scheduled_delivery_date",
        "shipping.shipping_point", "shipping.incoterms", "shipping.way_of_forwarding",
        "shipping.pol", "shipping.pod",
        "cargo.description",
        "marks.carton_marks", "marks.labelling",
        "shipper.name", "shipper.address", "shipper.contact", "shipper.email", "shipper.phone", "shipper.vat",
        "consignee.name", "consignee.address", "consignee.vat",
        "notify.name", "notify.address", "notify.email", "notify.phone",
        "bl.type",
    ]

    def getset(d, dotted):
        cur = d
        parts = dotted.split(".")
        for p in parts[:-1]:
            cur = cur.get(p, {})
        last = parts[-1]
        if last in cur and cur[last] is not None:
            cur[last] = clean_scalar(cur[last])

    for p in paths:
        getset(payload, p)

    # Try to extract Incoterms if buried in way_of_forwarding
    w = payload.get("shipping", {}).get("way_of_forwarding") or ""
    if w and not payload.get("shipping", {}).get("incoterms"):
        # e.g., 'Delivery Terms: CIP ASPROPYRGOS'
        m = re.search(r"(?:delivery\s*terms?\s*[:\-–]\s*)?([A-Z]{3})\b", w, flags=re.IGNORECASE)
        if m:
            payload.setdefault("shipping", {})["incoterms"] = m.group(1).upper()

    # Parse numeric weights reliably from text
    if "cargo" in payload:
        for k in ("net_kg", "gross_kg"):
            raw = payload["cargo"].get(k, "")
            if not raw:
                continue
            payload["cargo"][k] = parse_weight(str(raw)) or clean_scalar(raw)

    # Drop obviously empty strings
    def drop_empties(d):
        if isinstance(d, dict):
            for k, v in list(d.items()):
                if isinstance(v, str) and not v.strip():
                    d[k] = None
                else:
                    drop_empties(v)
        elif isinstance(d, list):
            for i, v in enumerate(d):
                drop_empties(v)
    drop_empties(payload)

    return payload

###############################################################################
# Core extraction (rules-based)
###############################################################################

def extract_fields(text: str) -> Dict[str, Any]:
    """
    Pulls values from full text. Add/adjust label variants as needed for your PDFs.
    """
    # Parties
    shipper_name = get_first_match(text, [
        r"Shipper\s*[:\-–]\s*(.+)",
        r"Exporter\s*[:\-–]\s*(.+)"
    ])
    consignee_name = get_first_match(text, [
        r"Consignee\s*[:\-–]\s*(.+)"
    ])
    notify_name = get_first_match(text, [
        r"Notify\s*Party\s*[:\-–]\s*(.+)",
        r"Notify\s*[:\-–]\s*(.+)"
    ])

    shipper_addr = get_first_match(text, [r"Shipper\s*Address\s*[:\-–]\s*(.+)", r"Address\s*\(Shipper\)\s*[:\-–]\s*(.+)"])
    consignee_addr = get_first_match(text, [r"Consignee\s*Address\s*[:\-–]\s*(.+)"])
    notify_addr = get_first_match(text, [r"Notify\s*Address\s*[:\-–]\s*(.+)"])

    shipper_email = get_first_match(text, [r"Shipper\s*Email\s*[:\-–]\s*([^\s;]+)"])
    notify_email = get_first_match(text, [r"Notify\s*Email\s*[:\-–]\s*([^\s;]+)"])
    shipper_phone = get_first_match(text, [r"Shipper\s*Phone\s*[:\-–]\s*([^\s;]+)"])
    notify_phone = get_first_match(text, [r"Notify\s*Phone\s*[:\-–]\s*([^\s;]+)"])
    shipper_contact = get_first_match(text, [r"Shipper\s*Contact\s*[:\-–]\s*(.+)"])
    shipper_vat = get_first_match(text, [r"VAT\s*[:\-–]\s*([A-Z0-9\-]+)"])

    # References
    shipment_no = get_after_label(text, ["Shipment No", "Shipment Number", "Shipment #", "SLI"])
    order_no_internal = get_after_label(text, ["Order No", "Internal Order No", "Order Number"])
    customer_po = get_after_label(text, ["Customer PO", "PO Number", "Purchase Order"])
    delivery_no = get_after_label(text, ["Delivery No", "Delivery Number"])
    customer_no = get_after_label(text, ["Customer No", "Customer Number"])

    loading_date = get_first_match(text, [r"Loading\s*Date\s*[:\-–]\s*([0-9./\- ]{6,12})"])
    scheduled_delivery_date = get_first_match(text, [r"(?:Scheduled\s*)?Delivery\s*Date\s*[:\-–]\s*([0-9./\- ]{6,12})"])

    # Shipping
    shipping_point = get_after_label(text, ["Shipping Point", "Loading Point"])
    incoterms = get_first_match(text, [r"\b(FOB|CIF|CIP|DAP|DDP|EXW|FCA|CFR|CPT|DAT|DDU)\b"])
    way_of_forwarding = get_after_label(text, ["Way of Forwarding", "Mode of Transport", "Transport Mode"])
    pol = get_after_label(text, ["POL", "Port of Loading"])
    pod = get_after_label(text, ["POD", "Port of Discharge", "Destination Port"])

    # Cargo
    cargo_desc = get_after_label(text, ["Cargo Description", "Goods Description", "Description of Goods"], max_chars=200)
    net_kg_raw = get_first_match(text, [r"Net\s*Weight(?:\s*\(kg\))?\s*[:\-–]\s*([^\n]+)"])
    gross_kg_raw = get_first_match(text, [r"Gross\s*Weight(?:\s*\(kg\))?\s*[:\-–]\s*([^\n]+)"])

    # Packaging lines (simple heuristic: look for a block after "Packaging")
    packaging_block = ""
    m = re.search(r"Packaging\s*[:\-–]?\s*(.+?)(?:\n{2,}|$)", text, flags=re.IGNORECASE | re.DOTALL)
    if m:
        packaging_block = m.group(1)
    packaging = []
    for line in packaging_block.splitlines():
        line = line.strip(" -•\t\r\n")
        if len(line) >= 2:
            packaging.append(line)
    if not packaging:
        packaging = None

    # Marks & Labels
    marks = get_after_label(text, ["Marks", "Marks & Numbers", "Carton Marks"], max_chars=200)
    labelling = get_after_label(text, ["Labelling", "Labels"], max_chars=120)

    # B/L
    bl_type = get_after_label(text, ["B/L Type", "BL Type", "Bill of Lading Type", "BOL Type"])

    # Normalize & return
    payload: Dict[str, Any] = {
        "shipper": {
            "name": shipper_name or None,
            "address": shipper_addr or None,
            "contact": shipper_contact or None,
            "email": shipper_email or None,
            "phone": shipper_phone or None,
            "vat": shipper_vat or None,
        },
        "consignee": {
            "name": consignee_name or None,
            "address": consignee_addr or None,
            "vat": None,  # set if you find it in your docs
        },
        "notify": {
            "name": notify_name or None,
            "address": notify_addr or None,
            "email": notify_email or None,
            "phone": notify_phone or None,
        },
        "refs": {
            "shipment_no": shipment_no or None,
            "order_no_internal": order_no_internal or None,
            "customer_po": customer_po or None,
            "delivery_no": delivery_no or None,
            "customer_no": customer_no or None,
            "loading_date": loading_date or None,
            "scheduled_delivery_date": scheduled_delivery_date or None,
        },
        "shipping": {
            "shipping_point": shipping_point or None,
            "incoterms": (incoterms or "").upper() or None,
            "way_of_forwarding": way_of_forwarding or None,
            "pol": pol or None,
            "pod": pod or None,
        },
        "cargo": {
            "description": cargo_desc or None,
            "packaging": packaging,
            "net_kg": net_kg_raw or None,
            "gross_kg": gross_kg_raw or None,
        },
        "marks": {
            "carton_marks": marks or None,
            "labelling": labelling or None,
        },
        "bl": {
            "type": bl_type or None,
        },
    }

    # Parse numeric weights
    if payload["cargo"]["net_kg"]:
        payload["cargo"]["net_kg"] = parse_weight(payload["cargo"]["net_kg"])
    if payload["cargo"]["gross_kg"]:
        payload["cargo"]["gross_kg"] = parse_weight(payload["cargo"]["gross_kg"])

    return payload

###############################################################################
# CLI
###############################################################################

def main():
    if len(sys.argv) != 3:
        print("Usage: python ai_normalizer.py <structuredData.json> <out.json>")
        sys.exit(1)

    in_json = sys.argv[1]
    out_json = sys.argv[2]

    data = load_structured(in_json)
    text = flatten_text(data)

    payload = extract_fields(text)
    payload = postprocess_cleanup(payload)

    # Optional: simple confidence map (present vs missing)
    conf = {}
    def mark(d: Dict[str, Any], prefix=""):
        for k, v in d.items():
            key = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
            if isinstance(v, dict):
                mark(v, key)
            else:
                conf[key] = 0.95 if (isinstance(v, str) and v.strip()) else (0.6 if v not in (None, "") else 0.0)
    mark(payload)
    payload["_confidence"] = conf

    Path(out_json).parent.mkdir(parents=True, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"Wrote normalized JSON to {out_json}")

if __name__ == "__main__":
    main()
