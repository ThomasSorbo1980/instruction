#!/usr/bin/env python3
import sys, json, re, os, math
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

# ============================================================
# Utilities
# ============================================================

def load_structured(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"structuredData.json not found: {path}")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def clean_scalar(x: Any) -> Any:
    if isinstance(x, str):
        x = x.replace("\u00a0", " ")
        x = re.sub(r'^\s*([.:;\-_/\\]+)\s*', '', x).strip()
        x = re.sub(r'\s{2,}', ' ', x)
        x = re.sub(r'[.:;\-_/\\]+$', '', x).strip()
    return x

def parse_weight(s: str) -> str:
    if not s: return ""
    m = re.search(r"(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?)\s*(?:kg|kilo|kilograms?)?\b", s, flags=re.I)
    if not m: return ""
    val = m.group(1).replace(" ", "")
    # normalize decimal separator
    if val.count(",") == 1 and val.count(".") == 0:
        val = val.replace(",", ".")
    else:
        val = val.replace(",", "")
    return val

# ============================================================
# Read layout elements from Adobe structuredData.json
# We try to be tolerant to schema variants.
# ============================================================

class Span:
    __slots__ = ("text", "page", "x", "y", "w", "h")
    def __init__(self, text: str, page: int, bounds: List[float]):
        self.text = norm_space(text)
        self.page = int(bounds[5]) if len(bounds) >= 6 else page
        # Adobe bounds often look like [x, y, width, height, rotation, page]
        self.x = float(bounds[0]) if bounds else 0.0
        self.y = float(bounds[1]) if bounds else 0.0
        self.w = float(bounds[2]) if len(bounds) > 2 else 0.0
        self.h = float(bounds[3]) if len(bounds) > 3 else 0.0

    def right_of(self, other, max_dx=500, same_line_tol=6):
        same_line = abs(self.y - other.y) <= same_line_tol
        return same_line and self.x >= other.x and (self.x - other.x) <= max_dx

    def below(self, other, max_dy=120, x_overlap_tol=10):
        # Some overlap in x to consider it "below the label"
        overlap = (min(self.x + self.w, other.x + other.w) - max(self.x, other.x)) > -x_overlap_tol
        return self.y > other.y and (self.y - other.y) <= max_dy and overlap

def collect_spans(doc: Dict[str, Any]) -> List[Span]:
    spans: List[Span] = []

    def walk(node):
        if isinstance(node, dict):
            # Typical holders
            txt = node.get("Text") or node.get("text") or node.get("content")
            b = node.get("Bounds") or node.get("bounds") or node.get("BBox") or []
            p = node.get("PageNumber") or node.get("pageNumber") or 1
            # Some nodes store "Lines":[{"Text":"..."}]
            if isinstance(txt, str):
                spans.append(Span(txt, int(p), b if isinstance(b, list) else []))
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(doc)
    # Filter empties and sort roughly by page,y,x
    spans = [s for s in spans if s.text]
    spans.sort(key=lambda s: (s.page, s.y, s.x))
    return spans

def collect_tables(doc: Dict[str, Any]) -> List[List[List[str]]]:
    """
    Return list of tables; each table is a list of rows; each row is list of cell strings.
    Tolerates variants: {"Table": {"bodyRows": [{"cells":[{"content":[{"Text":"..."},...]}, ...]}]}}
    """
    tables: List[List[List[str]]] = []

    def norm_cell(cell_obj) -> str:
        if isinstance(cell_obj, dict):
            # Cells usually have "content" list of text nodes
            cont = cell_obj.get("content") or cell_obj.get("Content") or []
            texts = []
            if isinstance(cont, list):
                for c in cont:
                    t = c.get("Text") or c.get("text") or c.get("content") if isinstance(c, dict) else str(c)
                    if isinstance(t, str):
                        texts.append(t)
            return norm_space(" ".join(texts))
        return norm_space(str(cell_obj))

    def walk(node):
        if isinstance(node, dict):
            if "Table" in node or "table" in node:
                tbl = node.get("Table") or node.get("table")
                rows = []
                # Prefer bodyRows; also check "rows"
                body = tbl.get("bodyRows") or tbl.get("rows") or []
                for r in body:
                    cells = r.get("cells") or r.get("Cells") or []
                    rows.append([norm_cell(c) for c in cells])
                if rows:
                    tables.append(rows)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(doc)
    return tables

# ============================================================
# Label dictionaries
# ============================================================

LABELS = {
    "shipment_no": ["Shipment No", "Shipment Number", "Shipment #", "SLI", "Shipment Ref", "Shpt No"],
    "order_no_internal": ["Order No", "Internal Order No", "Order Number", "Order #"],
    "customer_po": ["Customer PO", "PO Number", "Purchase Order", "Cust PO"],
    "delivery_no": ["Delivery No", "Delivery Number", "Del. No"],
    "customer_no": ["Customer No", "Customer Number", "Cust No", "Account No"],

    "loading_date": ["Loading Date", "Load Date"],
    "scheduled_delivery_date": ["Delivery Date", "Scheduled Delivery Date", "ETA"],

    "shipping_point": ["Shipping Point", "Loading Point"],
    "incoterms": ["Incoterms", "Delivery Terms", "Terms of Delivery"],
    "way_of_forwarding": ["Way of Forwarding", "Mode of Transport", "Transport Mode", "Shipment Mode"],
    "pol": ["POL", "Port of Loading", "Load Port"],
    "pod": ["POD", "Port of Discharge", "Destination Port", "Port of Destination"],

    "cargo_description": ["Cargo Description", "Goods Description", "Description of Goods", "Commodity"],
    "net_weight": ["Net Weight", "Net Wt", "Net (kg)"],
    "gross_weight": ["Gross Weight", "Gross Wt", "Gross (kg)"],

    "marks": ["Marks", "Marks & Numbers", "Carton Marks"],
    "labelling": ["Labelling", "Labels", "Labelling Instructions"],

    "bl_type": ["B/L Type", "BL Type", "Bill of Lading Type", "BOL Type"],
}

PARTY_LABELS = {
    "shipper": ["Shipper", "Exporter"],
    "consignee": ["Consignee", "Buyer"],
    "notify": ["Notify Party", "Notify"],
}

INCOTERMS_SET = {"EXW","FCA","CPT","CIP","DAP","DPU","DDP","FAS","FOB","CFR","CIF"}  # 2020 list

# ============================================================
# Extraction helpers
# ============================================================

def nearest_value(spans: List[Span], label_span: Span) -> Optional[str]:
    """
    Find the best value near a label:
    1) same line, to the right; else
    2) nearest below (within window).
    """
    # 1) same line to the right
    candidates = [s for s in spans if s.page == label_span.page and s.right_of(label_span)]
    if candidates:
        return candidates[0].text

    # 2) below
    below = [s for s in spans if s.page == label_span.page and s.below(label_span)]
    below.sort(key=lambda s: (s.y - label_span.y, abs(s.x - label_span.x)))
    if below:
        return below[0].text
    return None

def find_label_spans(spans: List[Span], labels: List[str]) -> List[Span]:
    out = []
    pat = re.compile("|".join([re.escape(l) for l in labels]), re.I)
    for s in spans:
        if pat.fullmatch(s.text) or pat.search(s.text):
            out.append(s)
    return out

def extract_party_block(text: str, tag: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Fallback for party name/address from flat text if layout fails.
    """
    name = None; addr = None
    # Name line
    m = re.search(rf"{tag}\s*[:\-–]\s*(.+)", text, flags=re.I)
    if m:
        name = m.group(1).strip()
    # Address
    m2 = re.search(rf"{tag}.*?(?:Address|Addr)\s*[:\-–]\s*(.+)", text, flags=re.I|re.S)
    if m2:
        addr = norm_space(m2.group(1))
    return name, addr

def flatten_text(doc: Dict[str,Any]) -> str:
    chunks = []
    def walk(node):
        if isinstance(node, dict):
            for k in ("Text","text","content"):
                v = node.get(k)
                if isinstance(v,str): chunks.append(v)
            for v in node.values(): walk(v)
        elif isinstance(node, list):
            for v in node: walk(v)
    walk(doc)
    t = "\n".join(chunks)
    t = re.sub(r"[ \t]+"," ",t)
    t = re.sub(r"\r?\n\s*\n+","\n",t)
    return t

def parse_tables_for_pairs(tables: List[List[List[str]]]) -> Dict[str, str]:
    """
    Look through 2-column tables or key/value rows and build a small dict.
    """
    kv = {}
    for tbl in tables:
        for row in tbl:
            cells = [norm_space(c) for c in row]
            if len(cells) == 2:
                k,v = cells
                if len(k)<=60 and len(v)<=200:
                    kv[k] = v
            elif len(cells) > 2:
                # sometimes "Key : Value" split across columns
                joined = " ".join(cells)
                m = re.match(r"(.{2,60})\s*[:\-–]\s*(.{1,200})$", joined)
                if m:
                    kv[norm_space(m.group(1))] = norm_space(m.group(2))
    return kv

# ============================================================
# Core extraction using layout + tables + fallbacks
# ============================================================

def extract_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
    spans = collect_spans(doc)
    text = flatten_text(doc)
    tables = collect_tables(doc)
    table_kv = parse_tables_for_pairs(tables)

    # Parties (name/address) via labels + proximity
    party = {"shipper":{}, "consignee":{}, "notify":{}}
    for role, labels in PARTY_LABELS.items():
        label_spans = find_label_spans(spans, labels)
        name = None; addr = None
        if label_spans:
            v = nearest_value(spans, label_spans[0])
            if v: name = v
            # try an "Address" label near-by
            addr_span = find_label_spans(spans, ["Address","Addr"])
            # choose the address label on same/next lines and closest
            if addr_span:
                addr_candidates = sorted(
                    [(s, math.hypot((s.x - label_spans[0].x), (s.y - label_spans[0].y))) for s in addr_span
                     if s.page==label_spans[0].page],
                    key=lambda t: t[1]
                )
                if addr_candidates:
                    addr_v = nearest_value(spans, addr_candidates[0][0])
                    if addr_v: addr = addr_v
        if not name:
            # fallback from flat text
            nm, ad = extract_party_block(text, labels[0])
            name = name or nm
            addr = addr or ad
        party[role] = {"name": name or None, "address": addr or None}

    # Basic refs via label proximity or tables
    def pick_from(label_key, max_chars=120):
        # 1) table kv
        for lab in LABELS[label_key]:
            for k,v in table_kv.items():
                if re.fullmatch(rf".*{re.escape(lab)}.*", k, flags=re.I):
                    return v
        # 2) label spans
        lsp = find_label_spans(spans, LABELS[label_key])
        if lsp:
            v = nearest_value(spans, lsp[0])
            if v: return v
        # 3) flat fallback: Label: value
        pat = rf"(?:{'|'.join(map(re.escape,LABELS[label_key]))})\s*[:\-–]\s*(.{{1,{max_chars}}})"
        m = re.search(pat, text, flags=re.I)
        if m:
            return m.group(1).strip()
        return ""

    shipment_no = pick_from("shipment_no")
    order_no_internal = pick_from("order_no_internal")
    customer_po = pick_from("customer_po")
    delivery_no = pick_from("delivery_no")
    customer_no = pick_from("customer_no")
    loading_date = pick_from("loading_date", 20)
    scheduled_delivery_date = pick_from("scheduled_delivery_date", 20)

    shipping_point = pick_from("shipping_point")
    incoterms = pick_from("incoterms")
    way_of_forwarding = pick_from("way_of_forwarding")
    pol = pick_from("pol")
    pod = pick_from("pod")

    cargo_description = pick_from("cargo_description", 220)
    net_raw = pick_from("net_weight", 60)
    gross_raw = pick_from("gross_weight", 60)

    marks = pick_from("marks", 220)
    labelling = pick_from("labelling", 140)
    bl_type = pick_from("bl_type", 50)

    # Normalize weights
    net_kg = parse_weight(net_raw) if net_raw else ""
    gross_kg = parse_weight(gross_raw) if gross_raw else ""

    payload: Dict[str, Any] = {
        "shipper": party["shipper"],
        "consignee": party["consignee"],
        "notify": party["notify"],
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
            "description": cargo_description or None,
            "packaging": None,  # can be extended to parse table rows of packages if needed
            "net_kg": net_kg or (net_raw or None),
            "gross_kg": gross_kg or (gross_raw or None),
        },
        "marks": {
            "carton_marks": marks or None,
            "labelling": labelling or None,
        },
        "bl": {
            "type": bl_type or None,
        },
    }
    return payload

# ============================================================
# Post processing + optional LLM refinement
# ============================================================

def postprocess_cleanup(payload: Dict[str, Any]) -> Dict[str, Any]:
    # Clean common scalar fields
    paths = [
        "refs.shipment_no", "refs.order_no_internal", "refs.customer_po",
        "refs.delivery_no", "refs.customer_no", "refs.loading_date",
        "refs.scheduled_delivery_date",
        "shipping.shipping_point", "shipping.incoterms", "shipping.way_of_forwarding",
        "shipping.pol", "shipping.pod",
        "cargo.description",
        "marks.carton_marks", "marks.labelling",
        "shipper.name", "shipper.address",
        "consignee.name", "consignee.address",
        "notify.name", "notify.address",
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

    # Infer Incoterms if buried elsewhere
    w = (payload.get("shipping", {}) or {}).get("way_of_forwarding") or ""
    if w and not payload["shipping"].get("incoterms"):
        m = re.search(r"\b([A-Z]{3})\b", w)
        if m and m.group(1).upper() in INCOTERMS_SET:
            payload["shipping"]["incoterms"] = m.group(1).upper()

    # Normalize weights again in case clean_scalar changed them
    for k in ("net_kg","gross_kg"):
        val = payload.get("cargo",{}).get(k)
        if isinstance(val,str):
            payload["cargo"][k] = parse_weight(val) or val

    # Drop empty strings -> None
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

def llm_refine(payload: Dict[str, Any], text: str) -> Dict[str, Any]:
    """
    Optional: if OPENAI_API_KEY is present, ask a model to fix obvious gaps
    and standardize fields. We keep it conservative (no hallucinating).
    """
    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        return payload

    try:
        import requests as _rq
        prompt = {
            "role": "system",
            "content": (
                "You correct a JSON form extracted from a shipping instruction PDF.\n"
                "Use only facts in the provided 'text' unless it's a trivial formatting fix.\n"
                "Allowed fixes: trim noise, correct obvious OCR typos (Incoterms, ports, weights),\n"
                "standardize Incoterms to one of EXW,FCA,CPT,CIP,DAP,DPU,DDP,FAS,FOB,CFR,CIF.\n"
                "Never fabricate values. If unsure, leave fields as-is."
            )
        }
        user = {
            "role": "user",
            "content": json.dumps({
                "text_excerpt": text[:6000],
                "json": payload
            }, ensure_ascii=False)
        }
        # Use Chat Completions format (gpt-4o-mini cheap/good)
        resp = _rq.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "gpt-4o-mini",
                "messages": [prompt, user],
                "temperature": 0.1,
                "response_format": {"type":"json_object"}
            },
            timeout=40
        )
        resp.raise_for_status()
        msg = resp.json()["choices"][0]["message"]["content"]
        fixed = json.loads(msg)
        # Sanity: merge fixed onto original, don't drop keys
        def deep_merge(a,b):
            if isinstance(a,dict) and isinstance(b,dict):
                out = dict(a)
                for k,v in b.items():
                    out[k] = deep_merge(a.get(k), v)
                return out
            return b if b is not None else a
        return deep_merge(payload, fixed)
    except Exception as e:
        # Fail quietly—return original payload
        print("LLM refine skipped:", e)
        return payload

# ============================================================
# CLI
# ============================================================

def main():
    if len(sys.argv) != 3:
        print("Usage: python ai_normalizer.py <structuredData.json> <out.json>")
        sys.exit(1)

    in_json = sys.argv[1]
    out_json = sys.argv[2]

    doc = load_structured(in_json)
    spans_text = flatten_text(doc)

    payload = extract_fields(doc)
    payload = postprocess_cleanup(payload)
    payload = llm_refine(payload, spans_text)

    # Confidence map (simple heuristic)
    conf = {}
    def mark(d: Dict[str, Any], prefix=""):
        for k, v in d.items():
            key = f"{prefix}.{k}"[1:] if prefix else k
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
