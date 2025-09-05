# ai_normalizer.py
# ------------------------------------------------------------
# Purpose: Turn Adobe PDF Extract 'structuredData.json' into a stable
# JSON schema for your Document Generation template, using:
#  1) Heuristics/rules (fast, reliable for obvious fields)
#  2) An LLM pass (fills missing/ambiguous fields; tolerant to label changes)
#  3) Validation (Pydantic) + confidence scoring
#
# Usage:
#   python ai_normalizer.py structuredData.json out.json
#
# Notes:
# - Plug in your preferred LLM in `llm_fill_missing()` (OpenAI/Anthropic/Gemini).
# - The script expects Adobe Extract JSON. If you only have raw text, place it
#   in `full_text` and skip the layout traversal.
# ------------------------------------------------------------

import json, sys, re, os
from typing import List, Optional, Dict, Any, Tuple

try:
    from pydantic import BaseModel, Field, validator
except ImportError:
    print("Please: pip install pydantic")
    sys.exit(1)

# ---------- Target Schema (must match your Doc Gen template tags) ----------
class Party(BaseModel):
    name: Optional[str] = None
    address: Optional[str] = None
    contact: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    vat: Optional[str] = None

class Refs(BaseModel):
    shipment_no: Optional[str] = None
    order_no_internal: Optional[str] = None
    customer_po: Optional[str] = None
    delivery_no: Optional[str] = None
    customer_no: Optional[str] = None
    loading_date: Optional[str] = None   # YYYY-MM-DD
    scheduled_delivery_date: Optional[str] = None  # YYYY-MM-DD

    @validator("loading_date", "scheduled_delivery_date")
    def _date_fmt(cls, v):
        if v is None: return v
        m = re.match(r"(\d{2})[./-](\d{2})[./-](\d{4})$", v.strip())
        if m:
            dd, mm, yyyy = m.groups()
            return f"{yyyy}-{mm}-{dd}"
        m2 = re.match(r"(\d{4})-(\d{2})-(\d{2})$", v.strip())
        if m2: return v
        return v

class Shipping(BaseModel):
    shipping_point: Optional[str] = None
    incoterms: Optional[str] = None
    way_of_forwarding: Optional[str] = None
    pol: Optional[str] = None
    pod: Optional[str] = None

class Cargo(BaseModel):
    description: Optional[str] = None
    packaging: List[str] = Field(default_factory=list)
    net_kg: Optional[float] = None
    gross_kg: Optional[float] = None

class Marks(BaseModel):
    carton_marks: Optional[str] = None
    labelling: Optional[str] = None

class BL(BaseModel):
    type: Optional[str] = None

class DocSchema(BaseModel):
    shipper: Party = Party()
    consignee: Party = Party()
    notify: Party = Party()
    refs: Refs = Refs()
    shipping: Shipping = Shipping()
    cargo: Cargo = Cargo()
    marks: Marks = Marks()
    bl: BL = BL()

# ---------- Helper: adobe extract traversal ----------
def extract_plain_text(adobe_json: Dict[str, Any]) -> str:
    texts: List[str] = []

    def walk(node):
        if isinstance(node, dict):
            t = node.get("Text")
            if isinstance(t, str) and t.strip():
                texts.append(t.strip())
            for k, v in node.items():
                if k in ("Text",):
                    continue
                walk(v)
        elif isinstance(node, list):
            for x in node:
                walk(x)

    walk(adobe_json)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(texts)).strip()

def extract_candidates_rules(adobe_json: Dict[str, Any], full_text: str) -> Dict[str, Any]:
    def find_val(label_variants: List[str]) -> Optional[str]:
        for lbl in label_variants:
            rx = re.compile(rf"{re.escape(lbl)}\s*[:：]?\s*(.+)$", re.IGNORECASE | re.MULTILINE)
            m = rx.search(full_text)
            if m:
                val = m.group(1).strip()
                val = re.split(r"\s{2,}|$|\n", val)[0].strip()
                if val and not re.fullmatch(r"[\-–—]+", val):
                    return val
        return None

    def find_weight(patterns: List[str]) -> Optional[float]:
        for p in patterns:
            m = re.search(rf"{p}\s*[:：]?\s*([\d.,]+)\s*kg", full_text, re.IGNORECASE)
            if m:
                num = m.group(1).replace(".", "").replace(",", ".")
                try:
                    return float(num)
                except:
                    pass
        return None

    candidates = {
        "refs.shipment_no": find_val(["Shipment No", "Shipment Number", "Shipment#"]),
        "refs.order_no_internal": find_val(["Order No", "Order Number", "Ord. Nr"]),
        "refs.customer_po": find_val(["Customer PO", "Customer Order", "PO Number"]),
        "refs.delivery_no": find_val(["Delivery No", "Delivery Number"]),
        "refs.customer_no": find_val(["Customer No", "Customer Number", "Customer ID"]),
        "refs.loading_date": find_val(["Loading Date", "Load Date"]),
        "refs.scheduled_delivery_date": find_val(["Scheduled Delivery Date", "Delivery Date", "ETA"]),
        "shipping.incoterms": find_val(["Incoterms", "Incoterm"]),
        "shipping.way_of_forwarding": find_val(["Way of Forwarding", "Mode of Transport"]),
        "bl.type": find_val(["B/L Type", "Bill of Lading Type"]),
    }

    candidates["cargo.net_kg"] = find_weight(["Net Weight", "Net Wt"])
    candidates["cargo.gross_kg"] = find_weight(["Gross Weight", "Gross Wt"])

    desc = find_val(["Cargo Description", "Description of Goods", "Commodity"])
    if desc:
        candidates["cargo.description"] = desc

    m = re.search(r"(Packaging\s*[:：]?\s*)([\s\S]{0,600})", full_text, re.IGNORECASE)
    if m:
        tail = m.group(2).split("\n")
        items = []
        for line in tail[:10]:
            line = line.strip("-• \t").strip()
            if not line or re.search(r"^(Net|Gross|HS|Incoterm|B/L|Marks)", line, re.IGNORECASE):
                break
            if len(line) > 2:
                items.append(line)
        if items:
            candidates["cargo.packaging"] = items

    marks_val = find_val(["Marks", "Marks & Numbers", "Shipping Marks"])
    if marks_val:
        candidates["marks.carton_marks"] = marks_val
    labelling_val = find_val(["Labelling", "Labeling", "Labels"])
    if labelling_val:
        candidates["marks.labelling"] = labelling_val

    return {k: v for k, v in candidates.items() if v is not None}

# ---------- LLM augmentation ----------
def llm_fill_missing(full_text: str, partial: Dict[str, Any]) -> Dict[str, Any]:
    system = (
        "You are an information extraction engine for shipping documents. "
        "Return ONLY valid JSON matching the provided schema. "
        "If a field is not present, omit it. "
        "Normalize dates to YYYY-MM-DD. Numbers are plain numerals, use '.' as decimal."
    )

    schema_desc = {
        "shipper": {"name":"str","address":"str","contact":"str","email":"str","phone":"str","vat":"str"},
        "consignee": {"name":"str","address":"str","vat":"str"},
        "notify": {"name":"str","address":"str","email":"str","phone":"str"},
        "refs": {"shipment_no":"str","order_no_internal":"str","customer_po":"str","delivery_no":"str","customer_no":"str","loading_date":"YYYY-MM-DD","scheduled_delivery_date":"YYYY-MM-DD"},
        "shipping": {"shipping_point":"str","incoterms":"str","way_of_forwarding":"str","pol":"str","pod":"str"},
        "cargo": {"description":"str","packaging":"list[str]","net_kg":"float","gross_kg":"float"},
        "marks": {"carton_marks":"str","labelling":"str"},
        "bl": {"type":"str"}
    }

    user = {
        "task": "Extract and normalize fields for a shipping instruction form.",
        "required_output_schema": schema_desc,
        "known_partial_values": partial,
        "document_text": full_text[:200000]
    }

    resp_json: Dict[str, Any] = {}
    try:
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key:
            import openai
            client = openai.OpenAI(api_key=api_key)
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0.0,
                messages=[
                    {"role":"system","content":system},
                    {"role":"user","content":json.dumps(user)}
                ],
                response_format={"type":"json_object"}
            )
            txt = resp.choices[0].message.content
            resp_json = json.loads(txt)
    except Exception:
        resp_json = {}

    return resp_json

TARGET_KEYS = {
    "shipper.name","shipper.address","shipper.contact","shipper.email","shipper.phone","shipper.vat",
    "consignee.name","consignee.address","consignee.vat",
    "notify.name","notify.address","notify.email","notify.phone",
    "refs.shipment_no","refs.order_no_internal","refs.customer_po","refs.delivery_no","refs.customer_no","refs.loading_date","refs.scheduled_delivery_date",
    "shipping.shipping_point","shipping.incoterms","shipping.way_of_forwarding","shipping.pol","shipping.pod",
    "cargo.description","cargo.packaging","cargo.net_kg","cargo.gross_kg",
    "marks.carton_marks","marks.labelling",
    "bl.type"
}

def merge_dict_path(out: Dict[str, Any], dotted_key: str, value: Any):
    parts = dotted_key.split(".")
    cur = out
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value

def validate_and_score(d: Dict[str, Any]) -> Tuple[DocSchema, Dict[str, float]]:
    conf: Dict[str, float] = d.pop("_confidence", {})
    model = DocSchema(**d)
    return model, conf

def flatten(d, prefix=""):
    out = {}
    if isinstance(d, dict):
        for k, v in d.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                out.update(flatten(v, path))
            else:
                out[path] = v
    return out

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("structured_json", help="Adobe Extract structuredData.json")
    parser.add_argument("out_json", help="Output JSON for Document Generation")
    args = parser.parse_args()

    with open(args.structured_json, "r", encoding="utf-8") as f:
        adobe_json = json.load(f)

    full_text = extract_plain_text(adobe_json)
    rule_hits = extract_candidates_rules(adobe_json, full_text)

    partial: Dict[str, Any] = {}
    for k, v in rule_hits.items():
        merge_dict_path(partial, k, v)
    conf = {k: 0.95 for k in rule_hits.keys()}

    ai_resp = llm_fill_missing(full_text, partial)
    def deep_merge(dst, src):
        for kk, vv in src.items():
            if isinstance(vv, dict):
                if kk not in dst: dst[kk] = {}
                deep_merge(dst[kk], vv)
            else:
                dst[kk] = vv
        return dst
    partial = deep_merge(partial, ai_resp or {})

    flat = flatten(partial)
    for k in flat:
        if k not in conf and k in TARGET_KEYS:
            conf[k] = 0.75
    partial["_confidence"] = conf

    model, confmap = validate_and_score(partial)
    payload = model.dict()
    payload["_confidence"] = confmap

    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"OK → {args.out_json}")

if __name__ == "__main__":
    main()
