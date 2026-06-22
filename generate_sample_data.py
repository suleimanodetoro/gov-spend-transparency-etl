"""
Generate messy 'spend over GBP 25k' CSVs, one per department per month, mimicking
real transparency data: different column names, dirty amounts, mixed date formats,
duplicates, negatives (credits), a schema-drift file with unmapped columns, and a
malformed file with rows that must be quarantined.

Standard library only, so it runs with no Spark and no extra installs.
Output: ./data/raw/*.csv
"""
import csv, os, random
from datetime import date

random.seed(7)
RAW = os.path.join(os.getcwd(), "data", "raw")
os.makedirs(RAW, exist_ok=True)

SUPPLIERS = ["Capita PLC", "Fujitsu Services", "Serco Group", "BAE Systems",
             "Deloitte LLP", "PwC", "Microsoft Ltd", "Methods Business", "Kainos"]
CATS = ["IT & Telecoms", "Consultancy", "Estates", "Staff & Agency", "Grants"]


def amount(allow_credit=True):
    style = random.random()
    val = round(random.uniform(25000, 950000), 2)
    if allow_credit and style < 0.06:
        return f"({val:,.2f})"          # credit / refund, legitimate
    if style < 0.35:
        return f"\u00A3{val:,.2f}"       # £1,234.56
    if style < 0.6:
        return f"{val:,.2f}"             # 1,234.56
    return f"{val:.2f}"                  # 1234.56


def dt(style):
    d = date(2026, random.randint(4, 5), random.randint(1, 28))
    return d.strftime({"uk": "%d/%m/%Y", "iso": "%Y-%m-%d", "mon": "%d-%b-%Y"}[style])


def write(name, header, rows):
    with open(os.path.join(RAW, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote {name:42s} {len(rows)} rows")


def cab_rows(mth, n, drift=False):
    rows = []
    for i in range(n):
        r = [f"CO-{mth}{'D' if drift else ''}-{i:04d}", random.choice(SUPPLIERS), dt("uk"),
             amount(), random.choice(CATS)]
        if drift:
            r += [f"\u00A3{round(random.uniform(0,40000),2):,.2f}", f"PRG{random.randint(100,999)}"]
        rows.append(r)
    # a duplicate transaction to exercise dedup
    if rows:
        rows.append(list(rows[0]))
    return rows


def mod_rows(mth, n, malformed=False):
    rows = [[f"MOD-{mth}-{i:04d}", random.choice(SUPPLIERS), dt("mon"),
             amount(), random.choice(CATS)] for i in range(n)]
    if malformed:
        rows += [
            ["MOD-bad-1", random.choice(SUPPLIERS), "31/02/2026", "INVALID", "Estates"],
            ["", random.choice(SUPPLIERS), dt("mon"), "1,000.00", "Grants"],
            ["MOD-bad-3", "", dt("mon"), "55,000.00", "IT & Telecoms"],
        ]
    return rows


def dhsc_rows(mth, n):
    return [[f"INV-{mth}-{i:04d}", random.choice(SUPPLIERS), dt("iso"),
             amount(), random.choice(CATS), f"\u00A3{round(random.uniform(0,40000),2):,.2f}"]
            for i in range(n)]


CAB_H = ["transaction_id", "supplier_name", "payment_date", "amount", "expense_area"]
CAB_DRIFT_H = CAB_H + ["VAT", "Programme Code"]   # VAT + Programme unmapped -> raw_extras
MOD_H = ["Ref", "Vendor", "Payment Date", "Gross (\u00A3)", "Cost Centre"]
DHSC_H = ["Invoice ID", "Supplier", "Date Paid", "Amount Paid", "Category", "VAT"]

spend = [
    ("cabinet_office_spend_2026_04.csv", CAB_H, cab_rows("2026-04", 250)),
    ("mod_spend_2026_04.csv", MOD_H, mod_rows("2026-04", 220)),
    ("dhsc_spend_2026_04.csv", DHSC_H, dhsc_rows("2026-04", 240)),
    ("cabinet_office_spend_2026_05.csv", CAB_H, cab_rows("2026-05", 260)),
    ("dhsc_spend_2026_05.csv", DHSC_H, dhsc_rows("2026-05", 250)),
    ("cabinet_office_spend_2026_05_schemadrift.csv", CAB_DRIFT_H, cab_rows("2026-05", 120, drift=True)),
    ("mod_spend_2026_05_malformed.csv", MOD_H, mod_rows("2026-05", 210, malformed=True)),
]
ids = []
for name, header, rows in spend:
    write(name, header, rows)
    ids += [r[0] for r in rows if r[0]]

# Internal approvals ledger + supplier risk reference: the sensitive sources that
# the open analytics layer must not expose and that Lake Formation ABAC governs.
REF = os.path.join(os.getcwd(), "data", "reference")
os.makedirs(REF, exist_ok=True)


def ref_write(name, header, rows):
    with open(os.path.join(REF, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote reference/{name:32s} {len(rows)} rows")


APPROVERS = ["a.okafor", "s.mills", "r.patel", "j.dunne", "l.zhang"]
OWNERS = ["Finance BP North", "Finance BP South", "Programme Office", "Estates Finance"]
STATUS = ["Approved", "Approved", "Approved", "Pending", "Rejected"]
NOTES = ["Approved under delegated authority", "Awaiting budget confirmation",
         "Flagged for spot check", "Within tolerance", "Escalated to budget owner"]
appr = [[tid, f"CC-{random.randint(100, 999)}", random.choice(APPROVERS),
         random.choice(OWNERS), random.choice(STATUS), random.choice(NOTES)]
        for tid in random.sample(ids, int(len(ids) * 0.85))]
ref_write("approvals_ledger.csv",
          ["transaction_id", "cost_centre", "approver", "budget_owner", "processing_status", "internal_notes"], appr)

RISK = ["Low", "Low", "Medium", "High"]
SUPCAT = ["Strategic", "Tactical", "Commodity"]
DD = ["DD completed 2026-01", "Enhanced checks pending", "Standard checks complete", "Under review"]
sup = [[s, random.choice(SUPCAT), random.choice(RISK), random.choice(DD)] for s in SUPPLIERS]
ref_write("supplier_reference.csv",
          ["supplier_name", "supplier_category", "risk_rating", "due_diligence_notes"], sup)

print(f"\n{len([f for f in os.listdir(RAW) if f.endswith('.csv')])} spend files + 2 reference files")
