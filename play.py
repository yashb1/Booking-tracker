# play.py
# SportVot Play — Final update:
# - Advance payment method tracked
# - Fixed fin_sel_recon session_state error (fin_clear_flag)
# - Finance: totals (outstanding vs received) and reconciliation improvements
import streamlit as st
import pandas as pd
import sqlite3
import os
import time
from datetime import datetime, date, time as dtime, timedelta
import altair as alt
from io import BytesIO
import hashlib
import importlib

# ------------------------
# CONFIG
# ------------------------
DB_PATH = "bookings.db"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

DEMO_USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "ops": {"password": "ops123", "role": "operations"},
    "fin": {"password": "fin123", "role": "finance"}
}

VENUES_BY_CITY = {
    "Mumbai": {
        "ISF - Mira road": ["Turf 1", "Turf 2"],
        "Lush - Mira road (Cricket only)": ["Turf 1", "Turf 2", "Turf 3"],
        "Ninestar": ["Turf 1 (Football)", "Turf 2 (Cricket)", "Cricket nets 1", "Cricket nets 2", "Bowling machine"],
        "Player's turf Kanchpada": ["Turf 1", "Turf 2"],
        "Players turf Goregaon": ["Turf 1", "Turf 2", "Turf 3 (Cricket only)"],
        "Players turf Mittal": ["Turf 1", "Turf 2", "Turf 3", "Turf 4"],
        "Shanti Park Ghatkopar": ["Turf 1"],
    },
    "Delhi": {
        "Sportvot Play Base Chattarpur": ["Turf 1", "Turf 2"],
        "Sportvot Play Base Ghitorni": ["Turf 1"],
        "Sportvot Play Base Turf Pro": ["Turf 1 (Cricket)"]
    }
}

PLATFORMS = ["Huddle", "KheloMore", "SportVot Direct", "Event Company", "Turf Owner (Direct)"]
PAYMENT_METHODS = ["Cash", "SV Paytm", "Huddle Payout", "KheloMore Payout", "Bank Transfer"]
ADVANCE_METHODS = ["Cash", "GPay", "PhonePe", "Bank Transfer", "Cheque", "Other"]
STATUSES = ["Pending", "Paid", "Received in Bank"]

# ------------------------
# UTILITIES
# ------------------------
def safe_rerun():
    """Robust rerun helper that works across Streamlit versions."""
    try:
        if hasattr(st, "experimental_rerun"):
            try:
                st.experimental_rerun()
                return
            except Exception:
                pass
        if hasattr(st, "rerun"):
            try:
                st.rerun()
                return
            except Exception:
                pass
        try:
            st.experimental_set_query_params(_rerun=str(time.time()))
        except Exception:
            pass
        st.stop()
    except Exception:
        try:
            st.stop()
        except Exception:
            pass

def hash_password(plain: str) -> str:
    return hashlib.sha256(plain.encode("utf-8")).hexdigest()

def verify_password(plain: str, hashed: str) -> bool:
    return hash_password(plain) == hashed

def ensure_db(path=DB_PATH):
    init = not os.path.exists(path)
    conn = sqlite3.connect(path, check_same_thread=False)
    if init:
        create_schema(conn)
    ensure_schema_ext(conn)
    return conn

def create_schema(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE bookings (
        booking_id TEXT PRIMARY KEY,
        created_on TEXT,
        date TEXT,
        end_time TEXT,
        city TEXT,
        venue TEXT,
        court TEXT,
        turf_name TEXT,
        platform TEXT,
        payment_method TEXT,
        amount REAL,
        amount_paid REAL,
        is_advance INTEGER,
        status TEXT,
        remarks TEXT,
        created_by TEXT,
        booking_name TEXT
    )
    """)
    conn.commit()

def ensure_schema_ext(conn):
    """
    Add reconciliation-related columns and advance_method if missing:
      - reconciled (INTEGER 0/1)
      - bank_ref (TEXT)
      - reconciled_on (TEXT)
      - advance_method (TEXT)
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(bookings)")
    cols = [r[1] for r in cur.fetchall()]
    if "reconciled" not in cols:
        cur.execute("ALTER TABLE bookings ADD COLUMN reconciled INTEGER DEFAULT 0")
    if "bank_ref" not in cols:
        cur.execute("ALTER TABLE bookings ADD COLUMN bank_ref TEXT DEFAULT ''")
    if "reconciled_on" not in cols:
        cur.execute("ALTER TABLE bookings ADD COLUMN reconciled_on TEXT DEFAULT ''")
    if "advance_method" not in cols:
        cur.execute("ALTER TABLE bookings ADD COLUMN advance_method TEXT DEFAULT ''")
    conn.commit()

def get_next_booking_id(conn):
    cur = conn.cursor()
    cur.execute("SELECT booking_id FROM bookings ORDER BY booking_id DESC LIMIT 1")
    r = cur.fetchone()
    if not r:
        return "BK0001"
    last = r[0]
    try:
        n = int(last.replace("BK",""))
        return f"BK{n+1:04d}"
    except Exception:
        return "BK0001"

def add_booking_db(conn, row: dict):
    cur = conn.cursor()
    # Insert including extended fields
    cur.execute("""
    INSERT INTO bookings (booking_id, created_on, date, end_time, city, venue, court, turf_name,
                          platform, payment_method, amount, amount_paid, is_advance, status, remarks, created_by, booking_name,
                          reconciled, bank_ref, reconciled_on, advance_method)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        row["booking_id"],
        row["created_on"],
        row["date"],
        row["end_time"],
        row["city"],
        row["venue"],
        row["court"],
        row["turf_name"],
        row["platform"],
        row["payment_method"],
        row["amount"],
        row["amount_paid"],
        1 if row.get("is_advance", False) else 0,
        row.get("status","Pending"),
        row.get("remarks",""),
        row.get("created_by",""),
        row.get("booking_name",""),
        1 if row.get("reconciled", False) else 0,
        row.get("bank_ref",""),
        row.get("reconciled_on",""),
        row.get("advance_method","")
    ))
    conn.commit()

def update_booking_db(conn, booking_id, updates: dict):
    keys = ", ".join([f"{k}=?" for k in updates.keys()])
    vals = list(updates.values()) + [booking_id]
    cur = conn.cursor()
    cur.execute(f"UPDATE bookings SET {keys} WHERE booking_id=?", vals)
    conn.commit()

def delete_booking_db(conn, booking_id):
    cur = conn.cursor()
    cur.execute("DELETE FROM bookings WHERE booking_id=?", (booking_id,))
    conn.commit()

def load_bookings_df(conn):
    try:
        df = pd.read_sql_query("SELECT * FROM bookings", conn, parse_dates=["date","end_time","created_on"])
    except Exception:
        df = pd.DataFrame(columns=[
            "booking_id","created_on","date","end_time","city","venue","court","turf_name",
            "platform","payment_method","amount","amount_paid","is_advance","status","remarks","created_by","booking_name",
            "reconciled","bank_ref","reconciled_on","advance_method"
        ])
    if "is_advance" in df.columns:
        df["is_advance"] = df["is_advance"].astype(bool)
    if "reconciled" in df.columns:
        df["reconciled"] = df["reconciled"].fillna(0).astype(int)
    if "amount_paid" in df.columns:
        df["amount_paid"] = df["amount_paid"].fillna(0).astype(float)
    if "amount" in df.columns:
        df["amount"] = df["amount"].fillna(0).astype(float)
    return df

def to_excel_bytes(df):
    buffer = BytesIO()
    if importlib.util.find_spec("openpyxl") is not None:
        try:
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Report")
            buffer.seek(0)
            return buffer.getvalue(), "excel"
        except Exception:
            pass
    return df.to_csv(index=False).encode("utf-8"), "csv"

# ---- Slot utilities ----
def generate_time_slots(start=dtime(6,0), end=dtime(23,30), step_minutes=30):
    slots = []
    cur = datetime.combine(date.today(), start)
    end_dt = datetime.combine(date.today(), end)
    while cur <= end_dt:
        slots.append(cur.time())
        cur += timedelta(minutes=step_minutes)
    return slots

def format_slot(t):
    return t.strftime("%H:%M")

def format_slot_range(t, duration_minutes=30):
    end_time = (datetime.combine(date.today(), t) + timedelta(minutes=duration_minutes)).time()
    return f"{format_slot(t)}-{format_slot(end_time)}"

def slots_to_dt(slot_time, slot_date):
    return datetime.combine(slot_date, slot_time)

def ranges_overlap(a_start, a_end, b_start, b_end):
    return not (a_end <= b_start or a_start >= b_end)

def selected_slots_contiguous(selected_times):
    if not selected_times:
        return False
    sorted_times = sorted(selected_times)
    for i in range(1, len(sorted_times)):
        prev = datetime.combine(date.today(), sorted_times[i-1])
        cur = datetime.combine(date.today(), sorted_times[i])
        if (cur - prev) != timedelta(minutes=30):
            return False
    return True

# ------------------------
# STREAMLIT UI & AUTH
# ------------------------
st.set_page_config(page_title="SportVot Play — Booking Tracker", layout="wide")
st.title("🏟️ SportVot Play — Booking Tracker")

# init DB connection & user store
if "conn" not in st.session_state:
    st.session_state.conn = ensure_db(DB_PATH)
if "user_store" not in st.session_state:
    store = {}
    for u, info in DEMO_USERS.items():
        store[u] = {"pw_hash": hash_password(info["password"]), "role": info["role"]}
    st.session_state.user_store = store
if "df" not in st.session_state:
    st.session_state.df = load_bookings_df(st.session_state.conn)

# LOGIN / LOGOUT UI
def login_form():
    st.sidebar.header("🔐 Login")
    user = st.sidebar.text_input("Username")
    pw = st.sidebar.text_input("Password", type="password")
    if st.sidebar.button("Login"):
        store = st.session_state.user_store
        if user in store and verify_password(pw, store[user]["pw_hash"]):
            st.session_state.user = user
            st.session_state.role = store[user]["role"]
            st.sidebar.success(f"Signed in as {user} ({st.session_state.role})")
            # init form selectors on first login (set defaults BEFORE widgets render)
            if "form_city" not in st.session_state:
                st.session_state.form_city = list(VENUES_BY_CITY.keys())[0]
            if "form_venue" not in st.session_state:
                st.session_state.form_venue = list(VENUES_BY_CITY[st.session_state.form_city].keys())[0]
            if "form_court" not in st.session_state:
                st.session_state.form_court = VENUES_BY_CITY[st.session_state.form_city][st.session_state.form_venue][0]
            safe_rerun()
        else:
            st.sidebar.error("Invalid username or password")

def logout():
    if st.sidebar.button("Logout"):
        for k in ["user","role"]:
            if k in st.session_state:
                del st.session_state[k]
        safe_rerun()

# require login
if "user" not in st.session_state:
    login_form()
    st.sidebar.markdown("---")
    st.sidebar.write("Need help? Ask admin to create users.")
    st.stop()
else:
    st.sidebar.write(f"Signed in: **{st.session_state.user}** — *{st.session_state.role}*")
    logout()
    st.sidebar.markdown("---")
    if st.session_state.role == "admin":
        st.sidebar.write("Admin: create users in Admin tab.")

conn = st.session_state.conn
role = st.session_state.role
user = st.session_state.user

# Today's summary - simplified (no Venue/Court breakdown)
st.markdown("## Today's Summary")
today = date.today()
cur = conn.cursor()
cur.execute("SELECT COUNT(*), COALESCE(SUM(amount),0), COALESCE(SUM(amount_paid),0) FROM bookings WHERE date(date) = ?", (today.strftime("%Y-%m-%d"),))
cnt, total_amt, total_paid = cur.fetchone()
c1, c2, c3 = st.columns(3)
c1.metric("Bookings today", f"{cnt}")
c2.metric("Total amount (today)", f"₹{int(total_amt):,}")
c3.metric("Amount paid (today)", f"₹{int(total_paid):,}")

# Main tabs (role-based)
tabs = st.tabs(
    ["Dashboard","Booking Entry","Reports","Finance","Admin"]
    if role=="admin" else
    (["Dashboard","Booking Entry","Reports","Finance"] if role=="operations" else ["Dashboard","Reports","Finance"])
)

# ------------------------
# Dashboard
# ------------------------
with tabs[0]:
    st.header("📊 Dashboard")
    dfrom = st.date_input("From", value=date.today().replace(day=1), key="dash_from")
    dto = st.date_input("To", value=date.today(), key="dash_to")
    df_all = load_bookings_df(conn)
    if not df_all.empty:
        df_all["date_only"] = pd.to_datetime(df_all["date"], errors="coerce").dt.date
        df_f = df_all[(df_all["date_only"] >= dfrom) & (df_all["date_only"] <= dto)]
    else:
        df_f = df_all
    st.write(f"Showing {len(df_f)} bookings")
    if not df_f.empty:
        agg = df_f.groupby(["venue","court"])["amount_paid"].sum().reset_index().sort_values("amount_paid", ascending=False)
        st.dataframe(agg, height=300)
    else:
        st.info("No bookings for selected range.")

# ------------------------
# Booking Entry (operations & admin)
# ------------------------
if role in ("operations","admin"):
    with tabs[1]:
        st.header("✍️ Booking Entry")
        all_slots = generate_time_slots()

        # Initialize defaults & clear flags BEFORE widgets render
        if "form_city" not in st.session_state:
            st.session_state.form_city = list(VENUES_BY_CITY.keys())[0]
        if "form_venue" not in st.session_state:
            st.session_state.form_venue = list(VENUES_BY_CITY[st.session_state.form_city].keys())[0]
        if "form_court" not in st.session_state:
            st.session_state.form_court = VENUES_BY_CITY[st.session_state.form_city][st.session_state.form_venue][0]
        if "prev_city" not in st.session_state:
            st.session_state.prev_city = st.session_state.form_city

        # Clear-on-submit handling: reset entry fields BEFORE rendering widgets if flag present
        if st.session_state.get("should_clear_form"):
            st.session_state.entry_selected_slots = []
            st.session_state.entry_booking_name = ""
            st.session_state.entry_amount = 1400
            st.session_state.entry_amount_paid = 0.0
            st.session_state.entry_remarks = ""
            st.session_state.entry_platform = PLATFORMS[0]
            st.session_state.entry_advance_method = ADVANCE_METHODS[0]
            del st.session_state["should_clear_form"]

        # ensure entry defaults exist
        st.session_state.setdefault("entry_selected_slots", [])
        st.session_state.setdefault("entry_booking_name", "")
        st.session_state.setdefault("entry_amount", 1400)
        st.session_state.setdefault("entry_amount_paid", 0.0)
        st.session_state.setdefault("entry_remarks", "")
        st.session_state.setdefault("entry_platform", PLATFORMS[0])
        st.session_state.setdefault("entry_advance_method", ADVANCE_METHODS[0])

        with st.form("booking_form", clear_on_submit=False):
            # ---------- SAFE City -> Venue -> Court (resets when city changes) ----------
            form_city = st.selectbox(
                "Select City",
                list(VENUES_BY_CITY.keys()),
                index=list(VENUES_BY_CITY.keys()).index(st.session_state["form_city"]),
                key="form_city"
            )

            # If city changed since last run, reset form_venue & form_court BEFORE creating their widgets
            if form_city != st.session_state.get("prev_city"):
                venues_for_city = list(VENUES_BY_CITY.get(form_city, {}).keys())
                if venues_for_city:
                    st.session_state["form_venue"] = venues_for_city[0]
                    courts_for_venue = VENUES_BY_CITY.get(form_city, {}).get(st.session_state["form_venue"], [])
                    st.session_state["form_court"] = courts_for_venue[0] if courts_for_venue else None
                else:
                    st.session_state["form_venue"] = None
                    st.session_state["form_court"] = None
                st.session_state["prev_city"] = form_city

            _all_venues = list(VENUES_BY_CITY.get(form_city, {}).keys())
            if not _all_venues:
                st.error("No venues configured for this city.")
                form_venue = None
            else:
                if st.session_state.get("form_venue") not in _all_venues:
                    st.session_state["form_venue"] = _all_venues[0]
                form_venue = st.selectbox("Select Venue", _all_venues, index=_all_venues.index(st.session_state["form_venue"]), key="form_venue")

            _all_courts = VENUES_BY_CITY.get(form_city, {}).get(form_venue, []) if form_venue else []
            if not _all_courts:
                st.warning("No courts/turfs configured for this venue.")
                form_court = None
            else:
                if st.session_state.get("form_court") not in _all_courts:
                    st.session_state["form_court"] = _all_courts[0]
                form_court = st.selectbox("Select Court / Turf", _all_courts, index=_all_courts.index(st.session_state["form_court"]), key="form_court")
            # -------------------------------------------------------------------------

            booking_name = st.text_input("Booking Name / Team Name", value=st.session_state["entry_booking_name"], key="entry_booking_name")
            b_date = st.date_input("Booking Date", value=date.today(), key="entry_date")

            # Determine booked ranges for selected court & date
            df_now = load_bookings_df(conn)
            if not df_now.empty:
                df_now["date_only"] = pd.to_datetime(df_now["date"], errors="coerce").dt.date
            turf_bookings = df_now[
                (df_now.get("city") == form_city) &
                (df_now.get("venue") == form_venue) &
                (df_now.get("court") == form_court) &
                (df_now.get("date_only") == b_date)
            ] if not df_now.empty else pd.DataFrame()

            booked_ranges = []
            for _, r in turf_bookings.iterrows():
                s = r["date"]; e = r["end_time"]
                if pd.notnull(s) and pd.notnull(e):
                    booked_ranges.append((pd.to_datetime(s), pd.to_datetime(e)))

            available_slots = []
            for slot in all_slots:
                sdt = slots_to_dt(slot, b_date)
                edt = sdt + timedelta(minutes=30)
                conflict=False
                for br in booked_ranges:
                    if ranges_overlap(sdt, edt, br[0], br[1]):
                        conflict=True; break
                if not conflict:
                    available_slots.append(slot)

            if not available_slots:
                st.warning("No available time slots for this court & date. Choose another date/court.")

            slot_labels = [format_slot_range(s) for s in available_slots]
            selected_slot_labels = st.multiselect("Select contiguous half-hour slots (multi-select)", slot_labels, default=st.session_state.get("entry_selected_slots", []), key="entry_selected_slots") if available_slots else []
            selected_slots=[]
            for lbl in selected_slot_labels:
                try:
                    start_str = lbl.split("-")[0].strip()
                    selected_slots.append(datetime.strptime(start_str, "%H:%M").time())
                except:
                    pass

            if selected_slot_labels and not selected_slots_contiguous(selected_slots):
                st.error("Selected slots must be contiguous (adjacent half-hour slots).")

            platform = st.selectbox("Platform", PLATFORMS, index=PLATFORMS.index(st.session_state["entry_platform"]), key="entry_platform")
            if platform == "Huddle":
                payment_method = "Huddle Payout"
            elif platform == "KheloMore":
                payment_method = "KheloMore Payout"
            elif platform == "Event Company":
                payment_method = "Bank Transfer"
            else:
                payment_method = st.selectbox("Payment Method", PAYMENT_METHODS, index=0, key="entry_payment")

            amount = st.number_input("Booking Amount (INR)", min_value=0, step=100, value=st.session_state["entry_amount"], key="entry_amount")
            amount_paid = st.number_input("Amount Paid Now (INR)", min_value=0.0, step=100.0, value=st.session_state["entry_amount_paid"], key="entry_amount_paid")
            # Show advance payment method selector only when some amount is paid now
            if float(amount_paid) > 0:
                # make sure default exists before widget
                st.session_state.setdefault("entry_advance_method", ADVANCE_METHODS[0])
                advance_method = st.selectbox("Advance payment received via", ADVANCE_METHODS, index=ADVANCE_METHODS.index(st.session_state["entry_advance_method"]) if st.session_state.get("entry_advance_method") in ADVANCE_METHODS else 0, key="entry_advance_method")
            else:
                advance_method = st.session_state.get("entry_advance_method", ADVANCE_METHODS[0])

            remarks = st.text_input("Remarks (optional)", value=st.session_state["entry_remarks"], key="entry_remarks", max_chars=200)
            submitted = st.form_submit_button("➕ Add Booking")

            if submitted:
                # validations
                if not booking_name:
                    st.error("Please enter a booking name")
                elif not selected_slots:
                    st.error("Select at least one slot")
                elif not selected_slots_contiguous(selected_slots):
                    st.error("Slots must be contiguous")
                elif amount <= 0:
                    st.error("Enter a valid amount")
                elif amount_paid > amount:
                    st.error("Amount paid cannot exceed amount")
                else:
                    sorted_slots = sorted(selected_slots)
                    chosen_start = slots_to_dt(sorted_slots[0], b_date)
                    chosen_end = slots_to_dt(sorted_slots[-1], b_date) + timedelta(minutes=30)
                    # final overlap check
                    conflict=False
                    cur = conn.cursor()
                    cur.execute("SELECT date, end_time FROM bookings WHERE city=? AND venue=? AND court=? AND date(date)=?", (form_city, form_venue, form_court, b_date.strftime("%Y-%m-%d")))
                    existing = cur.fetchall()
                    for ex in existing:
                        s_dt = pd.to_datetime(ex[0])
                        e_dt = pd.to_datetime(ex[1])
                        if ranges_overlap(chosen_start, chosen_end, s_dt, e_dt):
                            conflict=True; break
                    if conflict:
                        st.error("Selected slot conflicts with existing booking.")
                    else:
                        new_id = get_next_booking_id(conn)
                        created_on = datetime.now().strftime(DATE_FORMAT)
                        turf_name = f"{form_city} | {form_venue} | {form_court}"
                        new_row = {
                            "booking_id": new_id,
                            "created_on": created_on,
                            "date": chosen_start.strftime(DATE_FORMAT),
                            "end_time": chosen_end.strftime(DATE_FORMAT),
                            "city": form_city,
                            "venue": form_venue,
                            "court": form_court,
                            "turf_name": turf_name,
                            "platform": platform,
                            "payment_method": payment_method,
                            "amount": float(amount),
                            "amount_paid": float(amount_paid),
                            "is_advance": True if float(amount_paid) > 0 else False,
                            "status": "Pending" if float(amount_paid) < float(amount) else "Received in Bank",
                            "remarks": remarks,
                            "created_by": user,
                            "booking_name": booking_name,
                            # Reconciled if fully paid immediately
                            "reconciled": 1 if float(amount_paid) >= float(amount) and amount > 0 else 0,
                            "bank_ref": "" if float(amount_paid) == 0 else "",
                            "reconciled_on": (datetime.now().strftime(DATE_FORMAT) if float(amount_paid) >= float(amount) and amount > 0 else ""),
                            "advance_method": advance_method if float(amount_paid) > 0 else ""
                        }
                        add_booking_db(conn, new_row)
                        st.session_state.df = load_bookings_df(conn)
                        st.session_state.last_booking = {
                            "booking_id": new_id,
                            "city": form_city, "venue": form_venue, "court": form_court,
                            "time_range": f"{format_slot_range(sorted_slots[0], (len(sorted_slots)*30))}",
                            "amount": int(amount), "amount_paid": int(amount_paid)
                        }
                        st.session_state.should_clear_form = True
                        safe_rerun()

        # show success below the form if present
        if "last_booking" in st.session_state:
            lb = st.session_state.pop("last_booking")
            st.success(f"✅ Booking {lb['booking_id']} added for {lb['city']} | {lb['venue']} | {lb['court']} — {lb['time_range']} — ₹{lb['amount']:,} (Paid: ₹{lb['amount_paid']:,})")

# ------------------------
# Reports tab
# ------------------------
reports_tab_index = 2 if role in ("operations","admin") else 1
with tabs[reports_tab_index]:
    st.header("📑 Reports")
    r_from = st.date_input("From", value=date.today().replace(day=1), key="rep_from")
    r_to = st.date_input("To", value=date.today(), key="rep_to")
    r_city = st.selectbox("City", ["All Cities"] + list(VENUES_BY_CITY.keys()), key="r_city")
    if r_city != "All Cities":
        v_list = ["All Venues"] + list(VENUES_BY_CITY.get(r_city, {}).keys())
        r_venue = st.selectbox("Venue", v_list, key="r_venue")
        if r_venue != "All Venues":
            r_court = st.selectbox("Court", ["All Courts"] + VENUES_BY_CITY.get(r_city, {}).get(r_venue, []), key="r_court")
        else:
            r_court = st.selectbox("Court", ["All Courts"], key="r_court")
    else:
        r_venue = st.selectbox("Venue", ["All Venues"], key="r_venue")
        r_court = st.selectbox("Court", ["All Courts"], key="r_court")

    df_all = load_bookings_df(conn)
    if not df_all.empty:
        df_all["date_only"] = pd.to_datetime(df_all["date"], errors="coerce").dt.date
        df_rep = df_all[(df_all["date_only"] >= r_from) & (df_all["date_only"] <= r_to)]
    else:
        df_rep = df_all
    if r_city != "All Cities":
        df_rep = df_rep[df_rep["city"] == r_city]
    if r_venue != "All Venues":
        df_rep = df_rep[df_rep["venue"] == r_venue]
    if r_court != "All Courts":
        df_rep = df_rep[df_rep["court"] == r_court]

    st.write(f"Rows: {len(df_rep)}")
    st.dataframe(df_rep, height=350)

    # Timeline for selected court & date
    st.markdown("### 🕒 Visual Timeline")
    t_date = st.date_input("Timeline Date", value=date.today(), key="timeline_date")
    sel_city = st.session_state.get("form_city", list(VENUES_BY_CITY.keys())[0])
    sel_venue = st.session_state.get("form_venue", list(VENUES_BY_CITY[sel_city].keys())[0])
    sel_court = st.session_state.get("form_court", VENUES_BY_CITY[sel_city][sel_venue][0])
    timeline_df = df_all.copy() if not df_all.empty else pd.DataFrame()
    if not timeline_df.empty:
        timeline_df["start_dt"] = pd.to_datetime(timeline_df["date"], errors="coerce")
        timeline_df["end_dt"] = pd.to_datetime(timeline_df["end_time"], errors="coerce")
        timeline_df["date_only"] = timeline_df["start_dt"].dt.date
        tdf = timeline_df[
            (timeline_df["date_only"] == t_date) &
            (timeline_df["city"] == sel_city) &
            (timeline_df["venue"] == sel_venue) &
            (timeline_df["court"] == sel_court)
        ]
        if not tdf.empty:
            chart = alt.Chart(tdf).mark_bar().encode(
                x=alt.X("start_dt:T", title="Start"),
                x2="end_dt:T",
                y=alt.Y("booking_name:N", title="Booking"),
                color="status:N",
                tooltip=["booking_id","booking_name","start_dt","end_dt","amount","amount_paid","status"]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No bookings on timeline for selected court/date.")
    else:
        st.info("No bookings in system yet.")

    excel_bytes, btype = to_excel_bytes(df_rep)
    if btype == "excel":
        st.download_button("⬇️ Download Excel", data=excel_bytes, file_name="report.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.download_button("⬇️ Download CSV", data=excel_bytes, file_name="report.csv", mime="text/csv")

# ------------------------
# Finance tab (payments & reconciliation)
# ------------------------
fin_tab_index = 3 if role in ("operations","admin") else 2
with tabs[fin_tab_index]:
    st.header("💼 Finance — Payments & Reconciliation")
    f_from = st.date_input("From", value=date.today().replace(day=1), key="fin_from")
    f_to = st.date_input("To", value=date.today(), key="fin_to")

    df_all = load_bookings_df(conn)
    if not df_all.empty:
        df_all["date_only"] = pd.to_datetime(df_all["date"], errors="coerce").dt.date
        df_f = df_all[(df_all["date_only"] >= f_from) & (df_all["date_only"] <= f_to)]
    else:
        df_f = df_all

    st.markdown("### Summary")
    total_bookings = len(df_f)
    total_amount = df_f["amount"].sum() if not df_f.empty else 0
    total_paid = df_f["amount_paid"].sum() if not df_f.empty else 0
    # total outstanding = total amount - total amount_paid
    total_outstanding = (df_f["amount"] - df_f["amount_paid"]).sum() if not df_f.empty else 0
    total_reconciled_count = int(df_f["reconciled"].sum()) if not df_f.empty and "reconciled" in df_f.columns else 0
    total_reconciled_amount = df_f[df_f["reconciled"] == 1]["amount_paid"].sum() if not df_f.empty and "reconciled" in df_f.columns else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Bookings (range)", f"{total_bookings}")
    col2.metric("Total amount", f"₹{int(total_amount):,}")
    col3.metric("Amount collected", f"₹{int(total_paid):,}")
    col4.metric("Outstanding", f"₹{int(total_outstanding):,}")

    col5, col6 = st.columns(2)
    col5.metric("Reconciled bookings", f"{total_reconciled_count}")
    col6.metric("Reconciled (collected)", f"₹{int(total_reconciled_amount):,}")

    st.markdown("### Unreconciled / Pending Payments")
    if not df_f.empty:
        unreconciled = df_f[(df_f["reconciled"] == 0) | (df_f["amount_paid"] < df_f["amount"])]
        if unreconciled.empty:
            st.success("All bookings in range are reconciled or fully paid.")
        else:
            st.dataframe(unreconciled[["booking_id","date","booking_name","venue","court","amount","amount_paid","status","reconciled","bank_ref","advance_method"]], height=250)

            if role in ("finance","admin"):
                st.markdown("#### Manual Reconciliation")

                # Before creating the multiselect widget, handle fin_clear_flag
                if st.session_state.pop("fin_clear_flag", False):
                    # set default selection empty BEFORE widget instantiation
                    st.session_state["fin_sel_recon"] = []

                # Create multiselect widget (key = "fin_sel_recon")
                sel = st.multiselect("Select Booking IDs to reconcile", unreconciled["booking_id"].tolist(), key="fin_sel_recon")

                bank_ref = st.text_input("Bank / Reference (enter bank reference)", key="fin_bank_ref")
                reconcile_date = st.date_input("Reconciled on", value=date.today(), key="fin_recon_date")

                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Mark Selected as Reconciled"):
                        if not sel:
                            st.warning("Select at least one booking to reconcile.")
                        else:
                            for bid in sel:
                                update_booking_db(conn, bid, {
                                    "reconciled": 1,
                                    "bank_ref": bank_ref,
                                    "reconciled_on": reconcile_date.strftime("%Y-%m-%d"),
                                    "status": "Received in Bank"
                                })
                            st.success(f"Reconciled {len(sel)} bookings.")
                            st.session_state.df = load_bookings_df(conn)
                            # clear selection on next run (flag)
                            st.session_state["fin_clear_flag"] = True
                            safe_rerun()

                with c2:
                    if st.button("Mark Selected as Received in Bank"):
                        if not sel:
                            st.warning("Select at least one booking.")
                        else:
                            for bid in sel:
                                update_booking_db(conn, bid, {
                                    "status": "Received in Bank",
                                    "reconciled": 1,
                                    "bank_ref": bank_ref,
                                    "reconciled_on": reconcile_date.strftime("%Y-%m-%d")
                                })
                            st.success(f"{len(sel)} bookings marked Received in Bank.")
                            st.session_state.df = load_bookings_df(conn)
                            st.session_state["fin_clear_flag"] = True
                            safe_rerun()

    else:
        st.info("No bookings in the selected finance range.")

    st.markdown("---")
    st.markdown("### Upload bank statement / reconciliation file (CSV)")
    st.write("CSV should have columns: booking_id (optional), amount, bank_ref, date")
    uploaded = st.file_uploader("Upload bank CSV (auto-match by booking_id or amount)", type=["csv"], key="fin_upload")
    if uploaded is not None:
        try:
            bank_df = pd.read_csv(uploaded)
            matched = 0; unmatched_rows = []
            for _, r in bank_df.iterrows():
                bid = str(r.get("booking_id","")).strip() if pd.notnull(r.get("booking_id","")) else ""
                amt = float(r.get("amount", 0))
                bank_ref = str(r.get("bank_ref","")) if pd.notnull(r.get("bank_ref","")) else ""
                recon_dt = r.get("date", date.today().strftime("%Y-%m-%d"))
                # Try match by booking_id first
                if bid:
                    cur = conn.cursor()
                    cur.execute("SELECT booking_id FROM bookings WHERE booking_id=?", (bid,))
                    if cur.fetchone():
                        update_booking_db(conn, bid, {"reconciled":1, "bank_ref":bank_ref, "reconciled_on":str(recon_dt), "status":"Received in Bank"})
                        matched += 1
                        continue
                # else try match by amount (amount tolerance)
                cur = conn.cursor()
                cur.execute("SELECT booking_id, amount, amount_paid FROM bookings WHERE ABS(amount - ?) <= 1.0 AND reconciled=0", (amt,))
                rows = cur.fetchall()
                if len(rows) == 1:
                    update_booking_db(conn, rows[0][0], {"reconciled":1, "bank_ref":bank_ref, "reconciled_on":str(recon_dt), "status":"Received in Bank"})
                    matched += 1
                else:
                    unmatched_rows.append(r.to_dict())
            st.success(f"Matched & reconciled {matched} rows. {len(unmatched_rows)} unmatched.")
            if unmatched_rows:
                st.write("Unmatched rows preview:")
                st.dataframe(pd.DataFrame(unmatched_rows))
            st.session_state.df = load_bookings_df(conn)
            # clear selection next run
            st.session_state["fin_clear_flag"] = True
            safe_rerun()
        except Exception as e:
            st.error(f"Upload failed: {e}")

    st.markdown("---")
    st.markdown("### Reconciliation ledger (reconciled bookings)")
    if not df_f.empty and "reconciled" in df_f.columns:
        ledger = df_f[df_f["reconciled"] == 1].sort_values("reconciled_on", ascending=False)
        if ledger.empty:
            st.info("No reconciled bookings in range.")
        else:
            st.dataframe(ledger[["booking_id","reconciled_on","bank_ref","date","booking_name","venue","court","amount","amount_paid","status","advance_method"]], height=300)
            excel_bytes, btype = to_excel_bytes(ledger)
            if btype == "excel":
                st.download_button("⬇️ Download Reconciliation Ledger (Excel)", data=excel_bytes, file_name="reconciliation.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.download_button("⬇️ Download Reconciliation Ledger (CSV)", data=excel_bytes, file_name="reconciliation.csv", mime="text/csv")
    else:
        st.info("No reconciled data available.")

# ------------------------
# Admin tab (admin only)
# ------------------------
if role == "admin":
    with tabs[-1]:
        st.header("🛠 Admin")
        st.subheader("User management (demo)")
        store = st.session_state.user_store
        users_table = pd.DataFrame([{"username":u,"role":store[u]["role"]} for u in store.keys()])
        st.dataframe(users_table)

        new_user = st.text_input("New username")
        new_pw = st.text_input("New password", type="password")
        new_role = st.selectbox("Role", ["operations","finance","admin"])
        if st.button("Create user"):
            if not new_user or not new_pw:
                st.error("Provide username & password")
            else:
                st.session_state.user_store[new_user] = {"pw_hash": hash_password(new_pw), "role": new_role}
                st.success(f"Created {new_user}")
                safe_rerun()

        st.markdown("---")
        st.subheader("Bulk upload bookings (CSV)")
        st.write("CSV must have headers: booking_id,created_on,date,end_time,city,venue,court,turf_name,platform,payment_method,amount,amount_paid,is_advance,status,remarks,created_by,booking_name,reconciled,bank_ref,reconciled_on,advance_method")
        uploaded = st.file_uploader("Upload CSV", type=["csv"], key="admin_upload")
        if uploaded is not None:
            try:
                df_up = pd.read_csv(uploaded)
                required = ["booking_id","date","end_time","city","venue","court","turf_name","amount"]
                if not all(c in df_up.columns for c in required):
                    st.error("Missing required columns")
                else:
                    cur = conn.cursor()
                    inserted = 0
                    for _, r in df_up.iterrows():
                        try:
                            cur.execute("""INSERT INTO bookings
                                (booking_id,created_on,date,end_time,city,venue,court,turf_name,platform,payment_method,amount,amount_paid,is_advance,status,remarks,created_by,booking_name,reconciled,bank_ref,reconciled_on,advance_method)
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (r.get("booking_id"),
                             r.get("created_on", datetime.now().strftime(DATE_FORMAT)),
                             r.get("date"),
                             r.get("end_time"),
                             r.get("city"),
                             r.get("venue"),
                             r.get("court"),
                             r.get("turf_name"),
                             r.get("platform", ""),
                             r.get("payment_method",""),
                             float(r.get("amount",0)),
                             float(r.get("amount_paid",0)),
                             1 if r.get("is_advance",False) else 0,
                             r.get("status","Pending"),
                             r.get("remarks",""),
                             r.get("created_by","admin"),
                             r.get("booking_name",""),
                             1 if r.get("reconciled",False) else 0,
                             r.get("bank_ref",""),
                             r.get("reconciled_on",""),
                             r.get("advance_method","")
                             ))
                            inserted += 1
                        except sqlite3.IntegrityError:
                            continue
                    conn.commit()
                    st.success(f"Inserted {inserted} rows")
                    st.session_state.df = load_bookings_df(conn)
            except Exception as e:
                st.error(f"Upload failed: {e}")

st.markdown("---")
st.caption("SportVot Play — Updated: advance_payment_method, fin_sel_recon fix (fin_clear_flag), finance totals/outstanding. For production: replace demo auth and SQLite with managed services.")
