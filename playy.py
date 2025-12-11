# play.py
# SportVot Play — Fix: canonical city/venue/court keys and safe resets
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
ADVANCE_METHODS = ["Cash", "UPI", "Cheque", "Bank Transfer"]
STATUSES = ["Pending", "Paid", "Received in Bank"]

def safe_rerun():
    try:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun(); return
        if hasattr(st, "rerun"):
            st.rerun(); return
        st.experimental_set_query_params(_rerun=str(time.time()))
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

def load_bookings_df(conn):
    try:
        df = pd.read_sql_query("SELECT * FROM bookings", conn, parse_dates=["date","end_time","created_on"])
    except Exception:
        df = pd.DataFrame()
    # safe types
    if "amount_paid" in df.columns:
        df["amount_paid"] = df["amount_paid"].fillna(0).astype(float)
    if "amount" in df.columns:
        df["amount"] = df["amount"].fillna(0).astype(float)
    if "is_advance" in df.columns:
        df["is_advance"] = df["is_advance"].fillna(0).astype(int).astype(bool)
    if "reconciled" in df.columns:
        df["reconciled"] = df["reconciled"].fillna(0).astype(int)
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

# slot utils
def generate_time_slots(start=dtime(6,0), end=dtime(23,30), step_minutes=30):
    slots=[]
    cur=datetime.combine(date.today(), start)
    end_dt=datetime.combine(date.today(), end)
    while cur<=end_dt:
        slots.append(cur.time())
        cur += timedelta(minutes=step_minutes)
    return slots

def format_slot(t): return t.strftime("%H:%M")
def format_slot_range(t, duration_minutes=30):
    end_time=(datetime.combine(date.today(), t) + timedelta(minutes=duration_minutes)).time()
    return f"{format_slot(t)}-{format_slot(end_time)}"
def slots_to_dt(slot_time, slot_date): return datetime.combine(slot_date, slot_time)
def ranges_overlap(a_start,a_end,b_start,b_end): return not (a_end <= b_start or a_start >= b_end)
def selected_slots_contiguous(selected_times):
    if not selected_times: return False
    s=sorted(selected_times)
    for i in range(1, len(s)):
        if (datetime.combine(date.today(), s[i]) - datetime.combine(date.today(), s[i-1])) != timedelta(minutes=30):
            return False
    return True

# UI init
st.set_page_config(page_title="SportVot Play — Booking Tracker", layout="wide")
st.title("🏟️ SportVot Play — Booking Tracker")

if "conn" not in st.session_state:
    st.session_state.conn = ensure_db(DB_PATH)
if "user_store" not in st.session_state:
    store={}
    for u,i in DEMO_USERS.items():
        store[u]={"pw_hash":hash_password(i["password"]), "role":i["role"]}
    st.session_state.user_store = store
if "df" not in st.session_state:
    st.session_state.df = load_bookings_df(st.session_state.conn)

# canonical session keys:
# form_city, form_venue, form_court
def login_form():
    st.sidebar.header("🔐 Login")
    user=st.sidebar.text_input("Username")
    pw=st.sidebar.text_input("Password", type="password")
    if st.sidebar.button("Login"):
        store=st.session_state.user_store
        if user in store and verify_password(pw, store[user]["pw_hash"]):
            st.session_state.user=user
            st.session_state.role=store[user]["role"]
            st.sidebar.success(f"Signed in as {user} ({st.session_state.role})")
            # initialize canonical keys BEFORE any widgets render
            st.session_state.setdefault("form_city", list(VENUES_BY_CITY.keys())[0])
            first_city = st.session_state["form_city"]
            st.session_state.setdefault("form_venue", list(VENUES_BY_CITY[first_city].keys())[0])
            st.session_state.setdefault("form_court", VENUES_BY_CITY[first_city][st.session_state["form_venue"]][0])
            st.session_state.setdefault("prev_city", st.session_state["form_city"])
            # booking defaults
            st.session_state.setdefault("entry_selected_slots", [])
            st.session_state.setdefault("entry_booking_name", "")
            st.session_state.setdefault("entry_amount", 1400)
            st.session_state.setdefault("entry_amount_paid", 0.0)
            st.session_state.setdefault("entry_remarks", "")
            st.session_state.setdefault("entry_platform", PLATFORMS[0])
            st.session_state.setdefault("entry_advance_method", ADVANCE_METHODS[0])
            safe_rerun()
        else:
            st.sidebar.error("Invalid username or password")

def logout():
    if st.sidebar.button("Logout"):
        for k in ["user","role"]:
            if k in st.session_state: del st.session_state[k]
        safe_rerun()

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

# Summary
st.markdown("## Today's Summary")
today=date.today()
cur=conn.cursor()
cur.execute("SELECT COUNT(*), COALESCE(SUM(amount),0), COALESCE(SUM(amount_paid),0) FROM bookings WHERE date(date)=?", (today.strftime("%Y-%m-%d"),))
cnt,total_amt,total_paid = cur.fetchone()
c1,c2,c3 = st.columns(3)
c1.metric("Bookings today", f"{cnt}")
c2.metric("Total amount (today)", f"₹{int(total_amt):,}")
c3.metric("Amount paid (today)", f"₹{int(total_paid):,}")

tabs = st.tabs(
    ["Dashboard","Booking Entry","Reports","Finance","Admin"]
    if role=="admin" else
    (["Dashboard","Booking Entry","Reports","Finance"] if role=="operations" else ["Dashboard","Reports","Finance"])
)

# Dashboard
with tabs[0]:
    st.header("📊 Dashboard")
    dfrom=st.date_input("From", value=date.today().replace(day=1), key="dash_from")
    dto=st.date_input("To", value=date.today(), key="dash_to")
    df_all = load_bookings_df(conn)
    if not df_all.empty:
        df_all["date_only"]=pd.to_datetime(df_all["date"], errors="coerce").dt.date
        df_f = df_all[(df_all["date_only"] >= dfrom) & (df_all["date_only"] <= dto)]
    else:
        df_f = df_all
    st.write(f"Showing {len(df_f)} bookings")
    if not df_f.empty:
        agg = df_f.groupby(["venue","court"])["amount_paid"].sum().reset_index().sort_values("amount_paid", ascending=False)
        st.dataframe(agg, height=300)
    else:
        st.info("No bookings for selected range.")

# Booking Entry - canonical keys used everywhere here
if role in ("operations","admin"):
    with tabs[1]:
        st.header("✍️ Booking Entry")

        with st.form("booking_form", clear_on_submit=True):
            # CITY - write to session_state["form_city"]
            cities = list(VENUES_BY_CITY.keys())
            # ensure valid default
            if st.session_state.get("form_city") not in cities:
                st.session_state["form_city"] = cities[0]
            st.selectbox("Select City", cities, index=cities.index(st.session_state["form_city"]), key="form_city")
            current_city = st.session_state["form_city"]

            # Before rendering venue/court, compute their lists for this city
            venues = list(VENUES_BY_CITY.get(current_city, {}).keys())
            if not venues:
                st.error("No venues configured for this city.")
                form_venue = None
                form_court = None
            else:
                # ensure form_venue is valid
                if st.session_state.get("form_venue") not in venues:
                    st.session_state["form_venue"] = venues[0]
                st.selectbox("Select Venue", venues, index=venues.index(st.session_state["form_venue"]), key="form_venue")
                form_venue = st.session_state["form_venue"]

                # courts for chosen venue
                courts = VENUES_BY_CITY[current_city].get(form_venue, [])
                if not courts:
                    st.warning("No courts/turfs configured for this venue.")
                    form_court = None
                else:
                    if st.session_state.get("form_court") not in courts:
                        st.session_state["form_court"] = courts[0]
                    st.selectbox("Select Court / Turf", courts, index=courts.index(st.session_state["form_court"]), key="form_court")
                    form_court = st.session_state["form_court"]

            # Booking details
            booking_name = st.text_input("Booking Name / Team Name", value=st.session_state.get("entry_booking_name",""), key="entry_booking_name")
            b_date = st.date_input("Booking Date", value=st.session_state.get("entry_date", date.today()), key="entry_date")

            # available slots for selected city/venue/court/date
            df_now = load_bookings_df(conn)
            if not df_now.empty:
                df_now["date_only"] = pd.to_datetime(df_now["date"], errors="coerce").dt.date
            turf_bookings = df_now[
                (df_now.get("city") == current_city) &
                (df_now.get("venue") == form_venue) &
                (df_now.get("court") == form_court) &
                (df_now.get("date_only") == b_date)
            ] if not df_now.empty else pd.DataFrame()

            booked_ranges=[]
            for _,r in turf_bookings.iterrows():
                s=r["date"]; e=r["end_time"]
                if pd.notnull(s) and pd.notnull(e):
                    booked_ranges.append((pd.to_datetime(s), pd.to_datetime(e)))

            all_slots = generate_time_slots()
            available_slots=[]
            for slot in all_slots:
                sdt = slots_to_dt(slot, b_date)
                edt = sdt + timedelta(minutes=30)
                conflict=False
                for br in booked_ranges:
                    if ranges_overlap(sdt,edt,br[0],br[1]):
                        conflict=True; break
                if not conflict:
                    available_slots.append(slot)

            if not available_slots:
                st.warning("No available time slots for this court & date. Choose another date/court.")

            slot_labels = [format_slot_range(s) for s in available_slots]
            selected_slot_labels = st.multiselect("Select contiguous half-hour slots (multi-select)", slot_labels, default=[], key="entry_selected_slots") if available_slots else []
            selected_slots=[]
            for lbl in selected_slot_labels:
                try:
                    start_str = lbl.split("-")[0].strip()
                    selected_slots.append(datetime.strptime(start_str, "%H:%M").time())
                except:
                    pass

            if selected_slot_labels and not selected_slots_contiguous(selected_slots):
                st.error("Selected slots must be contiguous (adjacent half-hour slots).")

            platform = st.selectbox("Platform", PLATFORMS, index=PLATFORMS.index(st.session_state.get("entry_platform", PLATFORMS[0])), key="entry_platform")
            if platform == "Huddle":
                payment_method = "Huddle Payout"
            elif platform == "KheloMore":
                payment_method = "KheloMore Payout"
            elif platform == "Event Company":
                payment_method = "Bank Transfer"
            else:
                payment_method = st.selectbox("Payment Method", PAYMENT_METHODS, index=0, key="entry_payment")

            # Advance amount visible always; advance method visible always in UI but saved only when amount_paid>0
            amount = st.number_input("Booking Amount (INR)", min_value=0, step=100, value=st.session_state.get("entry_amount", 1400), key="entry_amount")
            amount_paid = st.number_input("Advance Amount Received (INR)", min_value=0.0, step=50.0, value=st.session_state.get("entry_amount_paid", 0.0), key="entry_amount_paid")
            st.selectbox("Advance payment received via", ADVANCE_METHODS, index=ADVANCE_METHODS.index(st.session_state.get("entry_advance_method", ADVANCE_METHODS[0])), key="entry_advance_method")
            advance_method_ui = st.session_state.get("entry_advance_method", "")

            remarks = st.text_input("Remarks (optional)", value=st.session_state.get("entry_remarks",""), key="entry_remarks", max_chars=200)
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
                    cur=conn.cursor()
                    cur.execute("SELECT date,end_time FROM bookings WHERE city=? AND venue=? AND court=? AND date(date)=?", (current_city, form_venue, form_court, b_date.strftime("%Y-%m-%d")))
                    existing = cur.fetchall()
                    for ex in existing:
                        s_dt = pd.to_datetime(ex[0]); e_dt = pd.to_datetime(ex[1])
                        if ranges_overlap(chosen_start, chosen_end, s_dt, e_dt):
                            conflict=True; break
                    if conflict:
                        st.error("Selected slot conflicts with existing booking.")
                    else:
                        new_id = get_next_booking_id(conn)
                        created_on = datetime.now().strftime(DATE_FORMAT)
                        turf_name = f"{current_city} | {form_venue} | {form_court}"
                        advance_method_to_save = advance_method_ui if float(amount_paid) > 0 else ""
                        new_row = {
                            "booking_id": new_id,
                            "created_on": created_on,
                            "date": chosen_start.strftime(DATE_FORMAT),
                            "end_time": chosen_end.strftime(DATE_FORMAT),
                            "city": current_city,
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
                            "reconciled": 1 if float(amount_paid) >= float(amount) and amount > 0 else 0,
                            "bank_ref": "",
                            "reconciled_on": (datetime.now().strftime(DATE_FORMAT) if float(amount_paid) >= float(amount) and amount > 0 else ""),
                            "advance_method": advance_method_to_save
                        }
                        add_booking_db(conn, new_row)

                        # reset form session_state fields BEFORE rerun so widgets load fresh
                        st.session_state["entry_selected_slots"]=[]
                        st.session_state["entry_booking_name"]=""
                        st.session_state["entry_amount"]=1400
                        st.session_state["entry_amount_paid"]=0.0
                        st.session_state["entry_remarks"]=""
                        st.session_state["entry_platform"]=PLATFORMS[0]
                        st.session_state["entry_advance_method"]=ADVANCE_METHODS[0]
                        # keep city but revalidate venue/court defaults
                        st.session_state.setdefault("form_city", current_city)
                        vs = list(VENUES_BY_CITY.get(current_city, {}).keys())
                        if vs:
                            st.session_state["form_venue"] = vs[0]
                            st.session_state["form_court"] = VENUES_BY_CITY[current_city][vs[0]][0]
                        st.session_state["last_booking"] = {
                            "booking_id": new_id,
                            "city": current_city, "venue": form_venue, "court": form_court,
                            "time_range": f"{format_slot_range(sorted_slots[0], (len(sorted_slots)*30))}",
                            "amount": int(amount), "amount_paid": int(amount_paid)
                        }
                        safe_rerun()

        # show success below the form (after rerun)
        if "last_booking" in st.session_state:
            lb = st.session_state.pop("last_booking")
            st.success(f"✅ Booking {lb['booking_id']} added for {lb['city']} | {lb['venue']} | {lb['court']} — {lb['time_range']} — ₹{lb['amount']:,} (Paid: ₹{lb['amount_paid']:,})")

# Reports
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

    # Timeline using canonical session keys
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

# Finance
fin_tab_index = 3 if role in ("operations","admin") else 2
with tabs[fin_tab_index]:
    st.header("💼 Finance — Payments Overview")
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
    total_outstanding = (df_f["amount"] - df_f["amount_paid"]).sum() if not df_f.empty else 0
    total_reconciled_count = int(df_f["reconciled"].sum()) if not df_f.empty and "reconciled" in df_f.columns else 0
    total_reconciled_amount = df_f[df_f["reconciled"] == 1]["amount_paid"].sum() if not df_f.empty and "reconciled" in df_f.columns else 0

    col1,col2,col3,col4 = st.columns(4)
    col1.metric("Bookings (range)", f"{total_bookings}")
    col2.metric("Total amount", f"₹{int(total_amount):,}")
    col3.metric("Amount collected", f"₹{int(total_paid):,}")
    col4.metric("Outstanding", f"₹{int(total_outstanding):,}")

    if not df_f.empty:
        unreconciled = df_f[(df_f["amount_paid"] < df_f["amount"])]
        st.dataframe(unreconciled[["booking_id","date","booking_name","venue","court","amount","amount_paid","advance_method","status"]], height=300)
    else:
        st.info("No bookings in selected range.")

    if role not in ("finance","admin"):
        st.warning("⚠ Finance-only actions are hidden. Contact Finance / Admin.")
        st.stop()

    # finance only actions here (unchanged)
    # ... (same reconciliation/upload/ledger code as before) ...

# Admin (kept minimal)
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
                st.session_state.user_store[new_user] = {"pw_hash":hash_password(new_pw),"role":new_role}
                st.success(f"Created {new_user}")
                safe_rerun()

st.caption("SportVot Play — canonical keys: form_city, form_venue, form_court. Replace '... finance only actions ...' with your existing finance code if needed.")
