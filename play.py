# play.py — Updated: exact venue/court list, safe Excel fallback, success message shown BELOW booking form.
import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime, date, time as dtime, timedelta
import altair as alt
from io import BytesIO
import importlib

# =====================================================
# CONFIG
# =====================================================
CSV_PATH = "bookings.csv"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# =====================================================
# VENUES / COURTS (exact list provided)
# =====================================================
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

def flatten_turf_label(city, venue, court):
    return f"{city} | {venue} | {court}"

PLATFORMS = ["Huddle", "KheloMore", "SportVot Direct", "Event Company", "Turf Owner (Direct)"]
PAYMENT_METHODS = ["Cash", "SV Paytm", "Huddle Payout", "KheloMore Payout", "Bank Transfer"]
STATUSES = ["Pending", "Paid", "Received in Bank"]

# =====================================================
# HELPERS
# =====================================================
def safe_rerun():
    """Rerun the app safely across different Streamlit versions."""
    try:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
        elif hasattr(st, "rerun"):
            st.rerun()
        else:
            st.experimental_set_query_params(_rerun=str(time.time()))
            st.stop()
    except Exception:
        try:
            st.experimental_set_query_params(_rerun=str(time.time()))
        except Exception:
            pass
        st.stop()

def ensure_csv_exists(path=CSV_PATH):
    if not os.path.exists(path):
        df = pd.DataFrame(columns=[
            "booking_id", "created_on", "date", "end_time",
            "city", "venue", "court", "turf_name",
            "platform", "payment_method", "amount", "amount_paid",
            "is_advance", "status", "remarks", "created_by", "booking_name"
        ])
        df.to_csv(path, index=False)

def load_data(path=CSV_PATH):
    ensure_csv_exists(path)
    df = pd.read_csv(path)
    for col, default in [
        ("end_time", ""), ("city", ""), ("venue", ""), ("court", ""),
        ("turf_name", ""), ("amount_paid", 0.0), ("is_advance", False),
        ("booking_name", "")
    ]:
        if col not in df.columns:
            df[col] = default

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "end_time" in df.columns:
        df["end_time"] = pd.to_datetime(df["end_time"], errors="coerce")
    if "created_on" in df.columns:
        df["created_on"] = pd.to_datetime(df["created_on"], errors="coerce")
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    if "amount_paid" in df.columns:
        df["amount_paid"] = pd.to_numeric(df["amount_paid"], errors="coerce").fillna(0)
    if "is_advance" in df.columns:
        df["is_advance"] = df["is_advance"].astype(bool)
    return df

def save_data(df, path=CSV_PATH):
    df2 = df.copy()
    for col in ["date", "end_time", "created_on"]:
        if col in df2.columns:
            df2[col] = pd.to_datetime(df2[col], errors="coerce").dt.strftime(DATE_FORMAT)
            df2[col] = df2[col].fillna("")
    if "is_advance" in df2.columns:
        df2["is_advance"] = df2["is_advance"].astype(bool)
    if "amount_paid" in df2.columns:
        df2["amount_paid"] = df2["amount_paid"].fillna(0)
    df2.to_csv(path, index=False)

def generate_booking_id(df):
    if df.shape[0] == 0:
        return "BK0001"
    existing = df.get("booking_id", pd.Series(dtype=str)).astype(str).str.extract(r"BK0*([0-9]+)$", expand=False).dropna()
    if existing.empty:
        return "BK0001"
    max_n = int(existing.astype(int).max())
    return f"BK{(max_n + 1):04d}"

def to_excel_bytes(df):
    """
    Return tuple (bytes, type) where type is "excel" if excel bytes produced, else "csv".
    Uses openpyxl if available; otherwise falls back to CSV bytes.
    """
    buffer = BytesIO()
    if importlib.util.find_spec("openpyxl") is not None:
        try:
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Report")
            buffer.seek(0)
            return buffer.getvalue(), "excel"
        except Exception:
            pass
    # fallback CSV
    return df.to_csv(index=False).encode("utf-8"), "csv"

# slot utilities
def generate_time_slots(start=dtime(6,0), end=dtime(23,30), step_minutes=30):
    slots = []
    cur = datetime.combine(date.today(), start)
    end_dt = datetime.combine(date.today(), end)
    while cur <= end_dt:
        slots.append(cur.time())
        cur += timedelta(minutes=step_minutes)
    return slots

def format_slot(t: dtime):
    return t.strftime("%H:%M")

def format_slot_range(t: dtime, duration_minutes: int = 30):
    end_time = (datetime.combine(date.today(), t) + timedelta(minutes=duration_minutes)).time()
    return f"{format_slot(t)}-{format_slot(end_time)}"

def slots_to_datetimes(slot_time: dtime, slot_date: date):
    return datetime.combine(slot_date, slot_time)

def ranges_overlap(a_start, a_end, b_start, b_end):
    return not (a_end <= b_start or a_start >= b_end)

def selected_slots_contiguous(selected_times):
    if not selected_times:
        return False
    sorted_times = sorted(selected_times)
    for i in range(1, len(sorted_times)):
        prev_dt = datetime.combine(date.today(), sorted_times[i-1])
        cur_dt = datetime.combine(date.today(), sorted_times[i])
        if (cur_dt - prev_dt) != timedelta(minutes=30):
            return False
    return True

# =====================================================
# STREAMLIT SETUP
# =====================================================
st.set_page_config(page_title="SportVot Play — Finance & Booking Tracker", layout="wide")
st.title("🏟️ SportVot Play — Finance & Booking Tracker")

# Keep Department radio in sidebar only
department = st.sidebar.radio("Select Department", ["Operations", "Finance"], key="dept_select")
st.sidebar.markdown("---")

# initialize session df
if "df" not in st.session_state:
    st.session_state.df = load_data()

# sensible defaults for form-level selectors
default_city = list(VENUES_BY_CITY.keys())[0]
default_venue = list(VENUES_BY_CITY[default_city].keys())[0]
default_court = VENUES_BY_CITY[default_city][default_venue][0]

if "form_city" not in st.session_state:
    st.session_state.form_city = default_city
if "form_venue" not in st.session_state:
    st.session_state.form_venue = default_venue
if "form_court" not in st.session_state:
    st.session_state.form_court = default_court

df = st.session_state.df

# =====================================================
# OPERATIONS SECTION
# =====================================================
if department == "Operations":
    st.subheader("🎯 Operations Section")
    tabs = st.tabs(["Ops Dashboard", "Booking Entry", "Ops Reports"])

    # --- OPS DASHBOARD ---
    with tabs[0]:
        st.header("📊 Operations Dashboard")
        date_from = st.date_input("From", value=date.today().replace(day=1), key="ops_dash_from")
        date_to = st.date_input("To", value=date.today(), key="ops_dash_to")

        # Dashboard uses current form-level selections if present
        sel_city = st.session_state.get("form_city", default_city)
        sel_venue = st.session_state.get("form_venue", default_venue)
        sel_court = st.session_state.get("form_court", default_court)

        df_dash = df.copy()
        if "city" in df_dash.columns:
            df_dash = df_dash[
                (df_dash["city"] == sel_city) &
                (df_dash["venue"] == sel_venue) &
                (df_dash["court"] == sel_court)
            ]
        else:
            df_dash = df_dash[df_dash["turf_name"] == flatten_turf_label(sel_city, sel_venue, sel_court)]

        df_dash["date_only"] = pd.to_datetime(df_dash["date"], errors="coerce").dt.date
        df_dash = df_dash[(df_dash["date_only"] >= date_from) & (df_dash["date_only"] <= date_to)]

        total_bookings = len(df_dash)
        total_amount = df_dash["amount"].sum()
        paid_amount = df_dash["amount_paid"].sum()
        pending_amount = total_amount - paid_amount

        c1, c2, c3 = st.columns(3)
        c1.metric("Bookings", f"{total_bookings:,}")
        c2.metric("Total Amount (₹)", f"{total_amount:,.0f}")
        c3.metric("Amount Paid (₹)", f"{paid_amount:,.0f}")

    # --- BOOKING ENTRY ---
    with tabs[1]:
        st.header("✍️ Booking Entry")

        # If a previous save set this flag, reset widget-backed session keys BEFORE widgets are created
        if st.session_state.get("should_clear_form", False):
            st.session_state.entry_selected_slots = []
            st.session_state.entry_booking_name = ""
            st.session_state.entry_amount = 1400
            st.session_state.entry_amount_paid = 0.0
            st.session_state.entry_remarks = ""
            st.session_state.entry_platform = PLATFORMS[0]
            st.session_state.entry_payment = PAYMENT_METHODS[0]
            # reset form-level selectors to defaults
            st.session_state.form_city = default_city
            st.session_state.form_venue = default_venue
            st.session_state.form_court = default_court
            del st.session_state["should_clear_form"]

        all_slots = generate_time_slots()  # 30-min slots

        # initialize defaults for widget-backed keys if missing
        if "entry_selected_slots" not in st.session_state:
            st.session_state.entry_selected_slots = []
        if "entry_booking_name" not in st.session_state:
            st.session_state.entry_booking_name = ""
        if "entry_amount" not in st.session_state:
            st.session_state.entry_amount = 1400
        if "entry_amount_paid" not in st.session_state:
            st.session_state.entry_amount_paid = 0.0
        if "entry_remarks" not in st.session_state:
            st.session_state.entry_remarks = ""
        if "entry_platform" not in st.session_state:
            st.session_state.entry_platform = PLATFORMS[0]
        if "entry_payment" not in st.session_state:
            st.session_state.entry_payment = PAYMENT_METHODS[0]

        # Booking form — includes form-level City/Venue/Court selectors
        with st.form("booking_form", clear_on_submit=False):
            # FORM-LEVEL cascading selectors
            form_city = st.selectbox("Select City", list(VENUES_BY_CITY.keys()),
                                     index=list(VENUES_BY_CITY.keys()).index(st.session_state.form_city) if st.session_state.form_city in VENUES_BY_CITY else 0,
                                     key="form_city")
            form_venues = list(VENUES_BY_CITY.get(form_city, {}).keys())
            default_venue_index = 0
            if st.session_state.form_venue in form_venues:
                default_venue_index = form_venues.index(st.session_state.form_venue)
            form_venue = st.selectbox("Select Venue", form_venues, index=default_venue_index, key="form_venue")
            form_courts = VENUES_BY_CITY.get(form_city, {}).get(form_venue, [])
            default_court_index = 0
            if st.session_state.form_court in form_courts:
                default_court_index = form_courts.index(st.session_state.form_court)
            form_court = st.selectbox("Select Court", form_courts, index=default_court_index, key="form_court")

            # rest of booking fields (note: removed is_advance and status here)
            booking_name = st.text_input("Booking Name / Team Name", value=st.session_state.entry_booking_name, key="entry_booking_name")
            b_date = st.date_input("Booking Date", value=date.today(), key="entry_date")

            # Determine booked ranges for selected form-level city/venue/court and date
            df_current = st.session_state.df.copy()
            if "city" in df_current.columns:
                turf_bookings = df_current[
                    (df_current["city"] == form_city) &
                    (df_current["venue"] == form_venue) &
                    (df_current["court"] == form_court)
                ].copy()
            else:
                turf_bookings = df_current[df_current["turf_name"] == flatten_turf_label(form_city, form_venue, form_court)].copy()

            turf_bookings["date_only"] = pd.to_datetime(turf_bookings["date"], errors="coerce").dt.date
            turf_bookings = turf_bookings[turf_bookings["date_only"] == b_date]

            booked_ranges = []
            for _, row in turf_bookings.iterrows():
                s = row.get("date")
                e = row.get("end_time")
                if pd.notnull(s) and pd.notnull(e):
                    booked_ranges.append((pd.to_datetime(s), pd.to_datetime(e)))

            available_slots = []
            for slot in all_slots:
                slot_dt = slots_to_datetimes(slot, b_date)
                slot_end = slot_dt + timedelta(minutes=30)
                conflict = False
                for br in booked_ranges:
                    if ranges_overlap(slot_dt, slot_end, br[0], br[1]):
                        conflict = True
                        break
                if not conflict:
                    available_slots.append(slot)

            if not available_slots:
                st.warning("No available time slots for this court & date. Choose another date or court.")

            slot_labels = [format_slot_range(s) for s in available_slots]
            selected_slot_labels = st.multiselect(
                "Select contiguous half-hour slots (e.g., 14:00-14:30, 14:30-15:00 for 1 hour)",
                slot_labels,
                default=st.session_state.entry_selected_slots,
                key="entry_selected_slots"
            ) if available_slots else []

            selected_slots = []
            for lbl in selected_slot_labels:
                try:
                    start_str = lbl.split("-")[0].strip()
                    selected_slots.append(datetime.strptime(start_str, "%H:%M").time())
                except Exception:
                    continue

            if selected_slot_labels and not selected_slots_contiguous(selected_slots):
                st.error("Selected slots are not contiguous. Please select contiguous half-hour slots (adjacent).")

            platform = st.selectbox("Platform", PLATFORMS, index=PLATFORMS.index(st.session_state.entry_platform) if st.session_state.entry_platform in PLATFORMS else 0, key="entry_platform")

            if platform == "Huddle":
                payment_method = "Huddle Payout"
            elif platform == "KheloMore":
                payment_method = "KheloMore Payout"
            elif platform == "Event Company":
                payment_method = "Bank Transfer"
            else:
                payment_method = st.selectbox("Payment Method", PAYMENT_METHODS, index=PAYMENT_METHODS.index(st.session_state.entry_payment) if st.session_state.entry_payment in PAYMENT_METHODS else 0, key="entry_payment")

            amount = st.number_input("Booking Amount (INR)", min_value=0, step=100, value=st.session_state.entry_amount, key="entry_amount")
            amount_paid = st.number_input("Amount Paid Now (INR)", min_value=0.0, step=100.0, value=st.session_state.entry_amount_paid, key="entry_amount_paid")
            remarks = st.text_input("Remarks (optional, max 100 chars)", value=st.session_state.entry_remarks, key="entry_remarks", max_chars=100)
            submitted = st.form_submit_button("➕ Add Booking")

            if submitted:
                # validations
                if not booking_name:
                    st.error("❌ Please enter Booking Name / Team Name.")
                elif not selected_slots:
                    st.error("❌ Please select at least one half-hour slot.")
                elif not selected_slots_contiguous(selected_slots):
                    st.error("❌ Slots must be contiguous.")
                elif amount <= 0:
                    st.error("❌ Please enter a valid amount greater than 0.")
                elif amount_paid > amount:
                    st.error("❌ Amount paid cannot exceed total amount.")
                else:
                    sorted_slots = sorted(selected_slots)
                    chosen_start_dt = slots_to_datetimes(sorted_slots[0], b_date)
                    chosen_end_dt = slots_to_datetimes(sorted_slots[-1], b_date) + timedelta(minutes=30)

                    # final overlap check against latest data
                    conflict = False
                    df_now = load_data()
                    if "city" in df_now.columns:
                        df_now_turf = df_now[
                            (df_now["city"] == form_city) &
                            (df_now["venue"] == form_venue) &
                            (df_now["court"] == form_court)
                        ].copy()
                    else:
                        df_now_turf = df_now[df_now["turf_name"] == flatten_turf_label(form_city, form_venue, form_court)].copy()
                    df_now_turf["date_only"] = pd.to_datetime(df_now_turf["date"], errors="coerce").dt.date
                    df_now_turf = df_now_turf[df_now_turf["date_only"] == b_date]
                    for _, row in df_now_turf.iterrows():
                        s = row.get("date")
                        e = row.get("end_time")
                        if pd.notnull(s) and pd.notnull(e):
                            s_dt = pd.to_datetime(s)
                            e_dt = pd.to_datetime(e)
                            if ranges_overlap(chosen_start_dt, chosen_end_dt, s_dt, e_dt):
                                conflict = True
                                break

                    if conflict:
                        st.error("❌ Selected slot(s) conflict with existing bookings. Please choose different slots.")
                    else:
                        df_current = df_now
                        new_id = generate_booking_id(df_current)
                        created_on = datetime.now().strftime(DATE_FORMAT)
                        turf_name = flatten_turf_label(form_city, form_venue, form_court)
                        # is_advance removed from form, default False; status default "Pending"
                        new_row = {
                            "booking_id": new_id,
                            "created_on": created_on,
                            "date": chosen_start_dt.strftime(DATE_FORMAT),
                            "end_time": chosen_end_dt.strftime(DATE_FORMAT),
                            "city": form_city,
                            "venue": form_venue,
                            "court": form_court,
                            "turf_name": turf_name,
                            "platform": platform,
                            "payment_method": payment_method,
                            "amount": float(amount),
                            "amount_paid": float(amount_paid),
                            "is_advance": False,
                            "status": "Pending",
                            "remarks": remarks,
                            "created_by": "Operations",
                            "booking_name": booking_name
                        }
                        df_current = pd.concat([df_current, pd.DataFrame([new_row])], ignore_index=True)
                        save_data(df_current)
                        # update in-memory dataset
                        st.session_state.df = load_data()
                        # store last booking into session_state so message persists after rerun
                        st.session_state.last_booking = {
                            "booking_id": new_id,
                            "city": form_city,
                            "venue": form_venue,
                            "court": form_court,
                            "time_range": f"{format_slot_range(sorted_slots[0], (len(sorted_slots) * 30))}",
                            "amount": int(amount),
                            "amount_paid": int(amount_paid)
                        }
                        # mark to clear on next run
                        st.session_state.should_clear_form = True
                        safe_rerun()

        # ===== Show success message BELOW the form (if present) =====
        if "last_booking" in st.session_state:
            lb = st.session_state.pop("last_booking")
            st.success(f"✅ Booking {lb.get('booking_id')} added for {lb.get('city')} | {lb.get('venue')} | {lb.get('court')} — "
                       f"{lb.get('time_range')} — ₹{int(lb.get('amount',0)):,} (Paid: ₹{int(lb.get('amount_paid',0)):,})")

    # --- OPS REPORTS ---
    with tabs[2]:
        st.header("📑 Ops Reports")
        rep_from = st.date_input("From", value=date.today().replace(day=1), key="ops_rep_from")
        rep_to = st.date_input("To", value=date.today(), key="ops_rep_to")

        # Use form-level selections for reports (fallback to defaults if not set)
        sel_city = st.session_state.get("form_city", default_city)
        sel_venue = st.session_state.get("form_venue", default_venue)
        sel_court = st.session_state.get("form_court", default_court)

        rep_df = st.session_state.df.copy()
        if "city" in rep_df.columns:
            rep_df = rep_df[
                (rep_df["city"] == sel_city) &
                (rep_df["venue"] == sel_venue) &
                (rep_df["court"] == sel_court)
            ].copy()
        else:
            rep_df = rep_df[rep_df["turf_name"] == flatten_turf_label(sel_city, sel_venue, sel_court)].copy()

        rep_df["date_only"] = pd.to_datetime(rep_df["date"], errors="coerce").dt.date
        rep_df = rep_df[(rep_df["date_only"] >= rep_from) & (rep_df["date_only"] <= rep_to)]
        st.write(f"Report rows: {len(rep_df)}")

        st.markdown("### 🕒 Visual Timeline — selected court & date")
        timeline_date = st.date_input("Timeline Date", value=date.today(), key="timeline_date")
        timeline_df = st.session_state.df.copy()
        if "city" in timeline_df.columns:
            timeline_df = timeline_df[
                (timeline_df["city"] == sel_city) &
                (timeline_df["venue"] == sel_venue) &
                (timeline_df["court"] == sel_court)
            ].copy()
        else:
            timeline_df = timeline_df[timeline_df["turf_name"] == flatten_turf_label(sel_city, sel_venue, sel_court)].copy()

        timeline_df["start_dt"] = pd.to_datetime(timeline_df["date"], errors="coerce")
        timeline_df["end_dt"] = pd.to_datetime(timeline_df["end_time"], errors="coerce")
        timeline_df["date_only"] = timeline_df["start_dt"].dt.date
        timeline_df = timeline_df[timeline_df["date_only"] == timeline_date]
        if not timeline_df.empty:
            chart_df = timeline_df[["booking_id", "booking_name", "start_dt", "end_dt", "status", "amount_paid", "amount"]].copy()
            chart = alt.Chart(chart_df).mark_bar().encode(
                x=alt.X("start_dt:T", title="Time"),
                x2="end_dt:T",
                y=alt.Y("booking_name:N", title="Booking"),
                color="status:N",
                tooltip=["booking_id", "booking_name", "start_dt", "end_dt", "amount", "amount_paid", "status"]
            ).properties(height=300)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No bookings for this court on timeline date.")

        st.dataframe(rep_df, height=300)

        st.markdown("### ✏️ Edit / Delete Booking")
        if not rep_df.empty:
            sel_booking_id = st.selectbox("Select Booking ID", rep_df["booking_id"].tolist(), key="ops_sel_booking")
            if sel_booking_id:
                sel_row = rep_df[rep_df["booking_id"] == sel_booking_id].iloc[0]
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Edit Booking", key="edit_booking_btn"):
                        with st.form("edit_form", clear_on_submit=False):
                            edit_booking_name = st.text_input("Booking Name", value=sel_row.get("booking_name", ""), key="edit_booking_name")
                            start_dt = sel_row["date"] if pd.notnull(sel_row["date"]) else pd.NaT
                            end_dt = sel_row["end_time"] if pd.notnull(sel_row["end_time"]) else pd.NaT
                            edit_date = st.date_input("Start Date", value=start_dt.date() if not pd.isna(start_dt) else date.today(), key="edit_date")
                            edit_start_time = st.time_input("Start Time", value=start_dt.time() if not pd.isna(start_dt) else dtime(9,0), key="edit_start_time")
                            edit_end_time = st.time_input("End Time", value=end_dt.time() if not pd.isna(end_dt) else dtime(10,0), key="edit_end_time")
                            edit_platform = st.selectbox("Platform", PLATFORMS, index=PLATFORMS.index(sel_row["platform"]) if sel_row["platform"] in PLATFORMS else 0, key="edit_platform")
                            edit_payment_method = st.selectbox("Payment Method", PAYMENT_METHODS, index=PAYMENT_METHODS.index(sel_row["payment_method"]) if sel_row["payment_method"] in PAYMENT_METHODS else 0, key="edit_payment_method")
                            edit_amount = st.number_input("Amount (INR)", min_value=0, step=100, value=float(sel_row.get("amount", 0)), key="edit_amount")
                            edit_amount_paid = st.number_input("Amount Paid (INR)", min_value=0.0, step=100.0, value=float(sel_row.get("amount_paid", 0)), key="edit_amount_paid")
                            edit_is_advance = st.checkbox("Is Advance", value=bool(sel_row.get("is_advance", False)), key="edit_is_advance")
                            edit_status = st.selectbox("Status", STATUSES, index=STATUSES.index(sel_row["status"]) if sel_row["status"] in STATUSES else 0, key="edit_status")
                            edit_remarks = st.text_input("Remarks", value=sel_row.get("remarks", ""), key="edit_remarks")
                            save_changes = st.form_submit_button("💾 Save Changes")
                            if save_changes:
                                new_start_dt = datetime.combine(edit_date, edit_start_time)
                                new_end_dt = datetime.combine(edit_date, edit_end_time)
                                if new_end_dt <= new_start_dt:
                                    st.error("End time must be after start time.")
                                else:
                                    df_now = load_data()
                                    others = df_now[
                                        (df_now["city"] == sel_row.get("city")) &
                                        (df_now["venue"] == sel_row.get("venue")) &
                                        (df_now["court"] == sel_row.get("court")) &
                                        (df_now["booking_id"] != sel_booking_id)
                                    ].copy() if "city" in df_now.columns else df_now[(df_now["turf_name"] == sel_row.get("turf_name")) & (df_now["booking_id"] != sel_booking_id)].copy()
                                    for _, r in others.iterrows():
                                        s = r.get("date"); e = r.get("end_time")
                                        if pd.notnull(s) and pd.notnull(e):
                                            s_dt = pd.to_datetime(s); e_dt = pd.to_datetime(e)
                                            if ranges_overlap(new_start_dt, new_end_dt, s_dt, e_dt):
                                                st.error("Updated time overlaps with existing booking: " + r.get("booking_id", ""))
                                                break
                                    else:
                                        df_now.loc[df_now["booking_id"] == sel_booking_id, ["booking_name", "date", "end_time", "platform", "payment_method", "amount", "amount_paid", "is_advance", "status", "remarks", "created_by"]] = [
                                            edit_booking_name,
                                            new_start_dt.strftime(DATE_FORMAT),
                                            new_end_dt.strftime(DATE_FORMAT),
                                            edit_platform,
                                            edit_payment_method,
                                            float(edit_amount),
                                            float(edit_amount_paid),
                                            bool(edit_is_advance),
                                            edit_status,
                                            edit_remarks,
                                            "Operations"
                                        ]
                                        save_data(df_now)
                                        st.session_state.df = load_data()
                                        st.success("✅ Booking updated.")
                                        safe_rerun()
                with col2:
                    if "delete_confirm" not in st.session_state:
                        st.session_state.delete_confirm = False
                    if st.button("Delete Booking", key="delete_booking_btn"):
                        st.session_state.delete_confirm = True
                    if st.session_state.get("delete_confirm", False):
                        st.warning(f"Are you sure you want to delete booking {sel_booking_id}? This action cannot be undone.")
                        confirm_delete = st.checkbox("Yes, permanently delete this booking", key="confirm_delete_checkbox")
                        if confirm_delete and st.button("Confirm Delete", key="confirm_delete_btn"):
                            df_now = load_data()
                            df_now = df_now[df_now["booking_id"] != sel_booking_id]
                            save_data(df_now)
                            st.session_state.df = load_data()
                            st.success(f"Deleted booking {sel_booking_id}.")
                            st.session_state.delete_confirm = False
                            safe_rerun()
        else:
            st.info("No bookings available for this court/date range.")

        csv_bytes = rep_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", data=csv_bytes, file_name="ops_report.csv", mime="text/csv", key="ops_csv")

# =====================================================
# FINANCE SECTION
# =====================================================
else:
    st.subheader("💼 Finance Section")
    tabs = st.tabs(["Finance Dashboard", "Reconciliation", "Finance Reports", "Data Backup"])

    # --- FINANCE DASHBOARD ---
    with tabs[0]:
        st.header("💰 Finance Dashboard — Overview")
        date_from = st.date_input("From", value=date.today().replace(day=1), key="fin_dash_from")
        date_to = st.date_input("To", value=date.today(), key="fin_dash_to")

        # FINANCE cascading selectors (same UX as booking form, but with "All" options)
        fin_city = st.selectbox("City", ["All Cities"] + list(VENUES_BY_CITY.keys()), key="fin_city")
        fin_venue = None
        fin_court = None
        if fin_city != "All Cities":
            venue_list = list(VENUES_BY_CITY.get(fin_city, {}).keys())
            fin_venue = st.selectbox("Venue", ["All Venues"] + venue_list, key="fin_venue")
            if fin_venue != "All Venues":
                court_list = VENUES_BY_CITY.get(fin_city, {}).get(fin_venue, [])
                fin_court = st.selectbox("Court", ["All Courts"] + court_list, key="fin_court")
            else:
                fin_court = st.selectbox("Court", ["All Courts"], key="fin_court")
        else:
            fin_venue = st.selectbox("Venue", ["All Venues"], key="fin_venue")
            fin_court = st.selectbox("Court", ["All Courts"], key="fin_court")

        df_dash = st.session_state.df.copy()
        df_dash["date_only"] = pd.to_datetime(df_dash["date"], errors="coerce").dt.date
        df_dash = df_dash[(df_dash["date_only"] >= date_from) & (df_dash["date_only"] <= date_to)]
        if fin_city != "All Cities":
            df_dash = df_dash[df_dash["city"] == fin_city]
        if fin_venue != "All Venues":
            df_dash = df_dash[df_dash["venue"] == fin_venue]
        if fin_court != "All Courts":
            df_dash = df_dash[df_dash["court"] == fin_court]

        total_bookings = len(df_dash)
        total_amount = df_dash["amount"].sum()
        total_paid = df_dash["amount_paid"].sum()
        pending_amount = total_amount - total_paid
        paid_amount = df_dash[df_dash["status"] == "Paid"]["amount_paid"].sum()
        received_amount = df_dash[df_dash["status"] == "Received in Bank"]["amount_paid"].sum()

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Bookings", f"{total_bookings:,}")
        c2.metric("Total (₹)", f"{total_amount:,.0f}")
        c3.metric("Paid (₹)", f"{total_paid:,.0f}")
        c4.metric("Pending (₹)", f"{pending_amount:,.0f}")
        c5.metric("Received (₹)", f"{received_amount:,.0f}")

        st.markdown("### 📊 Platform-wise Revenue (amount_paid)")
        if not df_dash.empty:
            chart_data = df_dash.groupby("platform")["amount_paid"].sum().reset_index()
            chart = alt.Chart(chart_data).mark_bar().encode(
                x=alt.X("platform", sort="-y"),
                y="amount_paid",
                tooltip=["platform", "amount_paid"]
            ).properties(width=700, height=350)
            st.altair_chart(chart, use_container_width=True)

        st.markdown("### 🏟️ Court-wise Paid Summary")
        turf_data = df_dash.groupby(["city","venue", "court"])["amount_paid"].sum().reset_index().sort_values("amount_paid", ascending=False)
        st.dataframe(turf_data, height=400)

    # --- RECONCILIATION ---
    with tabs[1]:
        st.header("✅ Reconciliation")
        rec_city = st.selectbox("City", ["All Cities"] + list(VENUES_BY_CITY.keys()), key="rec_city")
        if rec_city != "All Cities":
            rec_venue = st.selectbox("Venue", ["All Venues"] + list(VENUES_BY_CITY.get(rec_city, {}).keys()), key="rec_venue")
            if rec_venue != "All Venues":
                rec_court = st.selectbox("Court", ["All Courts"] + VENUES_BY_CITY.get(rec_city, {}).get(rec_venue, []), key="rec_court")
            else:
                rec_court = st.selectbox("Court", ["All Courts"], key="rec_court")
        else:
            rec_venue = st.selectbox("Venue", ["All Venues"], key="rec_venue")
            rec_court = st.selectbox("Court", ["All Courts"], key="rec_court")

        r_platform = st.selectbox("Platform", ["All Platforms"] + PLATFORMS, key="rec_platform")
        r_status = st.selectbox("Status", ["All Statuses"] + STATUSES, key="rec_status")
        r_from = st.date_input("From", value=date.today().replace(day=1), key="rec_from")
        r_to = st.date_input("To", value=date.today(), key="rec_to")

        dfr = st.session_state.df.copy()
        dfr["date_only"] = pd.to_datetime(dfr["date"], errors="coerce").dt.date
        dfr = dfr[(dfr["date_only"] >= r_from) & (dfr["date_only"] <= r_to)]
        if rec_city != "All Cities":
            dfr = dfr[dfr["city"] == rec_city]
        if rec_venue != "All Venues":
            dfr = dfr[dfr["venue"] == rec_venue]
        if rec_court != "All Courts":
            dfr = dfr[dfr["court"] == rec_court]
        if r_platform != "All Platforms":
            dfr = dfr[dfr["platform"] == r_platform]
        if r_status != "All Statuses":
            dfr = dfr[dfr["status"] == r_status]

        st.write(f"Showing {len(dfr)} bookings")
        if not dfr.empty:
            selected_ids = st.multiselect("Select Booking IDs", dfr["booking_id"].tolist(), key="rec_ids")
            st.dataframe(dfr, height=350)
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Mark as Paid", key="mark_paid"):
                    if selected_ids:
                        df_all = load_data()
                        df_all.loc[df_all["booking_id"].isin(selected_ids), ["status", "created_by"]] = ["Paid", "Finance"]
                        save_data(df_all)
                        st.session_state.df = load_data()
                        st.success(f"✅ {len(selected_ids)} booking(s) marked as Paid.")
                    else:
                        st.warning("⚠️ No bookings selected.")
            with c2:
                if st.button("Mark as Received in Bank", key="mark_received"):
                    if selected_ids:
                        df_all = load_data()
                        df_all.loc[df_all["booking_id"].isin(selected_ids), ["status", "created_by"]] = ["Received in Bank", "Finance"]
                        save_data(df_all)
                        st.session_state.df = load_data()
                        st.success(f"✅ {len(selected_ids)} booking(s) marked as Received in Bank.")
                    else:
                        st.warning("⚠️ No bookings selected.")

    # --- FINANCE REPORTS ---
    with tabs[2]:
        st.header("📑 Finance Reports — Latest Finance Entries")
        rep_city = st.selectbox("City", ["All Cities"] + list(VENUES_BY_CITY.keys()), key="rep_city")
        if rep_city != "All Cities":
            rep_venue = st.selectbox("Venue", ["All Venues"] + list(VENUES_BY_CITY.get(rep_city, {}).keys()), key="rep_venue")
            if rep_venue != "All Venues":
                rep_court = st.selectbox("Court", ["All Courts"] + VENUES_BY_CITY.get(rep_city, {}).get(rep_venue, []), key="rep_court")
            else:
                rep_court = st.selectbox("Court", ["All Courts"], key="rep_court")
        else:
            rep_venue = st.selectbox("Venue", ["All Venues"], key="rep_venue")
            rep_court = st.selectbox("Court", ["All Courts"], key="rep_court")

        rep_platform = st.selectbox("Platform", ["All Platforms"] + PLATFORMS, key="rep_platform")
        rep_status = st.selectbox("Payment Status", ["All Statuses", "Pending Only", "Paid Only", "Received in Bank Only"], key="rep_status")
        rep_from = st.date_input("From", value=date.today().replace(day=1), key="rep_from")
        rep_to = st.date_input("To", value=date.today(), key="rep_to")

        rep_df = st.session_state.df.copy()
        rep_df["date_only"] = pd.to_datetime(rep_df["date"], errors="coerce").dt.date
        rep_df = rep_df[(rep_df["date_only"] >= rep_from) & (rep_df["date_only"] <= rep_to)]
        if rep_city != "All Cities":
            rep_df = rep_df[rep_df["city"] == rep_city]
        if rep_venue != "All Venues":
            rep_df = rep_df[rep_df["venue"] == rep_venue]
        if rep_court != "All Courts":
            rep_df = rep_df[rep_df["court"] == rep_court]
        if rep_platform != "All Platforms":
            rep_df = rep_df[rep_df["platform"] == rep_platform]
        if rep_status == "Pending Only":
            rep_df = rep_df[rep_df["status"] == "Pending"]
        elif rep_status == "Paid Only":
            rep_df = rep_df[rep_df["status"] == "Paid"]
        elif rep_status == "Received in Bank Only":
            rep_df = rep_df[rep_df["status"] == "Received in Bank"]

        rep_df = rep_df.sort_values(by="created_on", ascending=False)
        st.write(f"Showing {len(rep_df)} finance reconciliation entries")

        if not rep_df.empty:
            col1, col2, col3 = st.columns(3)
            col1.metric("Pending (unpaid)", f"{rep_df[rep_df['status'] == 'Pending']['amount'].sum():,.0f} ₹")
            col2.metric("Paid (collected)", f"{rep_df['amount_paid'].sum():,.0f} ₹")
            col3.metric("Received (bank)", f"{rep_df[rep_df['status'] == 'Received in Bank']['amount_paid'].sum():,.0f} ₹")

            st.dataframe(rep_df, height=450)

            # Excel/CSV fallback using to_excel_bytes
            excel_bytes, btype = to_excel_bytes(rep_df)
            if btype == "excel":
                st.download_button("⬇️ Download Full Excel", data=excel_bytes, file_name="finance_latest.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="rep_excel")
            else:
                st.download_button("⬇️ Download CSV", data=excel_bytes, file_name="finance_latest.csv", mime="text/csv", key="rep_csv")
        else:
            st.info("No reconciliation entries found for Finance team.")

    # --- DATA BACKUP ---
    with tabs[3]:
        st.header("🗄️ Data Backup")
        df_all = st.session_state.df
        st.write(f"Total bookings in system: {len(df_all)}")
        st.dataframe(df_all.tail(20))
        all_csv = df_all.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Full CSV Backup", data=all_csv, file_name="bookings_backup.csv", mime="text/csv", key="backup_csv")
        excel_bytes, btype = to_excel_bytes(df_all)
        if btype == "excel":
            st.download_button("⬇️ Download Full Excel", data=excel_bytes, file_name="bookings_backup.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="backup_excel")
        else:
            st.download_button("⬇️ Download Full CSV (fallback)", data=excel_bytes, file_name="bookings_backup.csv", mime="text/csv", key="backup_csv2")

st.markdown("---")
st.caption("Built for SportVot Play — Venue list updated; Excel fallback; booking success message shown below the form")
