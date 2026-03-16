import streamlit as st
import psycopg2
import pandas as pd
from datetime import date

DATABASE_URL = "postgresql://postgres.shabbrpajsspqwcjdhgx:TMeCzgKhztrEJCln@aws-1-eu-west-1.pooler.supabase.com:5432/postgres"

FALLBACK_YEAR_LIST = [2025, 2024, 2023]
FALLBACK_PERIODS = {
    2025: [(1, "Q1 2025"), (2, "Q2 2025")],
    2024: [(3, "Q1 2024")],
    2023: [(4, "Q1 2023")],
}
FALLBACK_TARIFF_BLOCKS = {
    (1, 1): [(0, 50, 1.30), (51, 150, 1.50), (151, 250, 2.00), (251, None, 2.50)],
    (2, 1): [(0, 100, 1.25), (101, 250, 1.75), (251, None, 2.40)],
}
FALLBACK_SERVICE_CHARGE = {
    (1, 1): 10.0,
    (2, 1): 12.0,
}

LEVY_RATE = 0.05  # 5% levies and taxes on energy charge

def get_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        st.warning("DB connection failed; using local fallback data.")
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def get_available_years():
    conn = get_connection()
    if conn is None:
        return FALLBACK_YEAR_LIST
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT y.year
            FROM tariff_periods tp
            JOIN years y ON tp.year_id = y.id
            ORDER BY y.year DESC
        """)
        years = [row[0] for row in cur.fetchall()]
        return years if years else FALLBACK_YEAR_LIST
    finally:
        conn.close()

@st.cache_data(ttl=3600, show_spinner=False)
def get_periods_for_year(selected_year):
    conn = get_connection()
    if conn is None:
        return FALLBACK_PERIODS.get(selected_year, [])
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT tp.id, tp.period_name
            FROM tariff_periods tp
            JOIN years y ON tp.year_id = y.id
            WHERE y.year = %s
            ORDER BY tp.start_date
        """, (selected_year,))
        periods = cur.fetchall()
        return periods if periods else FALLBACK_PERIODS.get(selected_year, [])
    finally:
        conn.close()

@st.cache_data(ttl=3600, show_spinner=False)
def get_tariff_blocks(period_id, category_id):
    conn = get_connection()
    if conn is None:
        rows = FALLBACK_TARIFF_BLOCKS.get((period_id, category_id))
        if rows is None:
            return pd.DataFrame(columns=["block_start_kwh", "block_end_kwh", "rate"])
        return pd.DataFrame(rows, columns=["block_start_kwh", "block_end_kwh", "rate"])
    
    try:
        query = """
            SELECT block_start_kwh, block_end_kwh, rate
            FROM tariff_components
            WHERE tariff_period_id = %s
            AND category_id = %s
            AND component_type = 'energy'
            ORDER BY block_start_kwh
        """
        df = pd.read_sql(query, conn, params=(period_id, category_id))
        return df
    finally:
        conn.close()

@st.cache_data(ttl=3600, show_spinner=False)
def get_service_charge(period_id, category_id):
    conn = get_connection()
    if conn is None:
        return FALLBACK_SERVICE_CHARGE.get((period_id, category_id), 10.0)
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT charge
            FROM service_charges
            WHERE tariff_period_id = %s
            AND category_id = %s
            LIMIT 1
        """, (period_id, category_id))
        result = cur.fetchone()
        return result[0] if result else 0
    finally:
        conn.close()

def calculate_energy_bill(consumption, blocks):
    """Calculate bill from consumption using tiered pricing blocks."""
    total = 0
    remaining = consumption

    for _, row in blocks.iterrows():
        start = row["block_start_kwh"]
        end = row["block_end_kwh"]
        rate = row["rate"]

        # Calculate units in this block
        if end is None:
            energy = remaining
        else:
            block_size = end - start
            energy = min(remaining, block_size)

        if energy <= 0:
            break

        total += energy * rate
        remaining -= energy

    return total

def estimate_consumption_from_bill(bill, blocks, service_charge):
    """Estimate consumption from bill amount."""
    bill = float(bill) - float(service_charge)

    if bill <= 0:
        return 0

    # Remove levy/tax from bill
    bill = bill / (1 + LEVY_RATE)

    consumption = 0
    remaining_bill = bill

    for _, row in blocks.iterrows():
        start = row["block_start_kwh"]
        end = row["block_end_kwh"]
        rate = float(row["rate"])

        if end is None:
            consumption += remaining_bill / rate
            break

        block_units = end - start
        block_cost = block_units * rate

        if remaining_bill > block_cost:
            consumption += block_units
            remaining_bill -= block_cost
        else:
            consumption += remaining_bill / rate
            break

    return consumption

# ============================================================================
# STREAMLIT UI
# ============================================================================

st.set_page_config(page_title="Electricity Tariff Reckoner", layout="wide")
st.title("⚡ Electricity Tariff Reckoner")

# Category mapping
category_map = {
    "Residential": 1,
    "Non Residential": 2,
    "SLT LV": 3,
    "SLT MV2": 4,
    "SLT MV HV": 5
}

# Initialize session state for faster re-runs
if "mode" not in st.session_state:
    st.session_state.mode = "Consumption → Bill"
if "category" not in st.session_state:
    st.session_state.category = "Residential"

# Sidebar for main controls
with st.sidebar:
    st.header("Settings")
    mode = st.radio(
        "Mode",
        ["Consumption → Bill", "Bill → Consumption", "Historic Tariff Explorer"],
        key="mode"
    )
    
    category_name = st.selectbox(
        "Customer Category",
        list(category_map.keys()),
        key="category"
    )
    category = category_map[category_name]

# Main content
col1, col2 = st.columns(2)

with col1:
    st.subheader("📋 Tariff Selection")
    
    year_options = get_available_years()
    selected_year = st.selectbox("Year", year_options)
    
    periods = get_periods_for_year(selected_year)
    period_map = {period_name: period_id for period_id, period_name in periods}
    selected_period_name = st.selectbox("Tariff Quarter", list(period_map.keys()))
    selected_period_id = period_map[selected_period_name]

with col2:
    st.subheader("📊 Input Data")
    
    if mode == "Historic Tariff Explorer":
        st.info("Select year and quarter above to view tariff details.")
        consumption = None
        bill_amount = None
    elif mode == "Consumption → Bill":
        consumption = st.number_input(
            "Consumption (kWh)",
            min_value=0.0,
            step=1.0,
            value=100.0
        )
        bill_amount = None
    else:  # Bill → Consumption
        bill_amount = st.number_input(
            "Bill Amount (GHS)",
            min_value=0.0,
            step=0.1,
            value=50.0
        )
        consumption = None

# Run calculations on button click
if st.button("🔍 Calculate", use_container_width=True):
    blocks = get_tariff_blocks(selected_period_id, category)
    service_charge = get_service_charge(selected_period_id, category)
    
    st.markdown("---")
    
    # Display tariff block breakdown
    st.subheader("📈 Tariff Block Breakdown")
    
    block_data = []
    for _, row in blocks.iterrows():
        start = int(row["block_start_kwh"])
        end = int(row["block_end_kwh"]) if pd.notna(row["block_end_kwh"]) else None
        rate = float(row["rate"])
        
        if end is None:
            range_str = f"{start}+ kWh"
        else:
            range_str = f"{start} - {end} kWh"
        
        block_data.append({
            "Range": range_str,
            "Rate (GHS/kWh)": f"{rate:.4f}"
        })
    
    st.dataframe(pd.DataFrame(block_data), use_container_width=True)
    st.metric("Service Charge", f"GHS {float(service_charge):.2f}")
    
    st.markdown("---")
    st.subheader("💰 Results")
    
    if mode == "Historic Tariff Explorer":
        st.success(f"**Year:** {selected_year} | **Period:** {selected_period_name}")
    
    elif mode == "Consumption → Bill":
        energy_bill = calculate_energy_bill(consumption, blocks)
        levy_tax = energy_bill * LEVY_RATE
        total_bill = energy_bill + levy_tax + float(service_charge)
        
        res_col1, res_col2, res_col3, res_col4 = st.columns(4)
        
        with res_col1:
            st.metric("Consumption", f"{consumption:.2f} kWh")
        with res_col2:
            st.metric("Energy Charge", f"GHS {energy_bill:.2f}")
        with res_col3:
            st.metric("Levies/Taxes (5%)", f"GHS {levy_tax:.2f}")
        with res_col4:
            st.metric("📊 Total Bill", f"GHS {total_bill:.2f}", delta=None)
        
        # Breakdown table
        breakdown = pd.DataFrame({
            "Component": ["Energy Charge", "Levies/Taxes", "Service Charge", "Total"],
            "Amount (GHS)": [
                f"{energy_bill:.2f}",
                f"{levy_tax:.2f}",
                f"{float(service_charge):.2f}",
                f"{total_bill:.2f}"
            ]
        })
        st.dataframe(breakdown, use_container_width=True, hide_index=True)
    
    elif mode == "Bill → Consumption":
        if bill_amount < float(service_charge):
            st.error(f"⚠️ Bill amount must be at least GHS {float(service_charge):.2f} (service charge)")
        else:
            consumption_est = estimate_consumption_from_bill(bill_amount, blocks, service_charge)
            
            res_col1, res_col2, res_col3 = st.columns(3)
            
            with res_col1:
                st.metric("Bill Amount", f"GHS {bill_amount:.2f}")
            with res_col2:
                st.metric("Service Charge", f"GHS {float(service_charge):.2f}")
            with res_col3:
                st.metric("📊 Est. Consumption", f"{consumption_est:.2f} kWh")
            
            # Calculation breakdown
            st.info(f"**Estimated consumption:** {consumption_est:.2f} kWh based on the provided bill amount and current tariff rates.")

st.markdown("---")
st.caption("💡 Tip: Use the sidebar to change modes and customer categories quickly.")
