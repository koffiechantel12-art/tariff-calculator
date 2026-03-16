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

def get_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        st.warning("DB connection failed; using local fallback data.")
        return None

@st.cache_data(show_spinner=False)
def get_tariff_period(billing_date):
    conn = get_connection()
    if conn is None:
        # not used in UI currently, but keep signature
        return (1, "Q1 2025")
    cur = conn.cursor()

    cur.execute("""
        SELECT id, period_name
        FROM tariff_periods
        WHERE start_date <= %s
        ORDER BY start_date DESC
        LIMIT 1
    """, (billing_date,))

    result = cur.fetchone()
    conn.close()
    return result

@st.cache_data(show_spinner=False)
def get_tariff_blocks(period_id, category_id):
    conn = get_connection()
    if conn is None:
        rows = FALLBACK_TARIFF_BLOCKS.get((period_id, category_id))
        if rows is None:
            return pd.DataFrame(columns=["block_start_kwh", "block_end_kwh", "rate"])
        return pd.DataFrame(rows, columns=["block_start_kwh", "block_end_kwh", "rate"])

    query = """
        SELECT block_start_kwh, block_end_kwh, rate
        FROM tariff_components
        WHERE tariff_period_id = %s
        AND category_id = %s
        AND component_type = 'energy'
        ORDER BY block_start_kwh
    """
    df = pd.read_sql(query, conn, params=(period_id, category_id))
    conn.close()
    return df

@st.cache_data(show_spinner=False)
def get_service_charge(period_id, category_id):
    conn = get_connection()
    if conn is None:
        return FALLBACK_SERVICE_CHARGE.get((period_id, category_id), 10.0)

    cur = conn.cursor()
    cur.execute("""
        SELECT charge
        FROM service_charges
        WHERE tariff_period_id = %s
        AND category_id = %s
        LIMIT 1
    """, (period_id, category_id))

    result = cur.fetchone()
    conn.close()

    if result:
        return result[0]
    return 0

def calculate_energy_bill(consumption, blocks):
    total = 0
    remaining = consumption

    for _, row in blocks.iterrows():

        start = row["block_start_kwh"]
        end = row["block_end_kwh"]
        rate = row["rate"]

        if end is None:
            energy = remaining
        else:
            energy = min(remaining, end - start + 1)

        if energy <= 0:
            break

        total += energy * rate
        remaining -= energy

    return total

def estimate_consumption_from_bill(bill, blocks, service_charge):

    bill = float(bill) - float(service_charge)

    if bill <= 0:
        return 0

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

        block_units = end - start + 1
        block_cost = block_units * rate

        if remaining_bill > block_cost:
            consumption += block_units
            remaining_bill -= block_cost
        else:
            consumption += remaining_bill / rate
            break

    return consumption

@st.cache_data(show_spinner=False)
def get_available_years():
    conn = get_connection()
    if conn is None:
        return FALLBACK_YEAR_LIST

    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT y.year
        FROM tariff_periods tp
        JOIN years y ON tp.year_id = y.id
        ORDER BY y.year DESC
    """)

    years = [row[0] for row in cur.fetchall()]
    conn.close()
    if not years:
        return FALLBACK_YEAR_LIST
    return years


@st.cache_data(show_spinner=False)
def get_periods_for_year(selected_year):
    conn = get_connection()
    if conn is None:
        return FALLBACK_PERIODS.get(selected_year, [])

    cur = conn.cursor()
    cur.execute("""
        SELECT tp.id, tp.period_name
        FROM tariff_periods tp
        JOIN years y ON tp.year_id = y.id
        WHERE y.year = %s
        ORDER BY tp.start_date
    """, (selected_year,))

    periods = cur.fetchall()
    conn.close()
    if not periods:
        return FALLBACK_PERIODS.get(selected_year, [])
    return periods



    cur.execute("""
        SELECT tp.id, tp.period_name
        FROM tariff_periods tp
        JOIN years y ON tp.year_id = y.id
        WHERE y.year = %s
        ORDER BY tp.start_date
    """, (selected_year,))

    periods = cur.fetchall()
    conn.close()
    return periods


def get_tariff_blocks_by_period(period_id, category_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT block_start_kwh, block_end_kwh, rate
        FROM tariff_components
        WHERE tariff_period_id = %s
          AND category_id = %s
          AND component_type = 'energy'
        ORDER BY block_start_kwh
    """, (period_id, category_id))

    rows = cur.fetchall()
    conn.close()
    return rows

def get_service_charge_by_period(period_id, category_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT charge
        FROM service_charges
        WHERE tariff_period_id = %s
          AND category_id = %s
        LIMIT 1
    """, (period_id, category_id))

    result = cur.fetchone()
    conn.close()

    if result:
        return result[0]
    return 0

LEVY_RATE = 0.05  # 5% levies and taxes on energy charge

st.title("Electricity Tariff Reckoner")

mode = st.radio(
    "Mode",
    ["Consumption → Bill", "Bill → Consumption", "Historic Tariff Explorer"]
)

category_map = {
    "Residential": 1,
    "Non Residential": 2,
    "SLT LV": 3,
    "SLT MV2": 4,
    "SLT MV HV": 5
}

category_name = st.selectbox(
    "Customer Category",
    list(category_map.keys())
)

category = category_map[category_name]

if mode in ["Consumption → Bill", "Bill → Consumption"]:
    year_options = get_available_years()
    selected_year = st.selectbox("Year", year_options)
    periods = get_periods_for_year(selected_year)
    period_map = {period_name: period_id for period_id, period_name in periods}
    selected_period_name = st.selectbox("Tariff Quarter", list(period_map.keys()))
    selected_period_id = period_map[selected_period_name]

    preference = st.selectbox("Preference", ["Consumption (kWh)", "Bill (GHS)"])

    if mode == "Consumption → Bill":
        consumption = st.number_input("Consumption (kWh)", min_value=0.0, step=1.0)
        bill_amount = None
    else:
        bill_amount = st.number_input("Bill Amount (GHS)", min_value=0.0, step=0.1)
        consumption = None

elif mode == "Historic Tariff Explorer":
    available_years = get_available_years()
    selected_year = st.selectbox("Select Year", available_years)
    periods = get_periods_for_year(selected_year)
    period_map = {period_name: period_id for period_id, period_name in periods}
    selected_period_name = st.selectbox("Select Tariff Period", list(period_map.keys()))
    selected_period_id = period_map[selected_period_name]
    consumption = None
    bill_amount = None

if st.button("Run"):
    if mode == "Historic Tariff Explorer":
        blocks = get_tariff_blocks_by_period(selected_period_id, category)
        service_charge = get_service_charge_by_period(selected_period_id, category)

        st.subheader("Historic Tariff Details")
        st.write("Year:", selected_year)
        st.write("Tariff Period:", selected_period_name)
        st.write("Customer Category:", category_name)

        st.subheader("Tariff Block Breakdown")
        for start, end, rate in blocks:
            rate = float(rate)
            if end is None:
                st.write(f"{start}+ kWh : {rate:.4f} GHS/kWh")
            else:
                st.write(f"{start} to {end} kWh : {rate:.4f} GHS/kWh")

        st.write("Service Charge:", round(float(service_charge), 2), "GHS")

    else:
        period_id = selected_period_id
        period_name = selected_period_name

        blocks = get_tariff_blocks(period_id, category)
        service_charge = get_service_charge(period_id, category)

        st.subheader("Tariff Block Breakdown")
        for _, row in blocks.iterrows():
            start = row["block_start_kwh"]
            end = row["block_end_kwh"]
            rate = float(row["rate"])
            if pd.isna(end):
                st.write(f"{start}+ kWh : {rate:.4f} GHS/kWh")
            else:
                st.write(f"{start} to {end} kWh : {rate:.4f} GHS/kWh")

        if mode == "Consumption → Bill":
            energy_bill = calculate_energy_bill(consumption, blocks)
            levy_tax = energy_bill * LEVY_RATE
            total_bill = energy_bill + levy_tax + float(service_charge)

            st.write("Tariff Period:", period_name)
            st.write("Energy Charge (GHS):", round(float(energy_bill), 2))
            st.write("Levies/Taxes (GHS):", round(float(levy_tax), 2))
            st.write("Service Charge (GHS):", round(float(service_charge), 2))
            st.write("Total Amount (GHS):", round(float(total_bill), 2))

        elif mode == "Bill → Consumption":
            if bill_amount < service_charge:
                st.error("Bill amount must cover service charge before estimating consumption.")
            else:
                consumption_est = estimate_consumption_from_bill(bill_amount, blocks, service_charge)
                st.write("Tariff Period:", period_name)
                st.write("Estimated Consumption (kWh):", round(float(consumption_est), 2))