import streamlit as st
import sqlite3
import bcrypt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from datetime import datetime, timedelta
import numpy as np
import hashlib
import json
import io
import uuid
from io import BytesIO
import xlsxwriter

# Configure Streamlit page
st.set_page_config(
    page_title="Fixed Assets Management System",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Database initialization
def init_database():
    conn = sqlite3.connect("fa3_management.db")
    c = conn.cursor()

    # Create assets table
    c.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_name TEXT NOT NULL,
            description TEXT,
            brand TEXT,
            serial_number TEXT UNIQUE,
            acquisition_date DATE,
            status TEXT CHECK(status IN ('active', 'inactive', 'disposed', 'transferred')),
            location_id INTEGER,
            department TEXT,
            current_value REAL,
            original_value REAL,
            depreciation_rate REAL,
            depreciation_method TEXT CHECK(depreciation_method IN ('Straight-Line', 'Reducing Balance')),
            asset_type TEXT CHECK(asset_type IN (
                'Land & Building', 'Motor Vehicle', 'Computer & Accessories',
                'Office Equipment', 'Furniture & Fittings', 'Intangible Assets',
                'Legacy Assets', 'Other Assets'
            )),
            FOREIGN KEY (location_id) REFERENCES locations (location_id)
        )
    """)

    # Create users table
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT CHECK(role IN ('admin', 'management', 'staff')),
            department TEXT
        )
    """)

    # Create locations table
    c.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_name TEXT UNIQUE NOT NULL
        )
    """)

    # Create transactions table
    c.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER,
            transaction_type TEXT CHECK(transaction_type IN ('acquisition', 'disposal', 'transfer')),
            buyer_seller TEXT,
            transaction_date DATE,
            department TEXT,
            FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
        )
    """)

    # Create maintenance table
    c.execute("""
        CREATE TABLE IF NOT EXISTS maintenance (
            maintenance_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER,
            start_date DATE,
            end_date DATE,
            status TEXT CHECK(status IN ('in progress', 'completed')),
            FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
        )
    """)

    # Create revaluation table
    c.execute("""
        CREATE TABLE IF NOT EXISTS revaluation (
            revaluation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER,
            revaluation_date DATE,
            new_value REAL,
            FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
        )
    """)

    # Create disposal table
    c.execute("""
        CREATE TABLE IF NOT EXISTS disposal (
            disposal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER,
            disposal_date DATE,
            selling_price REAL,
            FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
        )
    """)

    # Create audit_logs table
    c.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT,
            user_id INTEGER,
            asset_id INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id),
            FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
        )
    """)

    # Create default admin user if not exists
    default_password = "admin123"
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(default_password.encode("utf-8"), salt)

    c.execute("""
        INSERT OR IGNORE INTO users (username, password_hash, role, department)
        VALUES (?, ?, ?, ?)
    """, ("admin", hashed_password.decode("utf-8"), "admin", "IT"))

    conn.commit()
    conn.close()


# Database connection helper
def get_database_connection():
    return sqlite3.connect("fa3_management.db")


# Initialize session state
def init_session_state():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "username" not in st.session_state:
        st.session_state.username = None
    if "user_role" not in st.session_state:
        st.session_state.user_role = None


# Authentication helper functions
def verify_password(stored_hash, provided_password):
    return bcrypt.checkpw(
        provided_password.encode("utf-8"), stored_hash.encode("utf-8")
    )


def hash_password(password):
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


# Initialize the application
def main():
    init_session_state()
    init_database()

    if not st.session_state.logged_in:
        show_login_page()
    else:
        show_main_application()


# Login page
def show_login_page():
    st.title("Fixed Assets Management System")
    st.subheader("Login")

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login"):
            conn = get_database_connection()
            c = conn.cursor()
            c.execute(
                "SELECT password_hash, role FROM users WHERE username = ?", (username,)
            )
            result = c.fetchone()

            if result and verify_password(result[0], password):
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.user_role = result[1]
                st.success("Login successful!")
                st.rerun()
            else:
                st.error("Invalid username or password")

            conn.close()


# Add these utility functions after the existing helper functions


def calculate_depreciation(
    original_value, depreciation_rate, acquisition_date, method="Straight-Line"
):
    """Calculate current value after depreciation"""
    years = (
        datetime.now() - datetime.strptime(acquisition_date, "%Y-%m-%d")
    ).days / 365.25

    if method == "Straight-Line":
        depreciation = original_value * (depreciation_rate / 100) * years
        current_value = original_value - depreciation
    else:  # Reducing Balance
        current_value = original_value * ((1 - depreciation_rate / 100) ** years)

    return max(0, current_value)


def get_asset_type_depreciation_rate(asset_type):
    """Return depreciation rate and method based on asset type"""
    depreciation_rules = {
        "Motor Vehicle": (20, "Reducing Balance"),
        "Land & Building": (2, "Straight-Line"),
        "Computer & Accessories": (25, "Reducing Balance"),
        "Office Equipment": (10, "Reducing Balance"),
        "Furniture & Fittings": (10, "Reducing Balance"),
        "Intangible Assets": (10, "Straight-Line"),
        "Legacy Assets": (5, "Straight-Line"),
        "Other Assets": (10, "Reducing Balance"),
    }
    return depreciation_rules.get(asset_type, (10, "Reducing Balance"))




# Dashboard helper functions
def get_total_assets_value(conn):
    c = conn.cursor()
    c.execute("SELECT SUM(current_value) FROM assets WHERE status = 'active'")
    return c.fetchone()[0] or 0


def get_asset_value_change(conn):
    c = conn.cursor()
    current_date = datetime.now()
    past_date = current_date - timedelta(days=30)

    c.execute(
        """
        SELECT 
            (SELECT SUM(current_value) FROM assets WHERE status = 'active') /
            NULLIF((SELECT SUM(original_value) FROM assets 
                   WHERE acquisition_date >= ? AND status = 'active'), 0) * 100 - 100
    """,
        (past_date.strftime("%Y-%m-%d"),),
    )

    return c.fetchone()[0] or 0


def get_active_assets_count(conn):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM assets WHERE status = 'active'")
    return c.fetchone()[0]


def get_assets_count_change(conn):
    c = conn.cursor()
    c.execute(
        """
        SELECT COUNT(*) FROM assets 
        WHERE status = 'active' 
        AND acquisition_date >= date('now', '-30 days')
    """
    )
    return c.fetchone()[0]


def get_maintenance_count(conn):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM maintenance WHERE status = 'in progress'")
    return c.fetchone()[0]


def create_asset_distribution_chart(conn):
    df = pd.read_sql_query(
        """
        SELECT asset_type, COUNT(*) as count, SUM(current_value) as total_value
        FROM assets
        WHERE status = 'active'
        GROUP BY asset_type
    """,
        conn,
    )

    fig = px.pie(
        df,
        values="total_value",
        names="asset_type",
        title="Asset Distribution by Value",
    )
    return fig


def create_department_value_chart(conn):
    df = pd.read_sql_query(
        """
        SELECT department, SUM(current_value) as total_value
        FROM assets
        WHERE status = 'active'
        GROUP BY department
    """,
        conn,
    )

    fig = px.bar(df, x="department", y="total_value", title="Asset Value by Department")
    return fig


def show_recent_transactions(conn):
    df = pd.read_sql_query(
        """
        SELECT t.transaction_date, t.transaction_type, a.asset_name, t.department
        FROM transactions t
        JOIN assets a ON t.asset_id = a.asset_id
        ORDER BY t.transaction_date DESC
        LIMIT 5
    """,
        conn,
    )

    st.dataframe(df)


# Update the show_asset_management function
def show_asset_management():
    st.title("💼 Asset Management")

    tabs = st.tabs(["Asset List", "Add New Asset", "Bulk Import"])

    with tabs[0]:
        show_asset_list()

    with tabs[1]:
        show_add_asset_form()

    with tabs[2]:
        show_bulk_import()


def show_asset_list():
    conn = get_database_connection()

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        asset_type_filter = st.selectbox(
            "Filter by Asset Type", ["All"] + get_asset_types(conn)
        )
    with col2:
        status_filter = st.selectbox(
            "Filter by Status", ["All", "active", "inactive", "disposed", "transferred"]
        )
    with col3:
        search_term = st.text_input("Search Assets", "")

    # Construct query based on filters
    query = """
        SELECT 
            asset_id, asset_name, asset_type, status, 
            current_value, department, acquisition_date
        FROM assets
        WHERE 1=1
    """
    params = []

    if asset_type_filter != "All":
        query += " AND asset_type = ?"
        params.append(asset_type_filter)

    if status_filter != "All":
        query += " AND status = ?"
        params.append(status_filter)

    if search_term:
        query += """ AND (
            asset_name LIKE ? OR 
            description LIKE ? OR 
            serial_number LIKE ?
        )"""
        search_pattern = f"%{search_term}%"
        params.extend([search_pattern, search_pattern, search_pattern])

    # Fetch and display assets
    df = pd.read_sql_query(query, conn, params=params)

    if not df.empty:
        st.dataframe(
            df,
            column_config={
                "current_value": st.column_config.NumberColumn(
                    "Current Value", format="$%.2f"
                ),
                "acquisition_date": st.column_config.DateColumn("Acquisition Date"),
            },
            hide_index=True,
        )
    else:
        st.info("No assets found matching the criteria.")

    conn.close()


def show_add_asset_form():
    st.subheader("Add New Asset")

    col1, col2 = st.columns(2)

    with col1:
        asset_name = st.text_input("Asset Name*")
        asset_type = st.selectbox("Asset Type*", get_asset_types())
        brand = st.text_input("Brand")
        serial_number = st.text_input("Serial Number")

    with col2:
        department = st.text_input("Department*")
        acquisition_date = st.date_input("Acquisition Date*")
        original_value = st.number_input("Original Value*", min_value=0.0)
        location = st.selectbox("Location*", get_locations())

    description = st.text_area("Description")

    if st.button("Add Asset"):
        if not all(
            [asset_name, asset_type, department, acquisition_date, original_value]
        ):
            st.error("Please fill in all required fields marked with *")
            return

        try:
            conn = get_database_connection()
            c = conn.cursor()

            # Get depreciation rate and method based on asset type
            dep_rate, dep_method = get_asset_type_depreciation_rate(asset_type)

            # Calculate initial current value
            current_value = calculate_depreciation(
                original_value,
                dep_rate,
                acquisition_date.strftime("%Y-%m-%d"),
                dep_method,
            )

            # Insert new asset
            c.execute(
                """
                INSERT INTO assets (
                    asset_name, description, brand, serial_number,
                    acquisition_date, status, location_id, department,
                    current_value, original_value, depreciation_rate,
                    depreciation_method, asset_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    asset_name,
                    description,
                    brand,
                    serial_number,
                    acquisition_date.strftime("%Y-%m-%d"),
                    "active",
                    location,
                    department,
                    current_value,
                    original_value,
                    dep_rate,
                    dep_method,
                    asset_type,
                ),
            )

            # Log the action
            c.execute(
                """
                INSERT INTO audit_logs (action, user_id, asset_id)
                VALUES (?, ?, ?)
            """,
                ("Asset created", get_user_id(st.session_state.username), c.lastrowid),
            )

            conn.commit()
            st.success("Asset added successfully!")

        except sqlite3.Error as e:
            st.error(f"An error occurred: {e}")
        finally:
            conn.close()


def show_bulk_import():
    st.subheader("Bulk Import Assets")

    # Show template download button
    if st.button("Download Template"):
        csv_template = create_csv_template()
        st.download_button(
            label="Download CSV Template",
            data=csv_template,
            file_name="asset_import_template.csv",
            mime="text/csv",
        )

    # File upload
    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.write("Preview of uploaded data:")
            st.write(df.head())

            if st.button("Import Assets"):
                import_assets_from_df(df)
                st.success("Assets imported successfully!")
        except Exception as e:
            st.error(f"Error processing file: {e}")


# Utility functions for asset management
def get_asset_types(conn=None):
    return [
        "Land & Building",
        "Motor Vehicle",
        "Computer & Accessories",
        "Office Equipment",
        "Furniture & Fittings",
        "Intangible Assets",
        "Legacy Assets",
        "Other Assets",
    ]


def get_locations():
    conn = get_database_connection()
    c = conn.cursor()
    c.execute("SELECT location_id, location_name FROM locations")
    locations = c.fetchall()
    conn.close()
    return [loc[0] for loc in locations] if locations else []


def get_user_id(username):
    conn = get_database_connection()
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE username = ?", (username,))
    user_id = c.fetchone()[0]
    conn.close()
    return user_id


def create_csv_template():
    template_data = {
        "asset_name": ["Example Asset 1"],
        "asset_type": ["Computer & Accessories"],
        "description": ["Description here"],
        "brand": ["Brand name"],
        "serial_number": ["SN123456"],
        "acquisition_date": ["2023-01-01"],
        "department": ["IT"],
        "original_value": [1000.00],
    }

    df = pd.DataFrame(template_data)
    return df.to_csv(index=False)


def import_assets_from_df(df):
    conn = get_database_connection()
    c = conn.cursor()

    try:
        for _, row in df.iterrows():
            dep_rate, dep_method = get_asset_type_depreciation_rate(row["asset_type"])
            current_value = calculate_depreciation(
                row["original_value"], dep_rate, row["acquisition_date"], dep_method
            )

            c.execute(
                """
                INSERT INTO assets (
                    asset_name, asset_type, description, brand,
                    serial_number, acquisition_date, department,
                    original_value, current_value, status,
                    depreciation_rate, depreciation_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    row["asset_name"],
                    row["asset_type"],
                    row["description"],
                    row["brand"],
                    row["serial_number"],
                    row["acquisition_date"],
                    row["department"],
                    row["original_value"],
                    current_value,
                    "active",
                    dep_rate,
                    dep_method,
                ),
            )

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# Add these utility functions


def export_to_excel(df, filename):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Sheet1", index=False)
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]

        # Add formatting
        money_fmt = workbook.add_format({"num_format": "$#,##0.00"})
        date_fmt = workbook.add_format({"num_format": "yyyy-mm-dd"})

        # Auto-adjust columns width
        for i, col in enumerate(df.columns):
            column_len = max(df[col].astype(str).apply(len).max(), len(col)) + 2
            worksheet.set_column(i, i, column_len)

    return output.getvalue()


# Update the show_transactions function
def show_transactions():
    st.title("🔄 Transaction Management")

    tabs = st.tabs(["Transaction List", "New Transaction", "Bulk Transfer"])

    with tabs[0]:
        show_transaction_list()

    with tabs[1]:
        show_new_transaction_form()

    with tabs[2]:
        show_bulk_transfer_form()


def show_transaction_list():
    st.subheader("Transaction History")

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        date_range = st.date_input(
            "Date Range", value=(datetime.now() - timedelta(days=30), datetime.now())
        )

    with col2:
        transaction_type = st.selectbox(
            "Transaction Type", ["All", "acquisition", "disposal", "transfer"]
        )

    with col3:
        department = st.selectbox("Department", ["All"] + get_departments())

    # Fetch transactions
    conn = get_database_connection()
    query = """
        SELECT 
            t.transaction_id,
            t.transaction_date,
            t.transaction_type,
            a.asset_name,
            t.department,
            t.buyer_seller,
            CASE 
                WHEN t.transaction_type = 'disposal' THEN d.selling_price
                ELSE a.current_value
            END as value
        FROM transactions t
        JOIN assets a ON t.asset_id = a.asset_id
        LEFT JOIN disposal d ON t.asset_id = d.asset_id
        WHERE t.transaction_date BETWEEN ? AND ?
    """
    params = [date_range[0], date_range[1]]

    if transaction_type != "All":
        query += " AND t.transaction_type = ?"
        params.append(transaction_type)

    if department != "All":
        query += " AND t.department = ?"
        params.append(department)

    df = pd.read_sql_query(query, conn, params=params)

    if not df.empty:
        st.dataframe(df, hide_index=True)

        # Export button
        if st.button("Export to Excel"):
            excel_data = export_to_excel(df, "transactions.xlsx")
            st.download_button(
                label="Download Excel file",
                data=excel_data,
                file_name="transactions.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    else:
        st.info("No transactions found for the selected criteria.")

    conn.close()


def show_new_transaction_form():
    st.subheader("New Transaction")

    transaction_type = st.selectbox(
        "Transaction Type", ["acquisition", "disposal", "transfer"]
    )

    col1, col2 = st.columns(2)

    with col1:
        if transaction_type == "acquisition":
            asset_name = st.text_input("Asset Name*")
            asset_type = st.selectbox("Asset Type*", get_asset_types())
            original_value = st.number_input("Purchase Value*", min_value=0.0)
        else:
            asset_id = st.selectbox(
                "Select Asset*",
                options=get_available_assets(),
                format_func=lambda x: get_asset_name(x),
            )

        department = st.text_input("Department*")

    with col2:
        transaction_date = st.date_input("Transaction Date*")
        buyer_seller = st.text_input(
            "Seller" if transaction_type == "acquisition" else "Buyer/Recipient"
        )

        if transaction_type == "disposal":
            selling_price = st.number_input("Selling Price", min_value=0.0)

    if st.button("Submit Transaction"):
        try:
            conn = get_database_connection()
            c = conn.cursor()

            if transaction_type == "acquisition":
                # Create new asset
                dep_rate, dep_method = get_asset_type_depreciation_rate(asset_type)
                current_value = calculate_depreciation(
                    original_value,
                    dep_rate,
                    transaction_date.strftime("%Y-%m-%d"),
                    dep_method,
                )

                c.execute(
                    """
                    INSERT INTO assets (
                        asset_name, asset_type, acquisition_date,
                        department, current_value, original_value,
                        depreciation_rate, depreciation_method, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        asset_name,
                        asset_type,
                        transaction_date.strftime("%Y-%m-%d"),
                        department,
                        current_value,
                        original_value,
                        dep_rate,
                        dep_method,
                        "active",
                    ),
                )

                asset_id = c.lastrowid

            elif transaction_type == "disposal":
                # Record disposal
                c.execute(
                    """
                    INSERT INTO disposal (
                        asset_id, disposal_date, selling_price
                    ) VALUES (?, ?, ?)
                """,
                    (asset_id, transaction_date.strftime("%Y-%m-%d"), selling_price),
                )

                # Update asset status
                c.execute(
                    """
                    UPDATE assets 
                    SET status = 'disposed'
                    WHERE asset_id = ?
                """,
                    (asset_id,),
                )

            else:  # transfer
                # Update asset department
                c.execute(
                    """
                    UPDATE assets 
                    SET department = ?,
                        status = 'transferred'
                    WHERE asset_id = ?
                """,
                    (department, asset_id),
                )

            # Record transaction
            c.execute(
                """
                INSERT INTO transactions (
                    asset_id, transaction_type, buyer_seller,
                    transaction_date, department
                ) VALUES (?, ?, ?, ?, ?)
            """,
                (
                    asset_id,
                    transaction_type,
                    buyer_seller,
                    transaction_date.strftime("%Y-%m-%d"),
                    department,
                ),
            )

            # Log the action
            c.execute(
                """
                INSERT INTO audit_logs (
                    action, user_id, asset_id
                ) VALUES (?, ?, ?)
            """,
                (
                    f"{transaction_type} transaction",
                    get_user_id(st.session_state.username),
                    asset_id,
                ),
            )

            conn.commit()
            st.success("Transaction recorded successfully!")

        except sqlite3.Error as e:
            st.error(f"An error occurred: {e}")
        finally:
            conn.close()


def show_bulk_transfer_form():
    st.subheader("Bulk Transfer")

    # Asset selection
    assets = st.multiselect(
        "Select Assets to Transfer",
        options=get_available_assets(),
        format_func=lambda x: get_asset_name(x),
    )

    if assets:
        col1, col2 = st.columns(2)

        with col1:
            new_department = st.text_input("New Department*")
            transfer_date = st.date_input("Transfer Date*")

        with col2:
            recipient = st.text_input("Recipient")
            notes = st.text_area("Transfer Notes")

        if st.button("Process Bulk Transfer"):
            try:
                conn = get_database_connection()
                c = conn.cursor()

                for asset_id in assets:
                    # Update asset
                    c.execute(
                        """
                        UPDATE assets 
                        SET department = ?,
                            status = 'transferred'
                        WHERE asset_id = ?
                    """,
                        (new_department, asset_id),
                    )

                    # Record transaction
                    c.execute(
                        """
                        INSERT INTO transactions (
                            asset_id, transaction_type, buyer_seller,
                            transaction_date, department
                        ) VALUES (?, ?, ?, ?, ?)
                    """,
                        (
                            asset_id,
                            "transfer",
                            recipient,
                            transfer_date.strftime("%Y-%m-%d"),
                            new_department,
                        ),
                    )

                    # Log the action
                    c.execute(
                        """
                        INSERT INTO audit_logs (
                            action, user_id, asset_id
                        ) VALUES (?, ?, ?)
                    """,
                        (
                            "bulk transfer",
                            get_user_id(st.session_state.username),
                            asset_id,
                        ),
                    )

                conn.commit()
                st.success(f"Successfully transferred {len(assets)} assets!")

            except sqlite3.Error as e:
                st.error(f"An error occurred: {e}")
            finally:
                conn.close()


# Update the show_maintenance function
def show_maintenance():
    st.title("🔧 Maintenance Management")

    tabs = st.tabs(
        ["Active Maintenance", "Maintenance History", "Schedule Maintenance"]
    )

    with tabs[0]:
        show_active_maintenance()

    with tabs[1]:
        show_maintenance_history()

    with tabs[2]:
        show_maintenance_form()


def show_active_maintenance():
    st.subheader("Active Maintenance Items")

    conn = get_database_connection()
    df = pd.read_sql_query(
        """
        SELECT 
            m.maintenance_id,
            a.asset_name,
            m.start_date,
            m.end_date,
            m.status,
            a.department
        FROM maintenance m
        JOIN assets a ON m.asset_id = a.asset_id
        WHERE m.status = 'in progress'
        ORDER BY m.start_date DESC
    """,
        conn,
    )

    if not df.empty:
        for _, row in df.iterrows():
            with st.expander(f"{row['asset_name']} - {row['department']}"):
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.write(f"Start Date: {row['start_date']}")
                with col2:
                    st.write(f"Expected End: {row['end_date']}")
                with col3:
                    if st.button(
                        "Mark Complete", key=f"complete_{row['maintenance_id']}"
                    ):
                        complete_maintenance(row["maintenance_id"])
                        st.rerun()
    else:
        st.info("No active maintenance items.")

    conn.close()


def show_maintenance_history():
    st.subheader("Maintenance History")

    # Filters
    col1, col2 = st.columns(2)

    with col1:
        date_range = st.date_input(
            "Date Range", value=(datetime.now() - timedelta(days=90), datetime.now())
        )

    with col2:
        department = st.selectbox("Department", ["All"] + get_departments())

    # Fetch maintenance history
    conn = get_database_connection()
    query = """
        SELECT 
            m.maintenance_id,
            a.asset_name,
            m.start_date,
            m.end_date,
            m.status,
            a.department
        FROM maintenance m
        JOIN assets a ON m.asset_id = a.asset_id
        WHERE m.start_date BETWEEN ? AND ?
    """
    params = [date_range[0], date_range[1]]

    if department != "All":
        query += " AND a.department = ?"
        params.append(department)

    df = pd.read_sql_query(query, conn, params=params)

    if not df.empty:
        st.dataframe(df, hide_index=True)

        if st.button("Export Maintenance History"):
            excel_data = export_to_excel(df, "maintenance_history.xlsx")
            st.download_button(
                label="Download Excel file",
                data=excel_data,
                file_name="maintenance_history.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    else:
        st.info("No maintenance history found for the selected criteria.")

    conn.close()


def show_maintenance_form():
    st.subheader("Schedule New Maintenance")

    col1, col2 = st.columns(2)

    with col1:
        asset_id = st.selectbox(
            "Select Asset*",
            options=get_available_assets(),
            format_func=lambda x: get_asset_name(x),
        )
        start_date = st.date_input("Start Date*")

    with col2:
        end_date = st.date_input("Expected End Date*")
        notes = st.text_area("Maintenance Notes")

    if st.button("Schedule Maintenance"):
        if start_date > end_date:
            st.error("End date must be after start date.")
            return

        try:
            conn = get_database_connection()
            c = conn.cursor()

            # Create maintenance record
            c.execute(
                """
                INSERT INTO maintenance (
                    asset_id, start_date, end_date, status
                ) VALUES (?, ?, ?, ?)
            """,
                (
                    asset_id,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    "in progress",
                ),
            )

            # Update asset status
            c.execute(
                """
                UPDATE assets 
                SET status = 'inactive'
                WHERE asset_id = ?
            """,
                (asset_id,),
            )

            # Log the action
            c.execute(
                """
                INSERT INTO audit_logs (
                    action, user_id, asset_id
                ) VALUES (?, ?, ?)
            """,
                (
                    "maintenance scheduled",
                    get_user_id(st.session_state.username),
                    asset_id,
                ),
            )

            conn.commit()
            st.success("Maintenance scheduled successfully!")

        except sqlite3.Error as e:
            st.error(f"An error occurred: {e}")
        finally:
            conn.close()


def complete_maintenance(maintenance_id):
    conn = get_database_connection()
    c = conn.cursor()

    try:
        # Get asset_id from maintenance record
        c.execute(
            """
            SELECT asset_id FROM maintenance
            WHERE maintenance_id = ?
        """,
            (maintenance_id,),
        )
        asset_id = c.fetchone()[0]

        # Update maintenance status
        c.execute(
            """
            UPDATE maintenance 
            SET status = 'completed',
                end_date = date('now')
            WHERE maintenance_id = ?
        """,
            (maintenance_id,),
        )

        # Update asset status
        c.execute(
            """
            UPDATE assets 
            SET status = 'active'
            WHERE asset_id = ?
        """,
            (asset_id,),
        )

        # Log the action
        c.execute(
            """
            INSERT INTO audit_logs (
                action, user_id, asset_id
            ) VALUES (?, ?, ?)
        """,
            ("maintenance completed", get_user_id(st.session_state.username), asset_id),
        )

        conn.commit()

    except sqlite3.Error as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# Utility functions for database operations and data retrieval


def get_departments():
    """Get list of all departments"""
    conn = get_database_connection()
    c = conn.cursor()
    c.execute("SELECT DISTINCT department FROM assets WHERE department IS NOT NULL")
    departments = [row[0] for row in c.fetchall()]
    conn.close()
    return departments


def get_users():
    """Get list of all users"""
    conn = get_database_connection()
    c = conn.cursor()
    c.execute("SELECT username FROM users")
    users = [row[0] for row in c.fetchall()]
    conn.close()
    return users


def get_available_assets():
    """Get list of available assets"""
    conn = get_database_connection()
    c = conn.cursor()
    c.execute(
        """
        SELECT asset_id 
        FROM assets 
        WHERE status = 'active'
        ORDER BY asset_name
    """
    )
    assets = [row[0] for row in c.fetchall()]
    conn.close()
    return assets


def get_asset_name(asset_id):
    """Get asset name by ID"""
    conn = get_database_connection()
    c = conn.cursor()
    c.execute("SELECT asset_name FROM assets WHERE asset_id = ?", (asset_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else "Unknown Asset"


def get_available_fields():
    """Get list of available fields for custom reports"""
    return [
        "asset_name",
        "asset_type",
        "description",
        "brand",
        "serial_number",
        "acquisition_date",
        "status",
        "department",
        "current_value",
        "original_value",
        "depreciation_rate",
        "depreciation_method",
    ]


def get_available_filters():
    """Get list of available filters for custom reports"""
    return ["department", "asset_type", "status", "acquisition_date", "value_range"]


def get_filter_options(filter_name):
    """Get options for a specific filter"""
    conn = get_database_connection()
    c = conn.cursor()

    if filter_name == "department":
        c.execute("SELECT DISTINCT department FROM assets")
    elif filter_name == "asset_type":
        c.execute("SELECT DISTINCT asset_type FROM assets")
    elif filter_name == "status":
        c.execute("SELECT DISTINCT status FROM assets")

    options = ["All"] + [row[0] for row in c.fetchall()]
    conn.close()
    return options


def save_report_template(name, fields, filters):
    """Save a custom report template"""
    templates = get_saved_templates()
    templates[name] = {"fields": fields, "filters": filters}

    with open("report_templates.json", "w") as f:
        json.dump(templates, f)


def get_saved_templates():
    """Get saved report templates"""
    try:
        with open("report_templates.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def run_custom_report(template, filter_values):
    """Run a custom report based on template and filters"""
    conn = get_database_connection()

    # Construct query
    fields = ", ".join(template["fields"])
    query = f"SELECT {fields} FROM assets WHERE 1=1"
    params = []

    # Apply filters
    for filter_name, filter_value in filter_values.items():
        if filter_value != "All":
            if filter_name == "value_range":
                min_value, max_value = filter_value
                query += " AND current_value BETWEEN ? AND ?"
                params.extend([min_value, max_value])
            else:
                query += f" AND {filter_name} = ?"
                params.append(filter_value)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def reset_user_password(user_id):
    """Reset user password to default"""
    default_password = "changeme123"
    hashed_password = hash_password(default_password)

    conn = get_database_connection()
    c = conn.cursor()

    try:
        c.execute(
            """
            UPDATE users 
            SET password_hash = ?
            WHERE user_id = ?
        """,
            (hashed_password, user_id),
        )
        conn.commit()
    finally:
        conn.close()


def delete_user(user_id):
    """Delete a user"""
    conn = get_database_connection()
    c = conn.cursor()

    try:
        c.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()


def add_location(location_name):
    """Add a new location"""
    conn = get_database_connection()
    c = conn.cursor()

    try:
        c.execute(
            """
            INSERT INTO locations (location_name)
            VALUES (?)
        """,
            (location_name,),
        )
        conn.commit()
    finally:
        conn.close()


def delete_location(location_id):
    """Delete a location"""
    conn = get_database_connection()
    c = conn.cursor()

    try:
        c.execute("DELETE FROM locations WHERE location_id = ?", (location_id,))
        conn.commit()
    finally:
        conn.close()


# Add these imports if not already present
import plotly.figure_factory as ff
from datetime import datetime, timedelta
import json
import hashlib


# Reports Generation Module
def show_reports():
    st.title("📈 Reports & Analytics")

    tabs = st.tabs(
        [
            "Asset Summary",
            "Financial Reports",
            "Depreciation Schedule",
            "Audit Trail",
            "Custom Reports",
        ]
    )

    with tabs[0]:
        show_asset_summary_report()

    with tabs[1]:
        show_financial_reports()

    with tabs[2]:
        show_depreciation_schedule()

    with tabs[3]:
        show_audit_trail()

    with tabs[4]:
        show_custom_reports()


def show_asset_summary_report():
    st.subheader("Asset Summary Report")

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        department = st.selectbox("Department", ["All"] + get_departments())
    with col2:
        asset_type = st.selectbox("Asset Type", ["All"] + get_asset_types())

    conn = get_database_connection()

    # Asset count by status
    status_query = """
        SELECT status, COUNT(*) as count
        FROM assets
        WHERE 1=1
    """
    params = []

    if department != "All":
        status_query += " AND department = ?"
        params.append(department)
    if asset_type != "All":
        status_query += " AND asset_type = ?"
        params.append(asset_type)

    status_query += " GROUP BY status"

    df_status = pd.read_sql_query(status_query, conn, params=params)

    # Create pie chart
    fig_status = px.pie(
        df_status, values="count", names="status", title="Asset Distribution by Status"
    )
    st.plotly_chart(fig_status)

    # Asset value by department
    value_query = """
        SELECT 
            department,
            COUNT(*) as asset_count,
            SUM(current_value) as total_value,
            SUM(original_value) as total_original_value,
            SUM(original_value - current_value) as total_depreciation
        FROM assets
        WHERE 1=1
    """

    if asset_type != "All":
        value_query += " AND asset_type = ?"

    value_query += " GROUP BY department"

    df_value = pd.read_sql_query(
        value_query, conn, params=[asset_type] if asset_type != "All" else []
    )

    st.dataframe(
        df_value,
        column_config={
            "total_value": st.column_config.NumberColumn(
                "Total Current Value", format="$%.2f"
            ),
            "total_original_value": st.column_config.NumberColumn(
                "Total Original Value", format="$%.2f"
            ),
            "total_depreciation": st.column_config.NumberColumn(
                "Total Depreciation", format="$%.2f"
            ),
        },
        hide_index=True,
    )

    conn.close()


def show_financial_reports():
    st.subheader("Financial Reports")

    report_type = st.selectbox(
        "Report Type", ["Asset Valuation", "Depreciation Analysis", "Disposal Summary"]
    )

    # Date range filter
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date", value=datetime.now() - timedelta(days=365)
        )
    with col2:
        end_date = st.date_input("End Date", value=datetime.now())

    conn = get_database_connection()

    if report_type == "Asset Valuation":
        query = """
            SELECT 
                asset_type,
                COUNT(*) as asset_count,
                SUM(original_value) as total_original_value,
                SUM(current_value) as total_current_value,
                SUM(original_value - current_value) as total_depreciation,
                AVG(current_value/original_value) * 100 as avg_remaining_value_percent
            FROM assets
            WHERE acquisition_date BETWEEN ? AND ?
            GROUP BY asset_type
        """

        df = pd.read_sql_query(query, conn, params=[start_date, end_date])

        st.dataframe(df, hide_index=True)

        # Visualization
        fig = px.bar(
            df,
            x="asset_type",
            y=["total_original_value", "total_current_value"],
            title="Asset Values by Type",
            barmode="group",
        )
        st.plotly_chart(fig)

    elif report_type == "Depreciation Analysis":
        query = """
            SELECT 
                a.asset_type,
                strftime('%Y', a.acquisition_date) as year,
                COUNT(*) as asset_count,
                SUM(a.original_value - a.current_value) as total_depreciation,
                AVG((a.original_value - a.current_value)/a.original_value) * 100 as avg_depreciation_percent
            FROM assets a
            WHERE a.acquisition_date BETWEEN ? AND ?
            GROUP BY a.asset_type, year
            ORDER BY year, a.asset_type
        """

        df = pd.read_sql_query(query, conn, params=[start_date, end_date])

        st.dataframe(df, hide_index=True)

        # Visualization
        fig = px.line(
            df,
            x="year",
            y="avg_depreciation_percent",
            color="asset_type",
            title="Depreciation Trends by Asset Type",
        )
        st.plotly_chart(fig)

    else:  # Disposal Summary
        query = """
            SELECT 
                a.asset_type,
                COUNT(*) as disposal_count,
                SUM(d.selling_price) as total_selling_price,
                SUM(a.original_value) as total_original_value,
                SUM(d.selling_price - a.original_value) as total_gain_loss
            FROM disposal d
            JOIN assets a ON d.asset_id = a.asset_id
            WHERE d.disposal_date BETWEEN ? AND ?
            GROUP BY a.asset_type
        """

        df = pd.read_sql_query(query, conn, params=[start_date, end_date])

        st.dataframe(df, hide_index=True)

        # Visualization
        fig = px.bar(
            df,
            x="asset_type",
            y="total_gain_loss",
            title="Gain/Loss on Disposal by Asset Type",
        )
        st.plotly_chart(fig)

    conn.close()

    # Export button
    if not df.empty:
        excel_data = export_to_excel(df, f"{report_type.lower()}_report.xlsx")
        st.download_button(
            label="Download Report",
            data=excel_data,
            file_name=f"{report_type.lower()}_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


def show_depreciation_schedule():
    st.subheader("Depreciation Schedule")

    # Filters with unique keys
    col1, col2 = st.columns(2)
    with col1:
        asset_type = st.selectbox(
            "Asset Type",
            ["All"] + get_asset_types(),
            key="depreciation_asset_type_filter"
        )
    with col2:
        forecast_years = st.number_input(
            "Forecast Years",
            min_value=1,
            max_value=10,
            value=5,
            key="depreciation_forecast_years"
        )

    conn = get_database_connection()

    # Get active assets
    query = """
        SELECT 
            asset_id,
            asset_name,
            asset_type,
            acquisition_date,
            original_value,
            current_value,
            depreciation_rate,
            depreciation_method
        FROM assets
        WHERE status = 'active'
    """

    if asset_type != "All":
        query += " AND asset_type = ?"
        params = [asset_type]
    else:
        params = []

    df_assets = pd.read_sql_query(query, conn, params=params)

    if df_assets.empty:
        st.info("No active assets found for the selected criteria.")
        conn.close()
        return

    # Calculate future depreciation
    forecast_data = []
    current_year = datetime.now().year

    for _, asset in df_assets.iterrows():
        value_start = float(asset["current_value"])
        
        for year in range(current_year, current_year + forecast_years):
            if asset["depreciation_method"] == "Straight-Line":
                annual_depreciation = float(asset["original_value"]) * (
                    float(asset["depreciation_rate"]) / 100
                )
            else:  # Reducing Balance
                annual_depreciation = value_start * (float(asset["depreciation_rate"]) / 100)

            value_end = max(0, value_start - annual_depreciation)

            forecast_data.append({
                "asset_name": asset["asset_name"],
                "asset_type": asset["asset_type"],
                "year": year,
                "value_start": value_start,
                "depreciation": annual_depreciation,
                "value_end": value_end
            })

            value_start = value_end

    df_forecast = pd.DataFrame(forecast_data)

    # Display summary by asset type and year
    df_summary = df_forecast.groupby(["asset_type", "year"]).agg({
        "depreciation": "sum",
        "value_end": "sum"
    }).reset_index()

    # Display summary table with formatted columns
    st.dataframe(
        df_summary,
        column_config={
            "year": "Year",
            "asset_type": "Asset Type",
            "depreciation": st.column_config.NumberColumn(
                "Annual Depreciation",
                format="$%.2f"
            ),
            "value_end": st.column_config.NumberColumn(
                "End Value",
                format="$%.2f"
            )
        },
        hide_index=True
    )

    # Visualization
    fig = px.line(
        df_summary,
        x="year",
        y="value_end",
        color="asset_type",
        title="Projected Asset Values Over Time",
        labels={
            "year": "Year",
            "value_end": "Asset Value",
            "asset_type": "Asset Type"
        }
    )
    
    # Customize the chart
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Asset Value ($)",
        legend_title="Asset Type",
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # Detailed view with expandable sections
    st.subheader("Detailed Asset Depreciation")
    for asset_name in df_forecast["asset_name"].unique():
        asset_data = df_forecast[df_forecast["asset_name"] == asset_name]
        
        with st.expander(f"Details for {asset_name}"):
            st.dataframe(
                asset_data[["year", "value_start", "depreciation", "value_end"]],
                column_config={
                    "year": "Year",
                    "value_start": st.column_config.NumberColumn(
                        "Starting Value",
                        format="$%.2f"
                    ),
                    "depreciation": st.column_config.NumberColumn(
                        "Depreciation",
                        format="$%.2f"
                    ),
                    "value_end": st.column_config.NumberColumn(
                        "Ending Value",
                        format="$%.2f"
                    )
                },
                hide_index=True
            )

    # Export functionality
    if st.button("Export Depreciation Schedule", key="export_depreciation"):
        excel_data = export_to_excel(df_forecast, "depreciation_schedule.xlsx")
        st.download_button(
            label="Download Schedule",
            data=excel_data,
            file_name="depreciation_schedule.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_depreciation"
        )

    conn.close()


def show_audit_trail():
    st.subheader("Audit Trail")

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        date_range = st.date_input(
            "Date Range", value=(datetime.now() - timedelta(days=30), datetime.now())
        )

    with col2:
        user_filter = st.selectbox("User", ["All"] + get_users())

    with col3:
        action_filter = st.selectbox(
            "Action",
            [
                "All",
                "Asset created",
                "Asset modified",
                "Asset disposed",
                "maintenance scheduled",
                "maintenance completed",
            ],
        )

    # Fetch audit logs
    conn = get_database_connection()
    query = """
        SELECT 
            l.log_id,
            l.timestamp,
            u.username,
            l.action,
            a.asset_name,
            a.asset_type
        FROM audit_logs l
        JOIN users u ON l.user_id = u.user_id
        JOIN assets a ON l.asset_id = a.asset_id
        WHERE l.timestamp BETWEEN ? AND ?
    """
    params = [date_range[0], date_range[1]]

    if user_filter != "All":
        query += " AND u.username = ?"
        params.append(user_filter)

    if action_filter != "All":
        query += " AND l.action = ?"
        params.append(action_filter)

    query += " ORDER BY l.timestamp DESC"

    df = pd.read_sql_query(query, conn, params=params)

    if not df.empty:
        st.dataframe(df, hide_index=True)

        # Export button
        excel_data = export_to_excel(df, "audit_trail.xlsx")
        st.download_button(
            label="Download Audit Trail",
            data=excel_data,
            file_name="audit_trail.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.info("No audit records found for the selected criteria.")

    conn.close()


def show_custom_reports():
    st.subheader("Custom Reports")

    # Save report template
    with st.expander("Save Report Template"):
        template_name = st.text_input("Template Name")

        col1, col2 = st.columns(2)
        with col1:
            selected_fields = st.multiselect("Select Fields", get_available_fields())

        with col2:
            selected_filters = st.multiselect("Select Filters", get_available_filters())

        if st.button("Save Template"):
            save_report_template(template_name, selected_fields, selected_filters)
            st.success("Template saved successfully!")

    # Load and run saved reports
    st.subheader("Saved Reports")

    templates = get_saved_templates()
    if templates:
        selected_template = st.selectbox(
            "Select Template", options=list(templates.keys())
        )

        if selected_template:
            template = templates[selected_template]

            # Apply filters
            filter_values = {}
            for filter_name in template["filters"]:
                filter_values[filter_name] = st.selectbox(
                    f"Filter by {filter_name}", get_filter_options(filter_name)
                )

            if st.button("Run Report"):
                df = run_custom_report(template, filter_values)

                if not df.empty:
                    st.dataframe(df, hide_index=True)

                    # Export button
                    excel_data = export_to_excel(df, f"{selected_template}.xlsx")
                    st.download_button(
                        label="Download Report",
                        data=excel_data,
                        file_name=f"{selected_template}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )
                else:
                    st.info("No data found for the selected criteria.")
    else:
        st.info("No saved report templates found.")


# User Management Module
def show_user_management():
    if st.session_state.user_role != "admin":
        st.error("Access denied. Admin privileges required.")
        return

    st.title("👥 User Management")

    tabs = st.tabs(["Users List", "Add User", "Role Management"])

    with tabs[0]:
        show_users_list()

    with tabs[1]:
        show_add_user_form()

    with tabs[2]:
        show_role_management()


def show_users_list():
    st.subheader("Users List")

    conn = get_database_connection()
    df = pd.read_sql_query(
        """
        SELECT 
            user_id,
            username,
            role,
            department,
            (SELECT COUNT(*) FROM audit_logs WHERE user_id = users.user_id) as action_count
        FROM users
    """,
        conn,
    )

    for _, user in df.iterrows():
        with st.expander(f"{user['username']} ({user['role']})"):
            col1, col2, col3 = st.columns(3)

            with col1:
                st.write(f"Department: {user['department']}")
            with col2:
                st.write(f"Actions: {user['action_count']}")
            with col3:
                if user["username"] != "admin":  # Prevent admin user modification
                    if st.button("Reset Password", key=f"reset_{user['user_id']}"):
                        reset_user_password(user["user_id"])
                    if st.button("Delete User", key=f"delete_{user['user_id']}"):
                        delete_user(user["user_id"])
                        st.rerun()

    conn.close()


def show_add_user_form():
    st.subheader("Add New User")

    col1, col2 = st.columns(2)

    with col1:
        username = st.text_input("Username*")
        password = st.text_input("Password*", type="password")

    with col2:
        role = st.selectbox("Role*", ["staff", "management", "admin"])
        department = st.text_input("Department*")

    if st.button("Add User"):
        if not all([username, password, role, department]):
            st.error("Please fill in all required fields.")
            return

        try:
            conn = get_database_connection()
            c = conn.cursor()

            # Check if username exists
            c.execute("SELECT 1 FROM users WHERE username = ?", (username,))
            if c.fetchone():
                st.error("Username already exists.")
                return

            # Hash password and create user
            hashed_password = hash_password(password)

            c.execute(
                """
                INSERT INTO users (username, password_hash, role, department)
                VALUES (?, ?, ?, ?)
            """,
                (username, hashed_password, role, department),
            )

            conn.commit()
            st.success("User created successfully!")

        except sqlite3.Error as e:
            st.error(f"An error occurred: {e}")
        finally:
            conn.close()


def show_role_management():
    st.subheader("Role Management")

    # Display role permissions
    role_permissions = {
        "admin": [
            "Full system access",
            "User management",
            "System configuration",
            "All asset operations",
            "Report generation",
        ],
        "management": [
            "View all assets",
            "Approve transactions",
            "Generate reports",
            "View audit trails",
        ],
        "staff": ["View assigned assets", "Request maintenance", "Basic reports"],
    }

    for role, permissions in role_permissions.items():
        with st.expander(f"{role.title()} Role"):
            for permission in permissions:
                st.write(f"✓ {permission}")


# Settings Module
def show_settings():
    if st.session_state.user_role != "admin":
        st.error("Access denied. Admin privileges required.")
        return

    st.title("⚙️ System Settings")

    tabs = st.tabs(
        [
            "General Settings",
            "Depreciation Rules",
            "Location Management",
            "System Backup",
        ]
    )

    with tabs[0]:
        show_general_settings()

    with tabs[1]:
        show_depreciation_settings()

    with tabs[2]:
        show_location_management()

    with tabs[3]:
        show_system_backup()


def show_general_settings():
    st.subheader("General Settings")

    settings = load_settings()

    # Company Information
    st.write("Company Information")
    company_name = st.text_input("Company Name", value=settings.get("company_name", ""))
    company_address = st.text_area(
        "Company Address", value=settings.get("company_address", "")
    )

    # System Settings
    st.write("System Settings")
    session_timeout = st.number_input(
        "Session Timeout (minutes)",
        min_value=5,
        max_value=120,
        value=settings.get("session_timeout", 30),
    )

    # Email Settings
    st.write("Email Notifications")
    enable_emails = st.checkbox(
        "Enable Email Notifications", value=settings.get("enable_emails", False)
    )
    if enable_emails:
        email_server = st.text_input(
            "SMTP Server", value=settings.get("email_server", "")
        )
        email_port = st.number_input("SMTP Port", value=settings.get("email_port", 587))

    if st.button("Save Settings"):
        settings.update(
            {
                "company_name": company_name,
                "company_address": company_address,
                "session_timeout": session_timeout,
                "enable_emails": enable_emails,
                "email_server": email_server if enable_emails else "",
                "email_port": email_port if enable_emails else 587,
            }
        )
        save_settings(settings)
        st.success("Settings saved successfully!")


def show_depreciation_settings():
    st.subheader("Depreciation Rules")

    conn = get_database_connection()
    c = conn.cursor()

    # Get current rules
    c.execute(
        """
        SELECT DISTINCT asset_type, depreciation_rate, depreciation_method
        FROM assets
        GROUP BY asset_type
    """
    )
    current_rules = c.fetchall()

    # Display and edit rules
    for asset_type, rate, method in current_rules:
        with st.expander(f"{asset_type} Settings"):
            col1, col2 = st.columns(2)

            with col1:
                new_rate = st.number_input(
                    "Depreciation Rate (%)",
                    min_value=0.0,
                    max_value=100.0,
                    value=float(rate),
                    key=f"rate_{asset_type}",
                )

            with col2:
                new_method = st.selectbox(
                    "Depreciation Method",
                    ["Straight-Line", "Reducing Balance"],
                    index=0 if method == "Straight-Line" else 1,
                    key=f"method_{asset_type}",
                )

            if st.button("Update", key=f"update_{asset_type}"):
                update_depreciation_rule(asset_type, new_rate, new_method)
                st.success(f"Updated depreciation rules for {asset_type}")

    conn.close()


def show_location_management():
    st.subheader("Location Management")

    # Add new location
    with st.expander("Add New Location"):
        location_name = st.text_input("Location Name")
        if st.button("Add Location"):
            add_location(location_name)
            st.success("Location added successfully!")
            st.rerun()

    # List and manage locations
    conn = get_database_connection()
    df = pd.read_sql_query(
        """
        SELECT 
            l.location_id,
            l.location_name,
            COUNT(a.asset_id) as asset_count
        FROM locations l
        LEFT JOIN assets a ON l.location_id = a.location_id
        GROUP BY l.location_id
    """,
        conn,
    )

    for _, location in df.iterrows():
        with st.expander(f"{location['location_name']}"):
            st.write(f"Assets: {location['asset_count']}")
            if location["asset_count"] == 0:
                if st.button("Delete", key=f"delete_{location['location_id']}"):
                    delete_location(location["location_id"])
                    st.rerun()

    conn.close()


def show_system_backup():
    st.subheader("System Backup")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Create Backup"):
            backup_file = create_backup()
            st.success("Backup created successfully!")

            # Provide download link
            with open(backup_file, "rb") as f:
                st.download_button(
                    label="Download Backup",
                    data=f,
                    file_name=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db",
                    mime="application/octet-stream",
                )

    with col2:
        uploaded_file = st.file_uploader("Restore from Backup", type=["db"])
        if uploaded_file is not None:
            if st.button("Restore System"):
                restore_backup(uploaded_file)
                st.success("System restored successfully!")
                st.rerun()


# Utility functions for settings
def load_settings():
    try:
        with open("settings.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_settings(settings):
    with open("settings.json", "w") as f:
        json.dump(settings, f)


def update_depreciation_rule(asset_type, rate, method):
    conn = get_database_connection()
    c = conn.cursor()

    try:
        c.execute(
            """
            UPDATE assets
            SET depreciation_rate = ?,
                depreciation_method = ?
            WHERE asset_type = ?
        """,
            (rate, method, asset_type),
        )

        conn.commit()
    finally:
        conn.close()


def create_backup():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"backup_{timestamp}.db"

    conn = get_database_connection()
    backup_conn = sqlite3.connect(backup_file)

    try:
        conn.backup(backup_conn)
        return backup_file
    finally:
        backup_conn.close()
        conn.close()


def restore_backup(backup_file):
    # Create a temporary file
    temp_file = "temp_restore.db"

    with open(temp_file, "wb") as f:
        f.write(backup_file.read())

    # Verify backup file
    try:
        verify_conn = sqlite3.connect(temp_file)
        verify_conn.cursor().execute("SELECT 1 FROM users LIMIT 1")
        verify_conn.close()
    except sqlite3.Error:
        os.remove(temp_file)
        raise ValueError("Invalid backup file")

    # Restore
    conn = get_database_connection()
    backup_conn = sqlite3.connect(temp_file)

    try:
        backup_conn.backup(conn)
    finally:
        backup_conn.close()
        conn.close()
        os.remove(temp_file)


# Main application structure
def show_main_application():
    st.sidebar.title(f"Welcome, {st.session_state.username}")

    # Sidebar navigation
    menu_options = {
        "Dashboard": "📊",
        "Asset Management": "💼",
        "Transactions": "🔄",
        "Maintenance": "🔧",
        "Reports": "📈",
        "Settings": "⚙️",
    }

    if st.session_state.user_role == "admin":
        menu_options["User Management"] = "👥"

    selected_page = st.sidebar.selectbox(
        "Navigation",
        options=list(menu_options.keys()),
        format_func=lambda x: f"{menu_options[x]} {x}",
    )

    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.username = None
        st.session_state.user_role = None
        st.rerun()

    # Page routing
    if selected_page == "Dashboard":
        show_dashboard()
    elif selected_page == "Asset Management":
        show_asset_management()
    elif selected_page == "Transactions":
        show_transactions()
    elif selected_page == "Maintenance":
        show_maintenance()
    elif selected_page == "Reports":
        show_reports()
    elif selected_page == "Settings":
        show_settings()
    elif selected_page == "User Management" and st.session_state.user_role == "admin":
        show_user_management()


# Placeholder functions for different pages
def show_dashboard():
    # Custom CSS for better styling
    st.markdown("""
        <style>
        .metric-card {
            background-color: #f8f9fa;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .metric-label {
            color: #6c757d;
            font-size: 0.9rem;
            font-weight: 500;
        }
        .metric-value {
            color: #212529;
            font-size: 1.8rem;
            font-weight: 600;
        }
        .metric-delta {
            font-size: 0.8rem;
            padding-top: 5px;
        }
        .section-header {
            padding: 15px 0;
            margin: 25px 0 15px 0;
            border-bottom: 2px solid #dee2e6;
            color: #345;
        }
        </style>
    """, unsafe_allow_html=True)

    # Dashboard Header
    st.title("📊 Asset Management Dashboard")
    
    # Database connection
    conn = get_database_connection()

    # Key Metrics Section
    st.markdown("<h3 class='section-header'>Key Performance Metrics</h3>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Total Active Assets
    with col1:
        total_assets = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM assets WHERE status = 'active'", 
            conn
        ).iloc[0]['count']
        
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Total Active Assets</div>
                <div class="metric-value">{}</div>
                <div class="metric-delta">+{} this month</div>
            </div>
        """.format(total_assets, get_assets_count_change(conn)), unsafe_allow_html=True)

    # Total Asset Value
    with col2:
        total_value = pd.read_sql_query(
            "SELECT COALESCE(SUM(current_value), 0) as value FROM assets WHERE status = 'active'", 
            conn
        ).iloc[0]['value']
        
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Total Asset Value</div>
                <div class="metric-value">${:,.2f}</div>
                <div class="metric-delta">{:+.1f}% from last month</div>
            </div>
        """.format(total_value, get_asset_value_change(conn)), unsafe_allow_html=True)

    # Assets in Maintenance
    with col3:
        maintenance_count = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM maintenance WHERE status = 'in progress'", 
            conn
        ).iloc[0]['count']
        
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">In Maintenance</div>
                <div class="metric-value">{}</div>
                <div class="metric-label">Active Cases</div>
            </div>
        """.format(maintenance_count), unsafe_allow_html=True)

    # Depreciation Overview
    with col4:
        total_depreciation = pd.read_sql_query("""
            SELECT COALESCE(SUM(original_value - current_value), 0) as depreciation 
            FROM assets WHERE status = 'active'
        """, conn).iloc[0]['depreciation']
        
        st.markdown("""
            <div class="metric-card">
                <div class="metric-label">Total Depreciation</div>
                <div class="metric-value">${:,.2f}</div>
                <div class="metric-label">Accumulated</div>
            </div>
        """.format(total_depreciation), unsafe_allow_html=True)

    # Asset Distribution Section
    st.markdown("<h3 class='section-header'>Asset Distribution Analysis</h3>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Asset Type Distribution
        asset_distribution = pd.read_sql_query("""
            SELECT 
                asset_type,
                COUNT(*) as count,
                COALESCE(SUM(current_value), 0) as total_value
            FROM assets
            WHERE status = 'active'
            GROUP BY asset_type
        """, conn)

        if not asset_distribution.empty:
            fig = px.pie(
                asset_distribution,
                values='total_value',
                names='asset_type',
                title='Asset Value Distribution by Type',
                hole=0.4  # Makes it a donut chart
            )
            fig.update_layout(
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Department Distribution
        dept_value = pd.read_sql_query("""
            SELECT 
                department,
                COALESCE(SUM(current_value), 0) as total_value
            FROM assets
            WHERE status = 'active' AND department IS NOT NULL
            GROUP BY department
            ORDER BY total_value DESC
        """, conn)

        if not dept_value.empty:
            fig = px.bar(
                dept_value,
                x='department',
                y='total_value',
                title='Asset Value by Department',
                color='total_value',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(
                xaxis_title="Department",
                yaxis_title="Total Value ($)",
                showlegend=False
            )
            st.plotly_chart(fig, use_container_width=True)

    # Recent Activities Section
    st.markdown("<h3 class='section-header'>Recent Activities & Maintenance</h3>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Recent Transactions")
        recent_transactions = pd.read_sql_query("""
            SELECT 
                t.transaction_date,
                t.transaction_type,
                a.asset_name,
                t.department,
                CASE 
                    WHEN t.transaction_type = 'disposal' THEN d.selling_price
                    ELSE a.current_value
                END as value
            FROM transactions t
            JOIN assets a ON t.asset_id = a.asset_id
            LEFT JOIN disposal d ON t.asset_id = d.asset_id
            ORDER BY t.transaction_date DESC
            LIMIT 5
        """, conn)

        if not recent_transactions.empty:
            st.dataframe(
                recent_transactions,
                column_config={
                    "transaction_date": st.column_config.DateColumn("Date"),
                    "transaction_type": "Type",
                    "asset_name": "Asset",
                    "department": "Department",
                    "value": st.column_config.NumberColumn("Value", format="$%.2f")
                },
                hide_index=True
            )

    with col2:
        st.subheader("Maintenance Status")
        maintenance_overview = pd.read_sql_query("""
            SELECT 
                m.status,
                COUNT(*) as count,
                GROUP_CONCAT(a.asset_name) as assets
            FROM maintenance m
            JOIN assets a ON m.asset_id = a.asset_id
            GROUP BY m.status
        """, conn)

        if not maintenance_overview.empty:
            fig = px.pie(
                maintenance_overview,
                values='count',
                names='status',
                title='Maintenance Status Distribution',
                color_discrete_sequence=['#ff9999', '#66b3ff']
            )
            st.plotly_chart(fig, use_container_width=True)

    # Export Data Section
    st.markdown("<h3 class='section-header'>Export Dashboard Data</h3>", unsafe_allow_html=True)
    
    if st.button("Export Dashboard Data"):
        # Create Excel writer object
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Get workbook and add formats
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'font_color': 'white',
                'bg_color': '#2c3e50',
                'border': 1,
                'align': 'center'
            })
            money_format = workbook.add_format({'num_format': '$#,##0.00'})
            date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
            
            # Asset Summary Sheet
            asset_summary = pd.read_sql_query("""
                SELECT 
                    asset_name, asset_type, status, department,
                    acquisition_date, current_value, original_value,
                    depreciation_rate
                FROM assets
                ORDER BY asset_type, asset_name
            """, conn)
            
            asset_summary.to_excel(writer, sheet_name='Asset Summary', index=False)
            worksheet = writer.sheets['Asset Summary']
            
            # Format headers
            for col_num, value in enumerate(asset_summary.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Format columns
            worksheet.set_column('A:A', 30)  # Asset Name
            worksheet.set_column('B:B', 20)  # Asset Type
            worksheet.set_column('C:D', 15)  # Status, Department
            worksheet.set_column('E:E', 12, date_format)  # Acquisition Date
            worksheet.set_column('F:G', 15, money_format)  # Values
            worksheet.set_column('H:H', 12)  # Depreciation Rate
            
            # Department Summary Sheet
            dept_summary = pd.read_sql_query("""
                SELECT 
                    department,
                    COUNT(*) as asset_count,
                    SUM(current_value) as total_value,
                    AVG(current_value) as average_value,
                    SUM(original_value - current_value) as total_depreciation
                FROM assets
                WHERE department IS NOT NULL
                GROUP BY department
            """, conn)
            
            dept_summary.to_excel(writer, sheet_name='Department Summary', index=False)
            worksheet = writer.sheets['Department Summary']
            
            # Format headers and columns
            for col_num, value in enumerate(dept_summary.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            worksheet.set_column('A:A', 20)  # Department
            worksheet.set_column('B:B', 12)  # Asset Count
            worksheet.set_column('C:E', 15, money_format)  # Value columns
            
            # Add charts sheet
            chart_data = asset_distribution.copy()
            chart_data.to_excel(writer, sheet_name='Charts', index=False)
            worksheet = writer.sheets['Charts']
            
            # Add pie chart
            pie_chart = workbook.add_chart({'type': 'pie'})
            pie_chart.add_series({
                'name': 'Asset Distribution',
                'categories': ['Charts', 1, 0, len(chart_data), 0],
                'values': ['Charts', 1, 2, len(chart_data), 2]
            })
            worksheet.insert_chart('E2', pie_chart)

        # Offer the Excel file for download
        excel_data = output.getvalue()
        st.download_button(
            label="📥 Download Excel Report",
            data=excel_data,
            file_name=f"asset_dashboard_report_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    conn.close()


def show_asset_management():
    st.title("💼 Asset Management")
    
    tabs = st.tabs(["Asset List", "Add New Asset", "Bulk Import", "Asset Categories"])
    
    with tabs[0]:
        show_asset_list()
    
    with tabs[1]:
        show_add_asset_form()
    
    with tabs[2]:
        show_bulk_import()
    
    with tabs[3]:
        show_asset_categories()

def show_asset_list():
    st.subheader("Asset List")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        asset_type_filter = st.selectbox("Filter by Asset Type", ["All"] + get_asset_types())
    with col2:
        status_filter = st.selectbox("Filter by Status", 
                                   ["All", "active", "inactive", "disposed", "transferred"])
    with col3:
        search_term = st.text_input("Search Assets", "")
    
    # Construct query based on filters
    query = """
        SELECT 
            a.asset_id,
            a.asset_name,
            a.description,
            a.brand,
            a.serial_number,
            a.acquisition_date,
            a.status,
            a.department,
            a.current_value,
            a.original_value,
            a.depreciation_rate,
            a.asset_type,
            l.location_name
        FROM assets a
        LEFT JOIN locations l ON a.location_id = l.location_id
        WHERE 1=1
    """
    params = []
    
    if asset_type_filter != "All":
        query += " AND a.asset_type = ?"
        params.append(asset_type_filter)
    
    if status_filter != "All":
        query += " AND a.status = ?"
        params.append(status_filter)
    
    if search_term:
        query += """ AND (
            a.asset_name LIKE ? OR 
            a.description LIKE ? OR 
            a.serial_number LIKE ?
        )"""
        search_pattern = f"%{search_term}%"
        params.extend([search_pattern, search_pattern, search_pattern])
    
    # Execute query
    conn = get_database_connection()
    df = pd.read_sql_query(query, conn, params=params)
    
    if not df.empty:
        # Add action buttons for each asset
        for idx, row in df.iterrows():
            with st.expander(f"{row['asset_name']} - {row['asset_type']}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.write(f"**Status:** {row['status']}")
                    st.write(f"**Department:** {row['department']}")
                    st.write(f"**Location:** {row['location_name']}")
                
                with col2:
                    st.write(f"**Current Value:** ${row['current_value']:,.2f}")
                    st.write(f"**Original Value:** ${row['original_value']:,.2f}")
                    st.write(f"**Depreciation Rate:** {row['depreciation_rate']}%")
                
                with col3:
                    if st.button("Edit", key=f"edit_{row['asset_id']}"):
                        show_edit_asset_form(row['asset_id'])
                    if st.button("Delete", key=f"delete_{row['asset_id']}"):
                        delete_asset(row['asset_id'])
                        st.rerun()
    else:
        st.info("No assets found matching the criteria.")
    
    conn.close()

def show_edit_asset_form(asset_id):
    conn = get_database_connection()
    c = conn.cursor()
    
    # Get asset details
    c.execute("""
        SELECT * FROM assets WHERE asset_id = ?
    """, (asset_id,))
    asset = c.fetchone()
    
    if asset:
        st.subheader(f"Edit Asset: {asset['asset_name']}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            new_name = st.text_input("Asset Name", value=asset['asset_name'])
            new_type = st.selectbox("Asset Type", get_asset_types(), 
                                  index=get_asset_types().index(asset['asset_type']))
            new_status = st.selectbox("Status", 
                                    ["active", "inactive", "disposed", "transferred"],
                                    index=["active", "inactive", "disposed", "transferred"].index(asset['status']))
        
        with col2:
            new_department = st.text_input("Department", value=asset['department'])
            new_location = st.selectbox("Location", get_locations(), 
                                      index=get_locations().index(asset['location_id']))
            new_value = st.number_input("Current Value", 
                                      value=float(asset['current_value']))
        
        if st.button("Update Asset"):
            try:
                c.execute("""
                    UPDATE assets
                    SET asset_name = ?,
                        asset_type = ?,
                        status = ?,
                        department = ?,
                        location_id = ?,
                        current_value = ?
                    WHERE asset_id = ?
                """, (new_name, new_type, new_status, new_department, 
                     new_location, new_value, asset_id))
                
                conn.commit()
                st.success("Asset updated successfully!")
                
                # Log the action
                c.execute("""
                    INSERT INTO audit_logs (action, user_id, asset_id)
                    VALUES (?, ?, ?)
                """, ("Asset modified", get_user_id(st.session_state.username), asset_id))
                
                conn.commit()
                
            except sqlite3.Error as e:
                st.error(f"An error occurred: {e}")
            finally:
                conn.close()

def delete_asset(asset_id):
    if st.session_state.user_role != "admin":
        st.error("Only administrators can delete assets.")
        return
    
    conn = get_database_connection()
    c = conn.cursor()
    
    try:
        # Check if asset exists
        c.execute("SELECT asset_name FROM assets WHERE asset_id = ?", (asset_id,))
        asset = c.fetchone()
        
        if not asset:
            st.error("Asset not found.")
            return
        
        # Delete asset
        c.execute("DELETE FROM assets WHERE asset_id = ?", (asset_id,))
        
        # Log the action
        c.execute("""
            INSERT INTO audit_logs (action, user_id, asset_id)
            VALUES (?, ?, ?)
        """, ("Asset deleted", get_user_id(st.session_state.username), asset_id))
        
        conn.commit()
        st.success(f"Asset {asset['asset_name']} deleted successfully!")
        
    except sqlite3.Error as e:
        st.error(f"An error occurred: {e}")
    finally:
        conn.close()

# Additional functions that were undefined in the original code

def show_asset_categories():
    st.subheader("Asset Categories")
    
    conn = get_database_connection()
    
    # Display asset type statistics
    df = pd.read_sql_query("""
        SELECT 
            asset_type,
            COUNT(*) as total_assets,
            SUM(current_value) as total_value,
            AVG(current_value) as average_value,
            AVG(depreciation_rate) as avg_depreciation_rate
        FROM assets
        GROUP BY asset_type
    """, conn)
    
    st.dataframe(
        df,
        column_config={
            "total_value": st.column_config.NumberColumn("Total Value", format="$%.2f"),
            "average_value": st.column_config.NumberColumn("Average Value", format="$%.2f"),
            "avg_depreciation_rate": st.column_config.NumberColumn("Avg Depreciation Rate", format="%.1f%%")
        },
        hide_index=True
    )
    
    # Category Distribution Chart
    fig1 = px.pie(df, values='total_assets', names='asset_type', 
                 title='Asset Distribution by Category')
    st.plotly_chart(fig1, use_container_width=True)
    
    # Value Distribution Chart
    fig2 = px.bar(df, x='asset_type', y=['total_value', 'average_value'],
                 title='Asset Values by Category',
                 barmode='group')
    st.plotly_chart(fig2, use_container_width=True)
    
    conn.close()

def show_bulk_import():
    st.subheader("Bulk Import Assets")
    
    # Template download
    if st.button("Download Import Template"):
        template_data = {
            "asset_name": ["Example Asset"],
            "asset_type": ["Computer & Accessories"],
            "description": ["Asset description"],
            "brand": ["Brand name"],
            "serial_number": ["SN123456"],
            "acquisition_date": ["2023-01-01"],
            "department": ["IT"],
            "location_id": [1],
            "original_value": [1000.00]
        }
        df_template = pd.DataFrame(template_data)
        
        # Convert to Excel
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_template.to_excel(writer, index=False)
            
        st.download_button(
            label="Download Template",
            data=output.getvalue(),
            file_name="asset_import_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    # File upload
    uploaded_file = st.file_uploader("Upload Excel File", type=['xlsx'])
    
    if uploaded_file is not None:
        try:
            df = pd.read_excel(uploaded_file)
            st.write("Preview of uploaded data:")
            st.write(df.head())
            
            if st.button("Import Assets"):
                success_count = 0
                error_count = 0
                conn = get_database_connection()
                
                for _, row in df.iterrows():
                    try:
                        # Get depreciation settings
                        dep_rate, dep_method = get_asset_type_depreciation_rate(row['asset_type'])
                        
                        # Calculate current value
                        current_value = calculate_depreciation(
                            row['original_value'],
                            dep_rate,
                            row['acquisition_date'].strftime('%Y-%m-%d'),
                            dep_method
                        )
                        
                        # Insert asset
                        c = conn.cursor()
                        c.execute("""
                            INSERT INTO assets (
                                asset_name, asset_type, description, brand,
                                serial_number, acquisition_date, status,
                                location_id, department, current_value,
                                original_value, depreciation_rate,
                                depreciation_method
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            row['asset_name'], row['asset_type'], row['description'],
                            row['brand'], row['serial_number'], row['acquisition_date'],
                            'active', row['location_id'], row['department'],
                            current_value, row['original_value'], dep_rate, dep_method
                        ))
                        
                        conn.commit()
                        success_count += 1
                        
                    except Exception as e:
                        error_count += 1
                        st.error(f"Error importing {row['asset_name']}: {str(e)}")
                
                st.success(f"Import completed. {success_count} assets imported successfully, {error_count} errors.")
                conn.close()
                
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

def show_edit_asset_form(asset_id):
    conn = get_database_connection()
    c = conn.cursor()
    
    # Get asset details
    c.execute("""
        SELECT * FROM assets WHERE asset_id = ?
    """, (asset_id,))
    asset = c.fetchone()
    
    if asset:
        st.subheader(f"Edit Asset: {asset['asset_name']}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            new_name = st.text_input("Asset Name", value=asset['asset_name'])
            new_type = st.selectbox("Asset Type", get_asset_types(), 
                                  index=get_asset_types().index(asset['asset_type']))
            new_brand = st.text_input("Brand", value=asset['brand'])
            new_serial = st.text_input("Serial Number", value=asset['serial_number'])
        
        with col2:
            new_department = st.text_input("Department", value=asset['department'])
            new_location = st.selectbox("Location", get_locations(), 
                                      index=get_locations().index(asset['location_id']))
            new_status = st.selectbox("Status", 
                                    ["active", "inactive", "disposed", "transferred"],
                                    index=["active", "inactive", "disposed", "transferred"].index(asset['status']))
        
        new_description = st.text_area("Description", value=asset['description'])
        new_value = st.number_input("Current Value", value=float(asset['current_value']))
        
        if st.button("Update Asset"):
            try:
                c.execute("""
                    UPDATE assets
                    SET asset_name = ?,
                        asset_type = ?,
                        brand = ?,
                        serial_number = ?,
                        department = ?,
                        location_id = ?,
                        status = ?,
                        description = ?,
                        current_value = ?
                    WHERE asset_id = ?
                """, (new_name, new_type, new_brand, new_serial, new_department,
                     new_location, new_status, new_description, new_value, asset_id))
                
                # Log the update
                c.execute("""
                    INSERT INTO audit_logs (action, user_id, asset_id)
                    VALUES (?, ?, ?)
                """, ("Asset updated", get_user_id(st.session_state.username), asset_id))
                
                conn.commit()
                st.success("Asset updated successfully!")
                
            except sqlite3.Error as e:
                st.error(f"An error occurred: {e}")
            finally:
                conn.close()

def show_transactions():
    st.title("🔄 Transaction Management")
    
    tabs = st.tabs(["Transaction List", "New Transaction", "Bulk Transfer"])
    
    with tabs[0]:
        show_transaction_list()
    
    with tabs[1]:
        show_new_transaction_form()
    
    with tabs[2]:
        show_bulk_transfer_form()

def show_transaction_list():
    st.subheader("Transaction History")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        date_range = st.date_input(
            "Date Range",
            value=(datetime.now() - timedelta(days=30), datetime.now())
        )
    
    with col2:
        transaction_type = st.selectbox(
            "Transaction Type",
            ["All", "acquisition", "disposal", "transfer"]
        )
    
    with col3:
        department = st.selectbox("Department", ["All"] + get_departments())
    
    # Fetch transactions
    conn = get_database_connection()
    query = """
        SELECT 
            t.transaction_id,
            t.transaction_date,
            t.transaction_type,
            a.asset_name,
            t.department,
            t.buyer_seller,
            CASE 
                WHEN t.transaction_type = 'disposal' THEN d.selling_price
                ELSE a.current_value
            END as value
        FROM transactions t
        JOIN assets a ON t.asset_id = a.asset_id
        LEFT JOIN disposal d ON t.asset_id = d.asset_id
        WHERE t.transaction_date BETWEEN ? AND ?
    """
    params = [date_range[0], date_range[1]]
    
    if transaction_type != "All":
        query += " AND t.transaction_type = ?"
        params.append(transaction_type)
    
    if department != "All":
        query += " AND t.department = ?"
        params.append(department)
    
    df = pd.read_sql_query(query, conn, params=params)
    
    if not df.empty:
        st.dataframe(df, hide_index=True)
        
        # Export functionality
        if st.button("Export to Excel"):
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
            
            st.download_button(
                label="Download Excel file",
                data=output.getvalue(),
                file_name="transactions.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.info("No transactions found for the selected criteria.")
    
    conn.close()

def show_new_transaction_form():
    st.subheader("New Transaction")
    
    transaction_type = st.selectbox(
        "Transaction Type",
        ["acquisition", "disposal", "transfer"]
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if transaction_type == "acquisition":
            asset_name = st.text_input("Asset Name*")
            asset_type = st.selectbox("Asset Type*", get_asset_types())
            original_value = st.number_input("Purchase Value*", min_value=0.0)
        else:
            asset_id = st.selectbox(
                "Select Asset*",
                options=get_available_assets(),
                format_func=lambda x: get_asset_name(x)
            )
        
        department = st.text_input("Department*")
    
    with col2:
        transaction_date = st.date_input("Transaction Date*")
        buyer_seller = st.text_input(
            "Seller" if transaction_type == "acquisition" else "Buyer/Recipient"
        )
        
        if transaction_type == "disposal":
            selling_price = st.number_input("Selling Price", min_value=0.0)
    
    if st.button("Submit Transaction"):
        try:
            conn = get_database_connection()
            c = conn.cursor()
            
            if transaction_type == "acquisition":
                # Create new asset
                dep_rate, dep_method = get_asset_type_depreciation_rate(asset_type)
                current_value = calculate_depreciation(
                    original_value,
                    dep_rate,
                    transaction_date.strftime('%Y-%m-%d'),
                    dep_method
                )
                
                c.execute("""
                    INSERT INTO assets (
                        asset_name, asset_type, acquisition_date,
                        department, current_value, original_value,
                        depreciation_rate, depreciation_method, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    asset_name, asset_type, transaction_date.strftime('%Y-%m-%d'),
                    department, current_value, original_value,
                    dep_rate, dep_method, 'active'
                ))
                
                asset_id = c.lastrowid
                
            elif transaction_type == "disposal":
                # Record disposal
                c.execute("""
                    INSERT INTO disposal (
                        asset_id, disposal_date, selling_price
                    ) VALUES (?, ?, ?)
                """, (asset_id, transaction_date.strftime('%Y-%m-%d'), selling_price))
                
                # Update asset status
                c.execute("""
                    UPDATE assets 
                    SET status = 'disposed'
                    WHERE asset_id = ?
                """, (asset_id,))
                
            else:  # transfer
                # Update asset department
                c.execute("""
                    UPDATE assets 
                    SET department = ?,
                        status = 'transferred'
                    WHERE asset_id = ?
                """, (department, asset_id))
            
            # Record transaction
            c.execute("""
                INSERT INTO transactions (
                    asset_id, transaction_type, buyer_seller,
                    transaction_date, department
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                asset_id, transaction_type, buyer_seller,
                transaction_date.strftime('%Y-%m-%d'), department
            ))
            
            # Log the action
            c.execute("""
                INSERT INTO audit_logs (
                    action, user_id, asset_id
                ) VALUES (?, ?, ?)
            """, (
                f"{transaction_type} transaction",
                get_user_id(st.session_state.username),
                asset_id
            ))
            
            conn.commit()
            st.success("Transaction recorded successfully!")
            
        except sqlite3.Error as e:
            st.error(f"An error occurred: {e}")
        finally:
            conn.close()

def show_bulk_transfer_form():
    st.subheader("Bulk Transfer")
    
    # Asset selection
    assets = st.multiselect(
        "Select Assets to Transfer",
        options=get_available_assets(),
        format_func=lambda x: get_asset_name(x)
    )
    
    if assets:
        col1, col2 = st.columns(2)
        
        with col1:
            new_department = st.text_input("New Department*")
            transfer_date = st.date_input("Transfer Date*")
        
        with col2:
            recipient = st.text_input("Recipient")
            notes = st.text_area("Transfer Notes")
        
        if st.button("Process Bulk Transfer"):
            try:
                conn = get_database_connection()
                c = conn.cursor()
                
                for asset_id in assets:
                    # Update asset
                    c.execute("""
                        UPDATE assets 
                        SET department = ?,
                            status = 'transferred'
                        WHERE asset_id = ?
                    """, (new_department, asset_id))
                    
                    # Record transaction
                    c.execute("""
                        INSERT INTO transactions (
                            asset_id, transaction_type, buyer_seller,
                            transaction_date, department
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        asset_id, "transfer", recipient,
                        transfer_date.strftime('%Y-%m-%d'), new_department
                    ))
                    
                    # Log the action
                    c.execute("""
                        INSERT INTO audit_logs (
                            action, user_id, asset_id
                        ) VALUES (?, ?, ?)
                    """, (
                        "bulk transfer",
                        get_user_id(st.session_state.username),
                        asset_id
                    ))
                
                conn.commit()
                st.success(f"Successfully transferred {len(assets)} assets!")
                
            except sqlite3.Error as e:
                st.error(f"An error occurred: {e}")
            finally:
                conn.close()




if __name__ == "__main__":
    main()
