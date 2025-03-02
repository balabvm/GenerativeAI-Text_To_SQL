import os
os.environ["GRPC_VERBOSITY"] = "ERROR"  # Suppress gRPC warnings

import pyodbc
import streamlit as st
import google.generativeai as genai
from dotenv import load_dotenv
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    filename="app.log",  # Log file name
    level=logging.INFO,  # Log level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    datefmt="%Y-%m-%d %H:%M:%S"  # Date format
)

# Load environment variables
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Function to connect to SQL Server
def connect_db():
    try:
        conn = pyodbc.connect(
            "DRIVER={SQL Server};"
            "SERVER=10.10.0.9;"  # Replace with your server IP or hostname
            "DATABASE=MIS;"  # Replace with your database name
            "Trusted_Connection=yes;"  # Use Windows authentication
            "Timeout=30;"
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        logging.error(f"Database connection failed: {e}")  # Log the error
        return None

# Function to generate a valid SQL query using Gemini AI
def get_gemini_sql(question):
    prompt = f"""
    You are an AI that generates **valid SQL Server queries**.
    The table `Sample_Data` has the following schema:

    - ID (int) PRIMARY KEY
    - Name (varchar) NULLABLE
    - Age (int) NULLABLE
    - City (varchar) NULLABLE
    - Salary (decimal) NULLABLE
    - Join_Date (date) NULLABLE

    The user asked: "{question}"

    🔹 **Rules for Query Generation**:
    - If the user asks for a name containing an apostrophe (e.g., "O'Brien"), escape the apostrophe by doubling it (e.g., "O''Brien").
    - If the user asks "how many records", generate a `COUNT(*)` query.
    - If the user specifies a **month**, use `MONTH(Join_Date) = value`.
    - If the user specifies a **year**, use `YEAR(Join_Date) = value`.
    - Ensure the query is **100% correct**.

    🔹 **Example for Name "O'Brien"**:
    ```sql
    SELECT ID, Name, Age, City, Salary, Join_Date
    FROM Sample_Data
    WHERE Name = 'O''Brien';
    ```

    Now generate the SQL query based on the user's question.
    """

    model = genai.GenerativeModel('gemini-1.5-flash')  # Use the updated model
    response = model.generate_content([prompt, question])

    sql_query = response.text.strip()

    # Fix SQL formatting issues
    sql_query = sql_query.replace('"', "'")  # Ensure proper quoting
    sql_query = sql_query.strip("```sql").strip("```")  # Remove Markdown formatting if present

    # Ensure the query contains SELECT or COUNT(*) correctly
    if "SELECT" not in sql_query.upper() or "FROM" not in sql_query.upper():
        logging.warning(f"Invalid SQL query generated: {sql_query}")  # Log invalid queries
        return None  # Invalid SQL query

    logging.info(f"Generated SQL Query: {sql_query}")  # Log the generated query
    return sql_query

# Function to execute SQL query and fetch results
def execute_query(sql_query):
    conn = connect_db()
    if not conn:
        return "Error: Database connection failed."

    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)

        # Fetch all rows of data
        result = cursor.fetchall()

        if result:
            # Get column names from the cursor description
            columns = [column[0] for column in cursor.description]
            # Create a DataFrame
            df = pd.DataFrame.from_records(result, columns=columns)
            logging.info(f"Query executed successfully. Rows returned: {len(result)}")  # Log success
            return df
        else:
            logging.info("Query executed successfully. No data found.")  # Log no data
            return "No data found."
    except Exception as e:
        logging.error(f"Error executing query: {e}")  # Log the error
        return f"Error executing query: {e}"
    finally:
        cursor.close()
        conn.close()

# Streamlit UI
st.set_page_config(page_title="SQL Query Generator with AI")
st.header("SQL Query Generator with AI")
question = st.text_input("Ask a question: ", key="input")
submit = st.button("Generate SQL & Fetch Data")

if submit:
    st.write("Fetching Data...")
    logging.info(f"User Question: {question}")  # Log the user's question

    sql_query = get_gemini_sql(question)

    if sql_query:
        st.subheader("Generated SQL Query:")
        st.code(sql_query, language="sql")

        st.subheader("Query Results:")
        result = execute_query(sql_query)

        if isinstance(result, pd.DataFrame):  # If result is a DataFrame (detailed rows)
            st.dataframe(result)  # Display result as a table
        elif isinstance(result, int):  # If result is a count (integer)
            st.write(f"Count: {result}")
        else:  # If result is a string message
            st.write(f"Result: {result}")
    else:
        st.error("Failed to generate a valid SQL query. Please try again.")
