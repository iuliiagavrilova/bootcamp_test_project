from flask import Flask, request, jsonify, render_template
import snowflake.connector
import os

app = Flask(__name__)

CONNECTION_PARAMETERS = {
    "account": "AVYQISK-KL94175",
    "user": "LUMOSMASSIMO",
    "password": r'NWd#xfnf8N>3>RP',
    "role": "SYSADMIN",
    "database": "COVID_DB",
    "schema": "PUBLIC",
    "warehouse": "ANALYTICS_WH"
}

# Snowflake connection function
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=CONNECTION_PARAMETERS["user"],
        password=CONNECTION_PARAMETERS["password"],
        account=CONNECTION_PARAMETERS['account'],
        database=CONNECTION_PARAMETERS['database'],
        warehouse=CONNECTION_PARAMETERS['warehouse'],
        schema=CONNECTION_PARAMETERS['schema']
    )


@app.route('/')
def index():
    return render_template('index.html')    


@app.route('/query', methods = ['GET'])
def query_snowflake():
    try:
        # Extract user input
        country = request.args.get('country')
        print(country)

        # Create Snowflake query
        query = f"""
        SELECT country, year, total_cases, score 
        FROM joined_data_table
        WHERE country='{country}'
        """

        # Connect to Snowflake and execute query
        conn = get_snowflake_connection()
        print("connection made")
        cursor = conn.cursor()
        cursor.execute(query)
        print("query executed")
        results = cursor.fetchall()
        print(results)

        return jsonify(results), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
