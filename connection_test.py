from snowflake.snowpark import Session

connection_params = {
    "account": "SQHVTHB-UX70775",
    "user": "CGBrian",
    "password": "W4whistle!W4whistle",
    "warehouse": "COMPUTE_WH",
    "database": "KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE",
    "schema": "PUBLIC"
}

session = Session.builder.configs(connection_params).create()
print("Connection Completed.")
print(session.sql("SELECT CURRENT_VERSION()").collect())