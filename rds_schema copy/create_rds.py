"""Python script to create the PostgreSQL RDS database schema for the Tech Stock Research Tool.

This script connects to the RDS instance using credentials stored in environment variables,
and executes SQL commands to create the necessary tables and relationships.
"""
import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s : %(message)s",
    handlers=[
        logging.StreamHandler()  # also logs to stdout (for CloudWatch/containers)
    ]
)


# Load environment variables from .env file
load_dotenv()

# PostgreSQL schema definition
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "schema.sql")

# Retrieve RDS credentials from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")


def get_db_connection():
    """Establishes a connection to the PostgreSQL RDS database using environment credentials."""
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        logger.info("Successfully connected to the PostgreSQL RDS database.")
        return connection
    except psycopg2.DatabaseError as db_err:
        logger.error("Failed to connect to the PostgreSQL RDS database.")
        raise db_err


def execute_schema(connection, sql_content: str) -> None:
    """Executes the provided SQL commands to create the database schema."""
    cursor = connection.cursor()

    try:
        # Split on semicolons to execute each statement individually
        statements = [
            stmt.strip()
            for stmt in sql_content.split(";")
            if stmt.strip()  # Filter out empty strings
        ]

        for i, statement in enumerate(statements, start=1):
            # Log the first 50 chars of the statement
            logger.info(f"  Running SQL statement {i}: {statement[:50]}...")
            cursor.execute(statement)

        # Commit all changes to the database
        connection.commit()
        logger.info("\nSchema committed successfully.")

    except psycopg2.DatabaseError as db_err:
        # Roll back any partial changes if an error occurs
        connection.rollback()
        logger.error("\nDatabase error encountered. Transaction rolled back.")
        raise db_err

    finally:
        cursor.close()


def main():
    """Main function to create the PostgreSQL RDS database schema."""

    connection = None
    try:
        # Connect to the RDS database
        connection = get_db_connection()

        # Read the SQL schema from the file
        with open(SCHEMA_FILE, "r") as file:
            schema_sql = file.read()

        # Run the SQL commands to create the database schema
        execute_schema(connection, schema_sql)

        logger.info(f"Database schema created successfully in '{DB_NAME}'.")

    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()
