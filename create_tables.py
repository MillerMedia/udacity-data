"""
This contains functions that create and drop tables.

This is the initial function to run for this program.
This drops (if needed) and then creates tables on Redshift.
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables if they exist."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create tables if they do not exists."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Create tables on Redshift cluster.

    First, drop tables if they already exist.
    Then create tables if they do not exist.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
        )
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
