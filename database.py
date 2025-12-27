import os

import psycopg2
from psycopg2.extras import execute_values


class Database:
    def __init__(self, database_url: str | None = None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        if not self.database_url:
            raise RuntimeError("DATABASE_URL is not set")
        self.conn = psycopg2.connect(self.database_url)

    def create_table(self):
        create_sql = """
        CREATE TABLE IF NOT EXISTS grades (
            id SERIAL PRIMARY KEY,
            grade_date TIMESTAMP NOT NULL,
            group_number VARCHAR(32) NOT NULL,
            full_name TEXT NOT NULL,
            grade SMALLINT NOT NULL CHECK (grade BETWEEN 1 AND 5)
        );
        """
        with self.conn.cursor() as cur:
            cur.execute(create_sql)
        self.conn.commit()

    def insert_dataframe(self, df):
        with self.conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS grades")
        self.conn.commit()
        self.create_table()

        rows = list(
            df[["Дата", "Номер группы", "ФИО", "Оценка"]].itertuples(
                index=False, name=None
            )
        )
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO grades (grade_date, group_number, full_name, grade)
                VALUES %s
                """,
                rows,
            )
        self.conn.commit()

    def get_students_with_twos_more_than(self, threshold: int):
        query = """
        SELECT full_name, COUNT(*) AS count_twos
        FROM grades
        WHERE grade = 2
        GROUP BY full_name
        HAVING COUNT(*) > %s
        ORDER BY count_twos DESC, full_name
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (threshold,))
            rows = cur.fetchall()
        return [{"full_name": row[0], "count_twos": row[1]} for row in rows]

    def get_students_with_twos_less_than(self, threshold: int):
        query = """
        SELECT full_name, COUNT(*) AS count_twos
        FROM grades
        WHERE grade = 2
        GROUP BY full_name
        HAVING COUNT(*) < %s
        ORDER BY count_twos ASC, full_name
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (threshold,))
            rows = cur.fetchall()
        return [{"full_name": row[0], "count_twos": row[1]} for row in rows]

    def close(self):
        self.conn.close()
