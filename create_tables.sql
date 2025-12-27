CREATE TABLE IF NOT EXISTS grades (
    id SERIAL PRIMARY KEY,
    grade_date TIMESTAMP NOT NULL,
    group_number VARCHAR(32) NOT NULL,
    full_name TEXT NOT NULL,
    grade SMALLINT NOT NULL CHECK (grade BETWEEN 1 AND 5)
);

CREATE INDEX IF NOT EXISTS idx_grades_fullname_grade ON grades (full_name, grade);
