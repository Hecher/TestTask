from io import BytesIO
import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from database import Database

REQUIRED_COLUMNS = ["Дата", "Номер группы", "ФИО", "Оценка"]

app = FastAPI()
db = Database()
db.create_table()


@app.post("/upload-grades")
async def upload_grades(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="BAD REQUEST: expected a CSV file")

    content = await file.read()
    try:
        df = pd.read_csv(BytesIO(content), sep=";")
    except Exception as exc:
        raise HTTPException(status_code=400, detail="BAD REQUEST: invalid CSV") from exc

    if df.empty:
        raise HTTPException(status_code=400, detail="BAD REQUEST: CSV is empty")

    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "BAD REQUEST: missing columns",
                "columns": missing_columns,
            },
        )

    df = df.replace(r"^\s*$", pd.NA, regex=True)
    incomplete_rows = df[REQUIRED_COLUMNS].isna().any(axis=1)

    if incomplete_rows.any():
        row_numbers = (df.index[incomplete_rows] + 1).tolist()
        raise HTTPException(
            status_code=400,
            detail={
                "message": "BAD REQUEST: incomplete data",
                "rows": row_numbers,
            },
        )

    try:
        df["Дата"] = pd.to_datetime(
            df["Дата"], format="%d.%m.%Y", errors="raise"
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="BAD REQUEST: invalid date format"
        ) from exc

    db.insert_dataframe(df)
    unique_students = df["ФИО"].nunique()
    return {"status": "ok", "records_loaded": len(df), "students": unique_students}


@app.get("/students/more-than-3-twos")
def students_more_than_3_twos():
    return db.get_students_with_twos_more_than(3)


@app.get("/students/less-than-5-twos")
def students_less_than_5_twos():
    return db.get_students_with_twos_less_than(5)
