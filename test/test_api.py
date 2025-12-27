import requests

URL = "http://localhost:8000"

resp_upload_grades = {'status': 'ok', 'records_loaded': 10, 'students': 5}
resp_upload_grades_false = {"detail":{"message":"BAD REQUEST: incomplete data","rows":[1,4,8,10]}}
resp_more_than_3_twos = [{'full_name': 'Москвичев Андрей', 'count_twos': 6}]
resp_less_than_5_twos = [{'full_name': 'Третьяков Максим', 'count_twos': 1}]


def test_upload_grades():
    with open("./test_data.csv", "rb") as f:
        files = {"file": ("test_data.csv", f, "text/csv")}
        resp = requests.post(f"{URL}/upload-grades", files=files)
    assert resp.json() == resp_upload_grades

def test_upload_grades_false():
    with open("./test_data2.csv", "rb") as f:
        files = {"file": ("test_data.csv", f, "text/csv")}
        resp = requests.post(f"{URL}/upload-grades", files=files)
    assert resp.json() == resp_upload_grades_false

def test_more_than_3_twos():
    resp = requests.get(f"{URL}/students/more-than-3-twos")
    assert resp.json() == resp_more_than_3_twos

def test_less_than_5_twos():
    resp = requests.get(f"{URL}/students/less-than-5-twos")
    assert resp.json() == resp_less_than_5_twos