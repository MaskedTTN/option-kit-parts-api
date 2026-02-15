def test_root_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "BMW Parts API" in data["message"]


def test_get_vehicle_not_found(client):
    response = client.get("/vehicles/NO_SUCH_VID")
    assert response.status_code == 404


def test_get_vehicle_success(client):
    response = client.get("/vehicles/TESTVID")
    assert response.status_code == 200
    data = response.json()
    assert data["vid"] == "TESTVID"


def test_search_parts_success(client):
    response = client.get("/parts/search", params={"q": "Control"})
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 1
    parts = data["parts"]
    assert any(p["description"] == "Control Module" for p in parts)


def test_search_parts_invalid_query_too_short(client):
    response = client.get("/parts/search", params={"q": "aa"})
    assert response.status_code == 422


def test_get_parts_by_option_success(client):
    response = client.get("/options/S710A")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert any(p["part_number"] == "1234" for p in data)


def test_get_parts_by_option_not_found(client):
    response = client.get("/options/S999")
    assert response.status_code == 404
