from models.models import Taboola

def main(request):
    data = request.get_json(silent=True)
    print(data)

    if "table" in data:
        response = Taboola.factory(
            table=data["table"],
            start=data.get("start"),
            end=data.get("end"),
        ).run()
    else:
        raise ValueError(data)

    print(response)
    return response
