import json
import base64

from models import Taboola
from broadcast import broadcast


def main(request):
    request_json = request.get_json(silent=True)
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "broadcast" in data:
        results = broadcast(data)
    elif "table" in data:
        results = Taboola.factory(
            table=data["table"],
            start=data.get("start"),
            end=data.get("end"),
        ).run()
    else:
        raise NotImplementedError(data)

    responses = {"pipelines": "Taboola", "results": results}
    print(responses)
    return responses
