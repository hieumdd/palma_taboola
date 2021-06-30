from models import TopCampaignContent

def main(request):
    request_json = request.get_json()
    if request_json:
        job = TopCampaignContent(start=request_json.get('start'), end=request_json.get('end'))
    else:
        job = TopCampaignContent()
    
    responses = {
        "pipelines": "Taboola - TopCampaignContent",
        "results": job.run()
    }
    print(responses)
    return responses
