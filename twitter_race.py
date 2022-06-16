# connect to your instance using aws 'connect' page, then run this script. if instance goes down or is missing file, reupload this one with this
# scp -i ~/.ssh/wcd_can1.pem ~/Desktop/twitter_final.py ec2-user@[ip].ca-central-1.compute.amazonaws.com:~/data/
# check this stackoverflow if confused https://stackoverflow.com/questions/34869580/how-to-upload-local-system-files-to-amazon-ec2-using-ssh



import requests
import os
import json
import boto3

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAITebgEAAAAAGvX%2FRqvhQFbRkMDrtAUULgkCsNc%3DfQg1XPXt6L9h6oj9WcbNn3am12wb2BqWCDIGYBlh9U2lz3cfjc'

def get_log_client():
    log_client = boto3.client('logs')
    return log_client

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r
# def get_log_client():
#     log_client = boto3.client('logs')
#     return log_client

def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": '(racist OR racism OR racial OR race) '
                  '-is:retweet -sport -sports -athletic -athlete sample:80',
         "tag": "racism"}
        ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(set, log_client):
    tweet_fields = r'tweet.fields=id,text,created_at,lang,context_annotations' \
                   '&expansions=author_id,geo.place_id' \
                   '&place.fields=id,country_code' \
                   '&user.fields=id,name,usersname,public_metrics,location'

    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            response_line = response_line + b'\n'
            firehose_client.put_record(
               DeliveryStreamName=delivery_stream_name,
               Record={
                    'Data': response_line
               }
            )


def main():
    # log_client = get_log_client()
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set, log_client)

if __name__ == "__main__":

    session = boto3.Session()
    log_client = get_log_client()
    firehose_client = session.client('firehose', region_name='ca-central-1')
    delivery_stream_name = 'twitter-race-project'

    main()



