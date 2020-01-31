import google_translator as translator

TWITTER_PARAMS = {
    "IT_QUERY_FILTER": "-has:media place_country:IT",
    "IT_EN_QUERY_FILTER": "-has:media place_country:IT lang:en",
    "GENERIC_QUERY_FILTER": "-has:media",
    "RESULTS_PER_CALL": 10,  # 100,
    "MAX_RESULTS": 10,  # 100
}

TWITTER_COLS_MAP = {
    "text": "text_data",
    "links": "source_links",
    "handles": "source_writers",
    "tags": "text_tags",
    "created_time": "source_date"
}

NEWS_COLS_MAP = {
    "description": "text_data",
    "url": "source_links",
    "publishedAt": "source_date",
    "source": "source_writers"
}

NEWS_PARAMS = {
    "page_size": 1  # 100
}

QUERIES = ["early election", "snap election", "government collapse",
           "government coalition", "election", "instability", "uncertainty",
           "crisis", "coalition"]

QUERIES_IT = [q for q in map(lambda s: translator.translate_keyword(s, "it"),
                             QUERIES)]

FROM_USERS = ["lorepregliasco", "FerdiGiugliano", "AlbertoNardelli",
              "gavinjones10"]

NEWS_SOURCES = ",".join(["reuters", "ansa", "google-news-it"])

BERT_MODEL = {
    "endpoint_uri": "http://ac6a2064dee3c11e99ced0a13821e56d-733867741.ap"
                    "-southeast-1.elb.amazonaws.com/sentiment/classifier",
    "headers": {
        "content-type": "application/json"
    }
}

USERNAME = "*****"
PASSWORD = "*****"
HOST = "127.0.0.1"
PORT = "3306"
SCHEMA = "search_automation"
TABLE = "text_data_reserve"

GOOGLE_MIME_TYPES = {
    "spreadsheet": "application/vnd.google-apps.spreadsheet",
    "folder": "application/vnd.google-apps.folder"
}
