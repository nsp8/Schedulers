from datetime import datetime as dt
from html import unescape
from newsapi import NewsApiClient
from numpy import nan
from searchtweets import load_credentials, gen_rule_payload, collect_results
from time import sleep
from unidecode import unidecode
from urllib import parse
import constants as c
import drive_test as drive
import google_translator as translator
import json
import os
import pandas as pd
import re
import requests
import unicodedata

DRIVE_SERVICES = drive.test_drive()


def phrase_min_length(text_sample):
    """To get text with the least length from a collection of text values"""
    sample_df = pd.DataFrame(text_sample, columns=['text'])
    sample_df["length"] = [l for l in map(len, text_sample)]
    min_pos = sample_df["length"] == sample_df["length"].min()
    min_phrase = sample_df[min_pos]["text"]


def remove_emojis(input_string):
    """Function to remove emojis from a text string"""
    return_string = ""

    for character in input_string.strip():
        try:
            character.encode("ascii")
            return_string += character
        except UnicodeEncodeError:
            replaced = unidecode(str(character))
            if replaced not in ["", "[?]"]:
                return_string += replaced
    return return_string.strip()


def extract_components(clean_string):
    """Function to extract (and segregate) tweet-components:
    links, user-handles and hashtags"""
    token_list = re.split(r"\s+", clean_string.strip())
    url_ptrn = r"^https?:\/\/.*[\r\n]*"
    handle_ptrn = r"^\@.*"
    trend_ptrn = r"^#.*"
    url_collection = list()
    handle_collection = list()
    trend_collection = list()
    tokens = list()
    for token in token_list:
        # print("token = {}".format(token))
        url_match = re.search(url_ptrn, token) or re.match(url_ptrn, token)
        if url_match:
            # print("\t\tURL Pattern matched")
            url_collection.append(
                token) if token not in url_collection else False
            ix = token_list.index(token)
            token_list[ix] = ""
            continue
        handle_match = re.search(
            handle_ptrn, token) or re.match(handle_ptrn, token)
        if handle_match:
            # print("\t\tHandle Pattern matched")
            handle_collection.append(
                token) if token not in handle_collection else False
            ix = token_list.index(token)
            token_list[ix] = ""
            continue
        trend_match = re.search(
            trend_ptrn, token) or re.match(trend_ptrn, token)
        if trend_match:
            # print("\t\tTrend Pattern matched")
            trend_collection.append(
                token) if token not in trend_collection else False
            ix = token_list.index(token)
            token_list[ix] = ""
            continue
        if token.strip():
            tokens.append(token)
    del token_list
    return tokens, url_collection, handle_collection, trend_collection


def structure_text(entire_str):
    """Function to clean and structure the text data"""
    separations = dict()
    clean_str = remove_emojis(entire_str)
    tokens, links, handles, tags = extract_components(clean_str)
    clean_str = (" ".join(tokens)).strip()
    print("clean_str = {}".format(clean_str))
    cleaner_str = (re.subn(r"[:-]+", "", clean_str))[0].strip()
    try:
        cleaner_str = unescape(translator.translate_keyword(cleaner_str))
    except Exception as e:
        print("Exception occurred: {}".format(e))
        sleep(5)
        try:
            cleaner_str = unescape(translator.translate_keyword(cleaner_str))
        except Exception as e:
            cleaner_str = ""
    print("cleaner_str = {}".format(cleaner_str))
    separations['text'] = cleaner_str
    separations['links'] = ",".join(links)
    separations['handles'] = ",".join(handles)
    separations['tags'] = ",".join(tags)
    return separations


def format_date_str(date_string):
    """
    Function to include a zero for date values less than 10
    :param date_string:
    :return: string - month and day formatted with prefix
    """
    if int(date_string) < 10:
        return "0{}".format(date_string)
    else:
        return "{}".format(date_string)


def today(purpose="file"):
    """
    Function to return a formatting of current datetime (for file names)

    :param purpose: a string value corresponding to the usage of the date-string
    :return: string
    """
    string_format = output_string = ""
    if purpose == "file":
        string_format = "{}-{}-{}_{}-{}"
        output_string = string_format.format(dt.now().year,
                                             format_date_str(dt.now().month),
                                             format_date_str(dt.now().day),
                                             format_date_str(dt.now().hour),
                                             format_date_str(dt.now().minute))
    elif purpose == "datetime":
        string_format = "{}-{}-{} {}:{}:{}"
        output_string = string_format.format(dt.now().year,
                                             format_date_str(dt.now().month),
                                             format_date_str(dt.now().day),
                                             format_date_str(dt.now().hour),
                                             format_date_str(dt.now().minute),
                                             format_date_str(dt.now().second))
    return output_string


def get_month_days_map(year, month=None):
    month_days_map = {(1, 3, 5, 7, 8, 10, 12): 31, (4, 6, 9, 11): 30}
    if year % 4 == 0:
        month_days_map.update({(2,): 29})
    else:
        month_days_map.update({(2,): 28})
    month = 12 if month == 0 else month
    if month:
        for months, n_days in month_days_map.items():
            if month in months:
                return n_days
    return month_days_map


def validate_custom_date(year, month, day):
    print("Date entered: {}-{}-{}".format(year,
                                          format_date_str(month),
                                          format_date_str(day)))
    n_days = get_month_days_map(year, month=month)
    if month not in range(1, 13):
        print("Invalid month entered!")
        return False
    if day not in range(1, n_days + 1):
        print("Invalid day entered!")
        return False
    if year > dt.now().year and month > dt.now().month and day > dt.now().day:
        print("Wrong date entered! Please enter correct date values.")
        return False
    elif year > dt.now().year:
        print("Invalid year entered!")
        return False
    elif year == dt.now().year:
        if month > dt.now().month:
            print("Invalid month entered!")
            return False
        if day > dt.now().day:
            print("Invalid day entered!")
            return False
    return True


def get_custom_date(year=dt.now().year, month=dt.now().month, day=dt.now().day):
    date_valid = validate_custom_date(year, month, day)
    custom_date = ""
    if date_valid:
        from_day = diff_day = day - 7
        from_mon = month
        from_year = year
        prev_n = get_month_days_map(year, month - 1)
        if diff_day < 0:
            from_day = diff_day % prev_n
            from_mon = month - 1 if month != 1 else 12
            from_year = year - 1 if month == 1 else year
        elif diff_day == 0:
            from_day = prev_n
            from_mon = month - 1 if month != 1 else 12
            from_year = year - 1 if month == 1 else year
        print("from_day = {}".format(from_day))
        custom_date = "{}-{}-{}".format(from_year,
                                        format_date_str(from_mon),
                                        format_date_str(from_day))
    return custom_date


def twitter_auth():
    twitter_credentials_file = os.path.join(os.getcwd(), "Credentials",
                                            "twitter_creds.yaml")
    search_tweets_api = 'search_tweets_30_day_dev'
    return load_credentials(filename=twitter_credentials_file,
                            yaml_key=search_tweets_api,
                            env_overwrite=False)


def get_twitter_api_usage(date):
    """Twitter API Usage Monitor"""
    api_usage_file = os.path.join(os.getcwd(), "API Usage", "twitter.json")
    if os.path.lexists(api_usage_file):
        api_usage = pd.read_json(api_usage_file, orient='records')
        print(api_usage)
        print(list(api_usage.columns))
        if date in api_usage.columns:
            return api_usage, api_usage[date].squeeze()
        else:
            # update_twitter_api_usage(date, 0)
            return api_usage, 0
    return None, None


def update_twitter_api_usage(date, count_add):
    api_usage_file = os.path.join(os.getcwd(), "API Usage", "twitter.json")
    api_usage, count = get_twitter_api_usage(date)
    if not api_usage.empty:
        updated_count = 0
        print(list(api_usage.columns))
        if date in api_usage.columns:
            print("Month present\n")
            api_usage[date] += count_add
            updated_count = api_usage[date].squeeze()
        else:
            print("Month absent\n")
            api_usage[date] = 1
        print("api_usage = {}".format(api_usage[date]))
        api_usage.to_json(api_usage_file,
                          orient='records',
                          date_format='iso')
        sleep(3)
        print("Updating new API usage...")
        _, new_count = get_twitter_api_usage(date)
        print("API usage count updated! Current usage: {}".format(new_count))


def get_news_api_usage(date):
    """NewsAPI Usage Monitor"""
    api_usage_file = os.path.join(os.getcwd(), "API Usage", "news.json")
    if os.path.lexists(api_usage_file):
        api_usage = pd.read_json(api_usage_file, orient="records")
        if date in api_usage.columns:
            return api_usage, api_usage[date].squeeze()
        else:
            # update_news_api_usage(date)
            return api_usage, 1
    return None, None


def update_news_api_usage(date):
    api_usage_file = os.path.join(os.getcwd(), "API Usage", "news.json")
    api_usage, count = get_news_api_usage(date)
    if not api_usage.empty:
        updated_count = 0
        if date in api_usage.columns:
            api_usage[date] += 1
            updated_count = api_usage[date].squeeze()
        else:
            api_usage[date] = 0
        api_usage.to_json(api_usage_file, orient="records",
                          date_format="iso")
        sleep(3)
        _, new_count = get_news_api_usage(date)
        print("Updating new API usage...")
        print("API usage count updated! Current usage: {}".format(new_count))


def news_api_auth():
    """Fetch NewsAPI Credentials"""
    src_ = os.path.join(os.getcwd(), "Credentials", "news_api_key.json")
    with open(src_) as key:
        obj = json.load(key)
        if "api_key" in obj.keys() and obj["api_key"]:
            return NewsApiClient(api_key=obj["api_key"])
        else:
            return None


def get_news(queries, sources, news_api, page_size=100, **kwargs):
    """Fetch News Headlines using the News API"""
    top_headlines_map = dict()
    # saved_files = list()
    news_collection = pd.DataFrame()
    for query in queries:
        headlines = news_api.get_everything(q=query,
                                            sources=sources,
                                            sort_by="relevancy",
                                            page_size=page_size)
        if headlines["totalResults"] > 0:
            # response_text = json.loads(headlines.text)
            all_articles = headlines["articles"]
            collection = list()
            for article in all_articles:
                data_ = dict()
                source = article.pop("source")
                content = article.pop("content")
                source = {"source": source["id"]}
                article.update(source)
                collection.append(article)
            data_df = pd.DataFrame(data=collection)
            if not data_df.empty:
                match_str = "({})".format(query)
                reqd_df = data_df
                # reqd_df = data_df[data_df["description"].str.contains(
                #     match_str)]
                if news_collection.empty:
                    news_collection = reqd_df
                else:
                    news_collection = news_collection.append(reqd_df)
            update_news_api_usage(today())
    return news_collection


def get_user_queries_filter(queries, from_users):
    user_queries_filter = '("{}") from:{} -has:media'
    user_queries_list = list()
    for query in queries:
        for user in from_users:
            filter_ = user_queries_filter.format(query, user)
            user_queries_list.append(filter_)
    return user_queries_list


def get_user_specific_filter(from_users):
    user_specific_filter = 'from:{} -has:media'
    user_specific_list = list()
    # for query in queries:
    for user in from_users:
        filter_ = user_specific_filter.format(user)
        user_specific_list.append(filter_)
    return user_specific_list


def get_tweets(query_set, twitter_args, query_filter=None):
    tweets_list = list()
    params = c.TWITTER_PARAMS
    for query in query_set:
        curr_month = "{}-{}".format(dt.now().year,
                                    format_date_str(dt.now().month))
        _, curr_usage = get_twitter_api_usage(curr_month)
        if curr_usage >= 24999:
            print("Twitter API limit is about to exceed! Returning now ...\n")
            break
        if query_filter:
            q = '("{}") {}'.format(query, query_filter)
        else:
            q = "{}".format(query)
            print("No filter/Filter in query_set: {}".format(q))
        print("Collecting for {}".format(q))
        try:
            rule = gen_rule_payload(q,
                                    results_per_call=params["RESULTS_PER_CALL"])
            tweets = collect_results(rule,
                                     max_results=params["MAX_RESULTS"],
                                     result_stream_args=twitter_args)
            print("number of tweets: {}".format(len(tweets)))
            update_twitter_api_usage(curr_month, len(tweets))
            tweets_list.append(tweets)

        except Exception as e:
            print("Exception occurred while fetching tweets: {}".format(e))
            break
    return tweets_list


def process_tweets(tweets_list):
    """Process the tweets to produce a collection of tweet-text, hashtags,
    links, and the date-time when they were created."""
    tweets_collection = pd.DataFrame()
    for tweets in tweets_list:
        for tweet in tweets:
            structured_tweets = structure_text(tweet.all_text)
            structured_tweets.update(
                {"created_time": tweet.created_at_datetime})
            reqd_df = pd.DataFrame([structured_tweets])

            if tweets_collection.empty:
                tweets_collection = reqd_df
            else:
                tweets_collection = tweets_collection.append(reqd_df)
    return tweets_collection


def save_output(tweets_collection,
                curr_datetime,
                output_location,
                category="General",
                subset="predictions"):
    """To save the output of the tweet-information into the categories,
    General or Italy"""
    if category in ["General", "Italy"]:
        file_name = 'relevant_{}_{}.csv'.format(subset, curr_datetime)
        tweets_collection.to_csv(
            os.path.join(output_location, category, file_name),
            index=False)
    else:
        print("Output Directory is not valid!")


def get_saved_versions(name, location):
    # files = os.listdir(location)
    file_objects = drive.get_folder(folder_name=location,
                                    drive_service=DRIVE_SERVICES["drive"])
    files = [f for f in file_objects["children"] if f["mimeType"] !=
             "application/vnd.google-apps.folder"]
    print(files)
    found_files = list()
    for f in files:
        match = re.search(r"({})-[v]?(\d+)(\.csv)?$".format(name), f["name"])
        if match:
            f_groups = match.groups()
            saved_version = f_groups[1]
            map_ = {"file": f, "version": int(saved_version)}
            found_files.append(map_)
    if found_files:
        versions_ = pd.DataFrame(found_files)
        return versions_
    else:
        return pd.DataFrame()


def save_output_version(data, name, location, version=None, to_database=False):
    filename_format = "{name}-{version}.csv"
    next_version = int(version) if version else 1
    all_versions = get_saved_versions(name, location)
    if not all_versions.empty:
        next_version = 1 + int(all_versions["version"].max())
        if version and version >= next_version:
            next_version = version
    new_name = filename_format.format(name=name, version=next_version)
    data.to_csv(os.path.join(location, new_name), index=False)
    return new_name


def get_polarity(sentence):
    """Get polarity of a piece of text"""
    data = json.dumps({"polarity": sentence})
    model = c.BERT_MODEL
    response = requests.post(model["endpoint_uri"], data=data,
                             headers=model["headers"])
    return response.json() if response.status_code == 200 else ""


def file_valid(file_path):
    if os.path.isfile(file_path):
        if re.search(r".*\.csv$", file_path):
            return file_path
    return ""


def correct_files(file_collection):
    """Correct Headers of files with inconsistent column names"""
    for filename in file_collection:
        if filename:
            df = pd.read_csv(filename, encoding="utf-8")
            df.rename(columns={df.columns[0]: "text"},
                      inplace=True)
            df.to_csv(filename, index=False)


def get_previous_data(file_loc, column_list=None):
    """Collect and combine previously saved and fresh data from Twitter and
    NewsAPI (and also from Firebolt)
    """
    # file_name = ""
    # file_loc = os.path.join(DRIVE_LOCATION, PREV_DATA_DIR, file_name)
    prev_data = pd.read_csv(file_loc)
    if not prev_data.empty:
        return prev_data[column_list] if column_list else prev_data
    return None


def get_fresh_data(tweets, headlines, tweets_output, news_output):
    processed_tweets = None
    news_collection = None
    fresh_data = dict()
    if tweets:
        # get fresh tweets:
        # tweets = get_tweets(queries, query_filter)
        processed_tweets = process_tweets(tweets)
        processed_tweets = processed_tweets.reset_index().drop(
            columns=['index'])
        duplicacy_subset = list(
            set(processed_tweets.columns) - {"created_time"})
        processed_tweets.drop_duplicates(subset=duplicacy_subset, inplace=True)
        processed_tweets.to_csv(
            os.path.join(tweets_output, "tweets_{}.csv".format(today())),
            index=False)
    # get fresh news headlines:
    # headlines = get_news(queries, sources, ...)
    if not headlines.empty:
        news_collection = headlines.reset_index().drop(columns=["index"])
        news_collection.to_csv(
            os.path.join(news_output, "news_{}.csv".format(today())),
            index=False)
    fresh_data["tweets"] = processed_tweets
    fresh_data["headlines"] = news_collection
    return fresh_data


def frame_files(files):
    final_df = pd.DataFrame()
    for f in files:
        if os.path.exists(f) and os.path.isfile(f):
            inter_df = pd.read_csv(f, encoding="UTF-8")
            final_df = final_df.append(inter_df)
    return final_df


def combine_data_offline(tweets_files, news_files, prev_files_map=None):
    # processed_headlines = processed_tweets = prev_data_df = pd.DataFrame()
    # tweets_files = "it-it-tweets" if not tweets_files else tweets_files
    # news_files = "it-it-news" if not news_files else news_files
    prev_data_df = pd.DataFrame()
    if prev_files_map:
        for filename, column in prev_files_map.items():
            file_df = get_previous_data(filename, column)
            print("prev_data = {}".format(file_df.shape))
            if prev_data_df.empty:
                prev_data_df = file_df
            else:
                prev_data_df = prev_data_df.append(file_df)
        print("combined prev_data = {}".format(prev_data_df.shape))
        prev_data_df = prev_data_df.reset_index().drop(columns=["index"])
        print("post-reset prev_data = {}".format(prev_data_df.shape))
        prev_data_df.rename(columns={prev_data_df.columns[0]: "text"},
                            inplace=True)
        prev_data_df.drop_duplicates(inplace=True)
        print("post-removing duplicates prev_data = {}".format(
            prev_data_df.shape))
    print("final prev_data = {}".format(prev_data_df.shape))

    tw_text = nh_text = list()
    saved_tweets = get_saved_versions(tweets_files["type"],
                                      tweets_files["path"])
    print("\nFetching it-it Tweets")
    if not saved_tweets.empty:
        tweets_ = list(saved_tweets["file"])
        processed_tweets = pd.DataFrame()
        for tweet_ in tweets_:
            print(
                "reading {}".format(os.path.join(tweets_files["path"], tweet_)))
            df_ = pd.read_csv(os.path.join(tweets_files["path"], tweet_),
                              encoding="UTF-8")
            print("{}: shape = {}".format(tweet_, df_.shape))
            processed_tweets = processed_tweets.append(df_)
        if not processed_tweets.empty:
            tw_text = list(processed_tweets["text"])
    print("len(tw_text): {}".format(len(tw_text)))

    saved_news = get_saved_versions(news_files["type"], news_files["path"])
    print("\nFetching it-it News Headlines")
    if not saved_news.empty:
        news_ = list(saved_news["file"])
        processed_headlines = pd.DataFrame()
        for headline_ in news_:
            print("reading {}".format(
                os.path.join(news_files["path"], headline_)))
            df_ = pd.read_csv(os.path.join(news_files["path"], headline_),
                              encoding="UTF-8")
            print("{}: shape = {}".format(headline_, df_.shape))
            processed_headlines = processed_headlines.append(df_)
        if not processed_headlines.empty:
            nh_text = list(processed_headlines["url"])
    print("len(nh_text): {}".format(len(nh_text)))
    tw_text.extend(nh_text)
    if not prev_data_df.empty:
        base_ = list(prev_data_df["text"])
        base_.extend(tw_text)
    else:
        base_ = tw_text
    print("len(base_): {}".format(len(base_)))
    final_df = pd.DataFrame(base_, columns=["data"])
    final_df.drop_duplicates(inplace=True)
    return final_df


def combine_data(prev_files, queries, query_filter, news_sources):
    prev_data_df = pd.DataFrame()
    for filename, column in prev_files.items():
        file_df = get_previous_data(filename, column)
        if prev_data_df.empty:
            prev_data_df = file_df
        else:
            prev_data_df = prev_data_df.append(file_df)
    prev_data_df = prev_data_df.reset_index().drop(columns=["index"])
    prev_data_df.rename(columns={prev_data_df.columns[0]: "text"}, inplace=True)
    prev_data_df.drop_duplicates(inplace=True)
    # get fresh data
    tweets = get_tweets(queries, query_filter)
    headlines = get_news(queries, news_sources)
    fresh_data = get_fresh_data(tweets, headlines)
    combined_data = prev_data_df.append(fresh_data["tweets"]["text"]).append(
        fresh_data["headlines"]["url"])
    return combined_data


def get_prev_files(twitter_output, news_output, search_output):
    dir_col_map = dict()
    dir_col_map[twitter_output] = "text"
    dir_col_map[news_output] = "url"
    dir_col_map[search_output] = "link"
    prev_files = dict()

    def extract_csv_files(directory, source):
        # files_ = list()
        if os.path.isdir(directory):
            for part in os.listdir(directory):
                # print(part)
                if os.path.isdir(os.path.join(directory, part)):
                    extract_csv_files(os.path.join(directory, part), source)
                else:
                    if re.search(r".*\.csv$", part) and \
                            part not in prev_files.keys():
                        prev_files[os.path.join(directory, part)] = source
        elif os.path.isfile(directory):
            if re.search(r".*\.csv$", directory) \
                    and directory not in prev_files.save_output_versionkeys():
                prev_files[directory] = source

    for directory_, column_ in dir_col_map.items():
        extract_csv_files(directory_, column_)

    return prev_files
