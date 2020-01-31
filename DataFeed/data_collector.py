from pandas import DataFrame
import constants as c
import drive_test as drive
import dataresource as collector
import database_handler as db
import pandas as pd


def reorder_columns(data_frame):
    db_match_dataframe = DataFrame()
    required_columns = db.get_columns(c.SCHEMA, c.TABLE)
    if "id" in required_columns:
        required_columns.remove("id")
    for col in required_columns:
        if col in data_frame.columns:
            db_match_dataframe[col] = data_frame[col]
            continue
        db_match_dataframe[col] = ""
    db_match_dataframe["record_created_date"] = collector.today("datetime")
    return db_match_dataframe


def csv_to_db(file_name):
    output_df = pd.read_csv(file_name)
    db.dataframe_to_table(output_df)


class DataCollector(object):
    def __init__(self):
        self.collected_data = DataFrame()

    def collect_tweets(self, locale):
        if locale not in ["en-it", "it-it"]:
            return None
        query_set = c.QUERIES
        query_filter = c.TWITTER_PARAMS["GENERIC_QUERY_FILTER"]
        locale = locale.strip().lower()
        if locale == "en-it":
            query_filter = c.TWITTER_PARAMS["IT_EN_QUERY_FILTER"]
        elif locale == "it-it":
            query_set = c.QUERIES_IT
            query_filter = c.TWITTER_PARAMS["IT_EN_QUERY_FILTER"]

        tweets = collector.get_tweets(query_set=query_set,
                                      twitter_args=collector.twitter_auth(),
                                      query_filter=query_filter)
        if tweets:
            processed_tweets = collector.process_tweets(tweets)
            if not processed_tweets.empty:
                processed_tweets.reset_index().drop(columns=["index"],
                                                    inplace=True)
                duplicity_subset = list(
                    set(processed_tweets.columns) - {"created_time"})
                processed_tweets.drop_duplicates(subset=duplicity_subset,
                                                 inplace=True)
                processed_tweets.rename(columns=c.TWITTER_COLS_MAP,
                                        inplace=True)
                processed_tweets = reorder_columns(processed_tweets)
                processed_tweets["source_product"] = "Twitter API"
                self.collected_data = self.collected_data.append(
                    processed_tweets, ignore_index=True)

    def collect_news(self, locale):
        if locale not in ["en-it", "it-it"]:
            return None
        news_sources = c.NEWS_SOURCES
        locale = locale.strip().lower()
        query_set = c.QUERIES
        if locale == "it-it":
            query_set = c.QUERIES_IT
        news_collection = collector.get_news(queries=query_set,
                                             sources=news_sources,
                                             news_api=collector.news_api_auth())
        if not news_collection.empty:
            news_collection.reset_index().drop(columns=["index"], inplace=True)
            news_collection.rename(columns=c.NEWS_COLS_MAP, inplace=True)
            news_collection = reorder_columns(news_collection)
            news_collection["source_product"] = "News API"
            self.collected_data = self.collected_data.append(news_collection,
                                                             ignore_index=True)


if __name__ == "__main__":
    collection_object = DataCollector()
    try:
        for loc in ["en-it", "it-it"]:
            collection_object.collect_tweets(locale=loc)
            collection_object.collect_news(locale=loc)
        final_df = collection_object.collected_data.copy()
        final_df.drop_duplicates(subset=["text_data"], inplace=True)
        if not final_df.empty:
            final_df["source_date"] = pd.to_datetime(final_df["source_date"])
            print("\nSerializing the data frame into the database")
            csv_file_name = "Outputs_{}.csv".format(collector.today())
            final_df.to_csv(csv_file_name, index=False)
            db.dataframe_to_table(final_df)
    except Exception as e:
        print("\nERROR: Encountered exception in: {}".format(e))
