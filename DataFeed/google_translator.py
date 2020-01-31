from google.cloud import translate_v2 as translate
import os


translate_api_key = os.path.join(os.getcwd(), 'eiu-searchautomation.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = translate_api_key
try:
    translate_client = translate.Client()


    def translate_keyword(keyword, target_lang="en"):
        response = translate_client.translate(keyword,
                                              target_language=target_lang)
        if response:
            return response["translatedText"]
        else:
            return ""


    def detect_language(text_string):
        result = translate_client.detect_language(text_string)
        return result["language"]


except Exception as e:
    print("Exception occurred while instantiating "
          "Google Cloud Translate: {}".format(e))
