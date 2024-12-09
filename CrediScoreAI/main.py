from app.models.data_preprocessing import DataPreprocessing
import os

data_preprocessing = DataPreprocessing()
path_dict = {
    "web_credibility_1000_url_ratings": os.path.join("app", "datasets", "webcredibility-1", "webcredibility",
                                                     "web_credibility_1000_url_ratings.xls"),
    "web_credibility_expert_ratings_for_test_set": os.path.join("app", "datasets", "webcredibility-1", "webcredibility",
                                                                "web_credibility_expert_ratings_for_test_set.xlsx"),

}
data_preprocessing(path_dict=path_dict)