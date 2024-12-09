from bs4 import BeautifulSoup
import pandas as pd
from app.configs.logging_file import logging
from urllib.parse import urlparse
import os

class DataPreprocessing:

    def __init__(self):
        pass

    def save_csv_file(self,df=None):
        df.to_csv('app/preprocessed_data/preprocessed_data.csv', index=False)
    def get_domain(self,url=None):
        try:
            if url:
                parsed_url = urlparse(url)  # Parse the URL
                return parsed_url.netloc  # Extract the domain (e.g., 'example.com')
            return None
        except Exception as e:
            logging.error(e,exc_info=True)
            return None

    def check_cached_pages_existance(self,domain=None):
        try:
            list_of_caches_files = os.listdir(
                os.path.join("app", "datasets", "webcredibility-1", "webcredibility", "cached_pages"))
            if domain in list_of_caches_files:
                return True
            else:
                return False
        except Exception as e:
            logging.error(e,exc_info=True)
            return False

    def html_files_walk(self,path=None):
        try:

            html_files = []
            for dirpath, _, filenames in os.walk(path):
                for file in filenames:
                    if file.endswith(".html"):  # Check if file has .html extension
                        full_path = os.path.join(dirpath, file)
                        html_files.append(full_path)
            return html_files
        except Exception as e:
            logging.error(e,exc_info=True)
            return  []

    def get_html_files_path(self,dir_path=None):
        try:

            root_path = os.path.join("app", "datasets", "webcredibility-1", "webcredibility", "cached_pages")
            to_files_path = os.path.join(root_path, dir_path)

            if os.path.exists(to_files_path):
                list_of_path = self.html_files_walk(path=to_files_path)
            else:
                list_of_path = []

            return list_of_path
        except Exception as e:
            logging.error(e,exc_info=True)
            return []
    def extract_word_counts_from_html(self,file_paths=None):
        try:

            features = {}
            word_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                soup = BeautifulSoup(content, 'lxml')
                word_count = len(soup.get_text().split())
                word_counter = word_count + word_counter
            return word_counter
        except Exception as e:
            logging.info(f"Error Found at __extract_word_counts_from_html__ : {file_paths} : {e}", exc_info=True)
            return 0

    def extract_avg_sentence_length_from_html(self,file_paths=None):
        try:
            avg_sentence_length_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')
                # Extract content features

                avg_sentence_length = sum(len(sentence.split()) for sentence in soup.get_text().split('.')) / max(
                    len(soup.get_text().split('.')), 1)

                avg_sentence_length_counter = avg_sentence_length_counter + avg_sentence_length
            return avg_sentence_length_counter
        except Exception as e:
            logging.error(e,exc_info=True)
            logging.info(f"Error Found at __extract_word_counts_from_html__ : {file_paths}")
            return  0

    def calculate_keyword_density(self,text, keyword='credibility'):
        words = text.lower().split()
        return words.count(keyword.lower()) / max(len(words), 1)

    # Helper function to check if a link is external
    def is_external_link(self,url=None):
        parsed = urlparse(url)
        return parsed.netloc != ''

    # Dummy function for broken link detection (extend as needed)
    def is_broken_link(self,url=None):
        return False  # Implement link validation if necessary
    def extract_keyword_density_from_html(self, file_paths=None):
        try:

            features = {}
            keyword_density_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')

                keyword_density = self.calculate_keyword_density(soup.get_text())

                keyword_density_counter = keyword_density_counter + keyword_density
            return keyword_density_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at extract_keyword_density_from_html : {file_paths}", exc_info=True)
            return 0

    def extract_outbound_links_from_html(self, file_paths=None):
        try:

            outbound_links_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')

                keyword_density = self.calculate_keyword_density(soup.get_text())
                outbound_links = len([a['href']
                                      for a in soup.find_all('a', href=True)
                                      if self.is_external_link(a['href'])])

                outbound_links_counter = outbound_links_counter + outbound_links
            return outbound_links_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at extract_keyword_density_from_html : {file_paths}", exc_info=True)
            return 0

    def extract_authoritative_links_from_html(self, file_paths=None):
        try:
            authoritative_links_counter = 0
            for file_path in file_paths:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')
                authoritative_links = sum(1
                                          for a in soup.find_all('a', href=True)
                                          if '.gov' in a['href'] or '.edu' in a['href'])


                authoritative_links_counter = authoritative_links_counter + authoritative_links
            return authoritative_links_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at extract_authoritative_links_from_html : {file_paths}", exc_info=True)
            return 0

    def extract_broken_links_from_html(self, file_paths=None):
        try:
            broken_links_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')
                broken_links = len([a['href']
                                    for a in soup.find_all('a', href=True)
                                    if self.is_broken_link(a['href'])])
                broken_links_counter = broken_links_counter + broken_links
            return broken_links_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at extract_broken_links_from_html : {file_paths}", exc_info=True)
            return 0

    def extract_https_usage_from_html(self, file_paths=None):
        try:
            https_usage_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')
                https_usage = 1 if soup.base and 'https' in soup.base.get('href', '') else 0
                https_usage_counter = https_usage_counter + https_usage
            return https_usage_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.error(f"Error Found at extract_https_usage_from_html : {file_paths}")
            return 0

    def extract_number_of_ads_from_html(self, file_paths=None):
        try:

            features = {}
            number_of_ads_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')
                number_of_ads = len(soup.find_all('script'))
                number_of_ads_counter = number_of_ads_counter + number_of_ads
            return number_of_ads_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at __extract_number_of_ads_from_html__ : {file_paths}")
            return 0

    def extract_mobile_responsive_from_html(self, file_paths=None):
        try:
            mobile_responsive_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')

                keyword_density = self.calculate_keyword_density(soup.get_text())
                mobile_responsive = 1 if soup.find('meta', {'name': 'viewport'}) else 0

                mobile_responsive_counter = mobile_responsive_counter + mobile_responsive
            return mobile_responsive_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at __extract_mobile_responsive_from_html__ : {file_paths}")
            return 0

    def extract_page_size_from_html(self, file_paths=None):
        try:

            features = {}
            page_size_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')

                page_size = os.path.getsize(file_path)  # File size in bytes

                page_size_counter = page_size_counter + page_size
            return page_size_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at extract_keyword_density_from_html : {file_paths}", exc_info=True)
            return 0

    def extract_num_images_from_html(self, file_paths=None):
        try:
            num_images_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')
                num_images = len(soup.find_all('img'))

                num_images_counter = num_images_counter + num_images
            return num_images_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at __extract_num_images_from_html__ : {file_paths}")
            return 0

    def extract_num_videos_from_html(self, file_paths=None):
        try:
            num_videos_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')
                num_videos = len(soup.find_all('video'))
                num_videos_counter = num_videos_counter + num_videos
            return num_videos_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at __extract_num_videos_from_html__ : {file_paths}", exc_info=True)
            return 0

    def extract_images_with_alt_from_html(self, file_paths=None):
        try:
            images_with_alt_counter = 0
            for file_path in file_paths:
                # Read the HTML file
                with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
                    content = file.read()
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(content, 'lxml')
                images_with_alt = sum(1 for img in soup.find_all('img') if img.get('alt'))
                images_with_alt_counter = images_with_alt_counter + images_with_alt
            return images_with_alt_counter
        except Exception as e:
            logging.error(e, exc_info=True)
            logging.info(f"Error Found at extract_images_with_alt_from_html : {file_paths}", exc_info=True)
            return 0

    def __call__(self,path_dict=None):
        logging.info("__call__ activated__")
        logging.info(f"path_dict: {path_dict}")
        if os.path.exists(path_dict['web_credibility_1000_url_ratings']):
            web_credibility_1000_url_ratings_df = pd.read_excel(path_dict['web_credibility_1000_url_ratings'])
        else:
            logging.error(f"File not found at given path: {path_dict['web_credibility_1000_url_ratings']}")
            web_credibility_1000_url_ratings_df = None

        if os.path.exists(path_dict['web_credibility_expert_ratings_for_test_set']):
            web_credibility_expert_ratings_for_test_set_df = pd.read_excel(
                path_dict['web_credibility_expert_ratings_for_test_set'])
        else:
            logging.error(f"File not found at given path : {path_dict['web_credibility_expert_ratings_for_test_set']}")
            web_credibility_expert_ratings_for_test_set_df = None

        logging.info("__calling__-01: check_cached_pages_existance")
        web_credibility_1000_url_ratings_df['domain'] = web_credibility_1000_url_ratings_df['URL'].apply(lambda url: self.get_domain(url=url))
        web_credibility_1000_url_ratings_df['check_cached_pages_existance'] = web_credibility_1000_url_ratings_df['domain'].apply(lambda domain:
                                                                                    self.check_cached_pages_existance(domain=domain))

        logging.info("__calling__-02: get_html_files_path")
        web_credibility_1000_url_ratings_df['html_walk_paths'] = web_credibility_1000_url_ratings_df['domain'].apply(
            lambda domain_name: self.get_html_files_path(dir_path=domain_name))


        logging.info("__calling-03: __extract_word_counts_from_html__")
        web_credibility_1000_url_ratings_df['word_counts'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_word_counts_from_html(file_paths=path))

        logging.info("__calling__-04: extract_avg_sentence_length_from_html")
        web_credibility_1000_url_ratings_df['avg_sentence_length'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_avg_sentence_length_from_html(file_paths=path))


        logging.info("__calling__-05: extract_keyword_density_from_html")
        web_credibility_1000_url_ratings_df['keyword_density'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_keyword_density_from_html(file_paths=path))

        logging.info("__calling__-06: extract_authoritative_links_from_html")
        web_credibility_1000_url_ratings_df['authoritative_links'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_authoritative_links_from_html(file_paths=path))

        logging.info("__calling__-07: extract_broken_links_from_html")
        web_credibility_1000_url_ratings_df['broken_links'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_broken_links_from_html(file_paths=path))


        logging.info("__calling__-08: extract_https_usage_from_html")
        web_credibility_1000_url_ratings_df['https_usage'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_https_usage_from_html(file_paths=path))

        logging.info("__calling__-09: extract_number_of_ads_from_html")
        web_credibility_1000_url_ratings_df['number_of_ads'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_number_of_ads_from_html(file_paths=path))

        logging.info("__calling__-10: extract_mobile_responsive_from_html")
        web_credibility_1000_url_ratings_df['mobile_responsiveness'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_mobile_responsive_from_html(file_paths=path))

        logging.info("__calling__-11: extract_num_images_from_html")
        web_credibility_1000_url_ratings_df['page_size'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_page_size_from_html(file_paths=path))

        logging.info("__calling__-12: extract_num_images_from_html")
        web_credibility_1000_url_ratings_df['num_of_images'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_num_images_from_html(file_paths=path))

        logging.info("__calling__-13: extract_num_videos_from_html")
        web_credibility_1000_url_ratings_df['num_videos'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_num_videos_from_html(file_paths=path))

        logging.info("__calling__-14: extract_images_with_alt_from_html")
        web_credibility_1000_url_ratings_df['images_with_alt'] = web_credibility_1000_url_ratings_df[
            'html_walk_paths'].apply(lambda path: self.extract_images_with_alt_from_html(file_paths=path))



        self.save_csv_file(df=web_credibility_1000_url_ratings_df)