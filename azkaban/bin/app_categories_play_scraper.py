import play_scraper
import pandas as pd

# takes a list of top installed apps as csv, gets associated category from google play store
# outputs a csv with app and category
def main():

    app_count = pd.read_csv('apps.csv')

    app_list = app_count['app'].tolist()

    app_dict = {}

    for app in app_list:

        try:
            app_details = play_scraper.details(app)

            category = app_details['category']

            app_dict[app] = category

        except:
            pass

        finally:
            if len(app_dict) % 500 == 0:
                progress = len(app_dict)
                print(progress)

    app_category_df = pd.DataFrame.from_dict(app_dict, orient='index')
    app_category_df.to_csv('app_categories.csv', header=False)


if __name__ == '__main__':
    main()





