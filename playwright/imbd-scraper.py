from playwright.sync_api import sync_playwright, TimeoutError
import re
import pandas as pd

SAVE_FREQ = 100

with sync_playwright() as pw:
    # create browser instance
    browser = pw.chromium.launch(headless=False)
    # create context
    context = browser.new_context(viewport={"width": 1920, "height": 1080})
    # create browser tab
    page = context.new_page()

    page.goto('https://www.imdb.com/search/title/?groups=top_1000&count=100&sort=user_rating,desc')
    
    print('Loading full movie list...\n')
    # load complete movie list
    while True:
        try:
            page.wait_for_timeout(500)
            page.get_by_role("button", name="100 more").click()
        except TimeoutError:
            break

    # extract links to all movies in the list
    movie_links = page.locator('a.ipc-title-link-wrapper').all()

    # make list of urls for all movies
    movie_urls = []
    for link in movie_links:
        movie_urls.append(f"https://www.imdb.com{link.get_attribute('href')}")

    page.close()

    movie_page = context.new_page()

    movies_list = []
    counter = 1
    prev_save_point = 0
   
    for url in movie_urls[500:]:
        # go to each movie page, move around to ensure everything loads
        movie_page.goto(url)
        movie_page.wait_for_timeout(1000)

        movie_page.mouse.wheel(0, -2000)

        # hover over element to make sure it has loaded
        movie_page.get_by_test_id('photos-title').hover()

        movie_page.get_by_test_id("storyline-header").first.hover()
        movie_page.wait_for_timeout(500)

        movie_page.get_by_role("link", name="Plot summary").first.hover()
        movie_page.wait_for_timeout(500)
        movie_page.get_by_role("link", name="Learn more about contributing").first.hover()
        movie_page.wait_for_timeout(500)

        # GET DATA ---------------------------------------------------------------------------------

        title = movie_page.locator('span.hero__primary-text').all()[0].inner_text()

        print(f'{title} ({counter} of 1000 - {round(counter/1000*100, 2)}%)')

        subheading = movie_page.locator('ul.joVhBE').locator('li').all()

        year = subheading[0].locator('a').inner_text()

        # deal with movies that has no certification information
        try:
            certificate = subheading[1].locator('a').inner_text()
            runtime_h_m = subheading[2].inner_text()
        except:
            certificate = "None"
            runtime_h_m = subheading[1].inner_text()

        rating_block = movie_page.get_by_test_id("hero-rating-bar__aggregate-rating")

        imbd_rating = rating_block.get_by_text(".").first.inner_text()

        try:
            n_reviews = rating_block.all_inner_texts()[-1].split('/10')[1]
        except IndexError:
            # they have a cutesie joke where Spinal Tap's rating is out of 11...
            n_reviews = rating_block.all_inner_texts()[-1].split('/11')[1]

        director = (movie_page.get_by_test_id("title-pc-principal-credit").filter(has_text='Director')
                    .get_by_role('link').first.inner_text())

        genre = movie_page.get_by_test_id('storyline-genres').get_by_role('link').all()[0].inner_text()

        country = movie_page.get_by_test_id('title-details-origin').get_by_role('link').all()[0].inner_text()

        # get box office info if available
        box_office_budget = movie_page.get_by_test_id('title-boxoffice-budget')

        if box_office_budget.count() > 0:
            budget = box_office_budget.inner_text()
        else:
            budget = 0

        box_office_gross = movie_page.get_by_test_id('title-boxoffice-cumulativeworldwidegross')

        if box_office_gross.count() > 0:
            gross = box_office_gross.inner_text()
        else:
            gross = 0

        # get award info if available
        awards_box = movie_page.get_by_test_id('awards')

        if awards_box.count() > 0:
            awards = movie_page.get_by_test_id('award_information').inner_text()
        else:
            awards = ''

        # DATA CLEANING, TYPE CASTING --------------------------------------------------------------

        title = title.strip()
        year = int(year.strip())
        certificate = certificate.strip()

        # calculate runtime in minutes
        hours = re.findall('(\d+)h', runtime_h_m)
        minutes = re.findall('(\d+)m', runtime_h_m)

        # some movies are less than an hour or is exactly x hours long...
        if len(hours) == 1:
            hours = hours[0]
        else:
            hours = 0
        if len(minutes) == 1:
            minutes = minutes[0]
        else:
            minutes = 0
        runtime = int(minutes) + 60*int(hours)

        imbd_rating = float(imbd_rating.strip())

        n_reviews = n_reviews.strip()

        # convert number of reviews to millions
        if n_reviews[-1] == 'M':
            n_reviews_mil = float(n_reviews[:-1])
        elif n_reviews[-1] == 'K':
            n_reviews_mil = float(n_reviews[:-1])/1000
        elif n_reviews[-1].isnumeric():
            n_reviews_mil = float(n_reviews)/1000000

        if budget != 0:
            budget = re.findall(r'\d+', budget)
            budget = int(''.join(budget))

        if gross != 0:
            gross = re.findall(r'\d+', gross)
            gross = int(''.join(gross))

        awards_won = re.findall(r"(\d+)\swin", awards)
        if len(awards_won) == 1:
            awards_won = int(awards_won[0])
        elif len(awards_won) == 0:
            awards_won = 0
        else:
            raise Exception("Problem with awards won for ", title)

        awards_nominated = re.findall(r"(\d+)\snominations", awards)
        if len(awards_nominated) == 1:
            awards_nominated = int(awards_nominated[0])
        elif len(awards_nominated) == 0:
            awards_nominated = 0
        else:
            raise Exception("Problem with awards nominated for ", title)

        movie_data = {'title' : title,
                      'year' : year,
                      'age_restriction' : certificate,
                      'runtime_min' : runtime,
                      'imbd_rating' : imbd_rating,
                      'n_reviews_mil' : n_reviews_mil,
                      'director' : director,
                      'genre' : genre,
                      'country' : country,
                      'budget' : budget,
                      'gross' : gross,
                      'awards_won' : awards_won,
                      'awards_nominated' : awards_nominated}

        counter += 1
        movies_list.append(movie_data)
        if len(movies_list) == SAVE_FREQ:
            new_save_point = prev_save_point + len(movies_list)
            file_name = f'imbd-top-movies-{prev_save_point+1}-{new_save_point}.csv'

            pd.DataFrame(movies_list).to_csv(file_name, index=False)
            movies_list = []
            prev_save_point = new_save_point

    movie_page.close()


# aggregate all files into one
df_list = []
start = 1
for i in range(1,11):
    stop = start+99
    df_list.append(pd.read_csv(f'imbd-top-movies-{start}-{stop}.csv'))
    start = stop +1

movies = pd.concat(df_list, axis=0).reset_index(drop=True)
print(movies)
movies.to_csv('imbd-top-movies.csv', index=False)