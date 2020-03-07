def movies_etl(wiki, kaggle, rating):
    file_path = "/Users/pegkhiev/Documents/UC_Berkeley/Movies-ETL/data/"
    #First, perform data cleaning steps for Wikipedia data 
    #Capture file not found error (eg. wrong file name)
    try:
        with open(f"{file_path}{wiki}", mode = "r") as file:
            wiki_movies_raw = json.load(file)
    except FileNotFoundError:
        print("Wikipedia file not found")
    wiki_movies = [movie for movie in wiki_movies_raw if ("Directed by" in movie or "Director" in movie) and "imdb_link" in movie and "No. of episodes" not in movie]
    #combine alternate titles
    def clean_movie(movie):
        movie = dict(movie)
        alt_titles = {}
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
            'Hangul','Hebrew','Hepburn','Japanese','Literally',
            'Mandarin','McCune–Reischauer','Original title','Polish',
            'Revised Romanization','Romanized','Russian',
            'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')
        return movie
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    wiki_movies_df["imdb_id"] = wiki_movies_df["imdb_link"].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset = "imdb_id", inplace = True)
    wiki_columns_to_keep = [column for column in wiki_movies_df if (wiki_movies_df[column].isnull().sum()) < (len(wiki_movies_df)*0.9) ]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
    #define function to clean and convert all numerical values
    def parse_dollars(s):
        if type(s) != str:
            return np.nan
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            value = float(s) * 10**6
            return value
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):
            s = re.sub('\$|\s|[a-zA-Z]','', s)
            value = float(s) * 10**9
            return value
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            s = re.sub('\$|,','', s)
            value = float(s)
            return value
        else:
            return np.nan
    
    #parse box office data
    #Catch error if "Box Office" is not available
    try:
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        box_office = wiki_movies_df["Box office"].dropna()
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        wiki_movies_df.drop('Box office', axis=1, inplace=True)
    except KeyError as e:
        print("Box office is not available")
    
    #parse budget data
    #Catch error if "Budget" is not available
    try:
        budget = wiki_movies_df["Budget"].dropna()
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        budget = budget.str.replace(r'\[\d+\]\s*','')
        wiki_movies_df["budget"] = budget.str.extract(f"({form_one}|{form_two})", flags=re.IGNORECASE)[0].apply(parse_dollars)
        wiki_movies_df.drop(columns={"Budget"},inplace=True)
    except KeyError as e:
        print("Budget is not available")
    
    #parse release date data
    #catch error if "Release date" is not available
    try:
        release_date=wiki_movies_df["Release date"].dropna().apply(lambda x: "".join(x) if type(x) == list else x)
        date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
        date_form_two = r'\d{4}.[01]\d.[123]\d'
        date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
        date_form_four = r'\d{4}'
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
        wiki_movies_df.drop("Release date", axis = 1, inplace = True)
    except KeyError as e: 
        print("Release date is not available")
        
    #parse running time data
    #catch error if "Running time" is not available
    try: 
        running_time = wiki_movies_df["Running time"].dropna().apply(lambda x: " ".join(x) if type(x)==list else x)
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
    except KeyError as e: 
        print("Running time is not available")
    
    #read kaggle data
    #catch any file error (e.g. wrong file name)
    try:
        kaggle_metadata = pd.read_csv(f"{file_path}{kaggle}", low_memory=False)
    except FileNotFoundError:
        print("Kaggle file not found")
    #Assumption: remove all adult movies even if the adult movies are high in number
    kaggle_metadata = kaggle_metadata[kaggle_metadata["adult"]== "False"].drop('adult', axis = "columns")
    #Assumption: convert "video" data from wrong data type back to boolean
    kaggle_metadata['video'] == 'True'
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    #Assumption: "budget", "id", "popularity" data are all in object type rather than numeric
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    
    #Read ratings data
    #Catch any file name error 
    try:
        ratings = pd.read_csv(f"{file_path}{rating}")
    except FileNotFoundError:
        print("Ratings file not found")
    #change timestamp data 
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    #Assumption: no glaring errors for ratings 
    
    #Merge Wikipedia and Kaggle Dataframes
    #Assumption: drop wiki data for title, release date, language, production company
    #Assumption: use kaggle data and fill zeros with Wiki for running time, budget, and box office
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
        df.drop(columns=wiki_column, inplace=True)
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    #Assumption: More than one column has just one value 
    #run a loop to drop columns that only have one value 
    for col in movies_df.columns:
        value_counts = movies_df[col].apply(lambda x:  tuple(x) if type(x)==list else x).value_counts(dropna = False)
        num_values = len(value_counts)
        if num_values ==1:
            movies_df.drop(col, axis = 1, inplace = True)
    
    #Reordering and renaming columns 
    #Assumption: all the defined columns are available 
    #Catch error if one of the columns is not available 
    try:
        movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    except KeyError as e: 
        print(e)
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    
    #Consolidate Ratings dataframe by movieId and rating, 
    #pivot it so that rating is the columns, then renaming the columns
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    
    #merge ratings with movies_df dataframe 
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    
    #LOAD: Load "movies_df" DataFrame to SQL (as per assignment, not the movies_with_ratings_df)
    #Append to existing table 
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)
    movies_df.to_sql(name = "movies", con = engine, if_exists = "append")
    
    #LOAD: Load ratings.csv to SQL
    #Break into smaller chunks for import 
    rows_imported = 0
    start_time = time.time()
    for data in pd.read_csv(f"{file_path}{rating}", chunksize=1000000):
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
        rows_imported += len(data)
    print(f"Done. {time.time()-start_time} total seconds elapsed")
  
    
movies_etl(wiki="wikipedia.movies.json", kaggle = "movies_metadata.csv",
          rating = "ratings.csv")   