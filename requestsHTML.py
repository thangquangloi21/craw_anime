import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime, timedelta

def insertReviewMangaIntoTable(idReview, noi_dung, link_manga, link_avatar_user_comment, link_user, time_conment):
    try:
        sqliteConnection = sqlite3.connect("manga-anime.db")
        cursor = sqliteConnection.cursor()
        # Check if idReview already exists in the table
        cursor.execute("SELECT idReview FROM Reviews_Manga WHERE idReview = ?", (idReview,))
        existing_id = cursor.fetchone()
        if existing_id:
            print("Record with idReview already exists. Skipping insertion.")
        else:
            sqlite_insert_with_param = """INSERT INTO Reviews_Manga
                                        (idReview, noi_dung, link_manga, link_avatar_user_comment, link_user, time) 
                                        VALUES (?, ?, ?, ?, ?, ?);"""
            data_tuple = (idReview, noi_dung, link_manga, link_avatar_user_comment, link_user, time_conment)
            cursor.execute(sqlite_insert_with_param, data_tuple)
            sqliteConnection.commit()
            print("Inserted successfully data into table: " + idReview)
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to insert Python variable into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")


def insertReviewAnimeIntoTable(idReview, noi_dung, link_anime, link_avatar_user_comment, link_user, time_conment):
	try:
		sqliteConnection = sqlite3.connect("manga-anime.db")
		cursor = sqliteConnection.cursor()
		sqlite_insert_with_param = """INSERT INTO Reviews_Anime
									(idReview, noi_dung, link_anime, link_avatar_user_comment, link_user, time) 
									VALUES (?, ?, ?, ?, ?, ?);"""
		data_tuple = (idReview, noi_dung, link_anime, link_avatar_user_comment, link_user, time_conment)
		cursor.execute(sqlite_insert_with_param, data_tuple)
		sqliteConnection.commit()
		print("Inserted successfully data into table: " + idReview)
		cursor.close()
	except sqlite3.Error as error:
		print("Failed to insert Python variable into sqlite table", error)
	finally:
		if sqliteConnection:
			sqliteConnection.close()
			print("The SQLite connection is closed")

def insertAnimeMangaNewsIntoTable(idNews, time, profile_user_post, title_news, images_poster, descript_pro):
    try:
        sqliteConnection = sqlite3.connect("manga-anime.db")
        cursor = sqliteConnection.cursor()
        sqlite_insert_with_param = """INSERT OR IGNORE INTO Anime_Manga_News
                                    (idNews, time, profile_user_post, title_news, images_poster, descript_pro) 
                                    VALUES (?, ?, ?, ?, ?, ?);"""
        data_tuple = (idNews, time, profile_user_post, title_news, images_poster, descript_pro)
        cursor.execute(sqlite_insert_with_param, data_tuple)
        sqliteConnection.commit()
        print("Inserted successfully data into table: " + idNews)
        cursor.close()
    except sqlite3.Error as error:
        print("Failed to insert Python variable into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")


def insertAllAnimeTable(animeID, name_anime, score, descript_pro, ranked, popularity, poster, genres):
	try:
		sqliteConnection = sqlite3.connect("manga-anime.db")
		cursor = sqliteConnection.cursor()
		sqlite_insert_with_param = """INSERT OR IGNORE INTO All_Anime
		                            (animeID, name_anime, score, descript_pro, ranked, popularity, poster, genres) 
		                            VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""

		data_tuple = (animeID, name_anime, score, descript_pro, ranked, popularity, poster, genres)
		cursor.execute(sqlite_insert_with_param, data_tuple)
		sqliteConnection.commit()
		print("Inserted successfully data into table: " + animeID)
		cursor.close()
	except sqlite3.Error as error:
		print("Failed to insert Python variable into sqlite table", error)
	finally:
		if sqliteConnection:
			sqliteConnection.close()
			print("The SQLite connection is closed")

def insertAllMangaTable(mangaID, name_manga, score, descript_pro, ranked, popularity, poster, genres):
	try:
		sqliteConnection = sqlite3.connect("manga-anime.db")
		cursor = sqliteConnection.cursor()
		sqlite_insert_with_param = """INSERT OR IGNORE INTO All_Manga
									(mangaID, name_manga, score, descript_pro, ranked, popularity, poster, genres) 
									VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""
		data_tuple = (mangaID, name_manga, score, descript_pro, ranked, popularity, poster, genres)
		cursor.execute(sqlite_insert_with_param, data_tuple)
		sqliteConnection.commit()
		print("Inserted successfully data into table: " + mangaID)
		cursor.close()
	except sqlite3.Error as error:
		print("Failed to insert Python variable into sqlite table", error)
	finally:
		if sqliteConnection:
			sqliteConnection.close()
			print("The SQLite connection is closed")

def convert_time(time_string):
	current_time = datetime.now()
	if "minutes ago" in time_string:
		minutes = int(time_string.split(" minutes ago")[0])
		converted_time = current_time - timedelta(minutes=minutes)
	elif "hours ago" in time_string:
		hours = int(time_string.split(" hours ago")[0])
		converted_time = current_time - timedelta(hours=hours)
	elif "Yesterday" in time_string:
		time_string = time_string.replace("Yesterday, ", "")
		time_format = "%I:%M %p"
		time_obj = datetime.strptime(time_string, time_format)

		one_day = timedelta(days=1)
		yesterday = current_time - one_day

		converted_time = time_obj.replace(year=yesterday.year, month=yesterday.month, day=yesterday.day)
	else:
		converted_time = datetime.strptime(time_string, "%b %d, %I:%M %p")

	return converted_time.strftime("%b %d, %I:%M %p")

def startInsertReviewManga(START_PAGE, END_PAGE):
	for pageIndex in range(START_PAGE, END_PAGE + 1):
		linkListManga = f"https://myanimelist.net/reviews.php?t=manga&filter_check=&filter_hide=&preliminary=on&spoiler=off&p={pageIndex}"
		requestListManga = requests.get(linkListManga, headers={"User-Agent": "Mozilla/5.0"})
		soupListManga = BeautifulSoup(requestListManga.text, "html.parser")
		reviews = soupListManga.find_all("div", {"class": "review-element js-review-element"})

		for review in reviews:
			idReview = review.find("div", {"class": "open"}).find("a")["href"]
			noi_dung = review.find("div", {"class": "text"}).get_text(strip=True)
			link_manga = review.find("div", {"class": "titleblock mb4"}).find("a", {"class": "title"}).get("href")
			link_avatar_user_comment = review.find("div", {"class": "thumb"}).find("a", {"class": "ga-click"}).find("img")["src"]
			link_user = review.find("div", {"class": "thumb"}).find("a")["href"]
			time_conment = review.find("div", {"class": "update_at"}).text

			insertReviewMangaIntoTable(idReview, noi_dung, link_manga, link_avatar_user_comment, link_user, time_conment)

		print(f"Reviews manga page {pageIndex} inserted into the database successfully.")

def startInsertReviewAnime(START_PAGE, END_PAGE):
	for pageIndex in range(START_PAGE, END_PAGE + 1):
		linkListAnime = f"https://myanimelist.net/reviews.php?t=anime&filter_check=&filter_hide=&preliminary=on&spoiler=off&p={pageIndex}"
		requestListAnime = requests.get(linkListAnime, headers={"User-Agent": "Mozilla/5.0"})
		soupListAnime = BeautifulSoup(requestListAnime.text, "html.parser")
		reviews = soupListAnime.find_all("div", {"class": "review-element js-review-element"})

		for review in reviews:
			idReview = review.find("div", {"class": "open"}).find("a")["href"]
			noi_dung = review.find("div", {"class": "text"}).get_text(strip=True)
			link_anime = review.find("div", {"class": "titleblock mb4"}).find("a", {"class": "title"})["href"]
			link_avatar_user_comment = review.find("div", {"class": "thumb"}).find("a", {"class": "ga-click"}).find("img")["src"]
			link_user = review.find("div", {"class": "thumb"}).find("a")["href"]
			time_conment = review.find("div", {"class": "update_at"}).text

			insertReviewAnimeIntoTable(idReview, noi_dung, link_anime, link_avatar_user_comment, link_user, time_conment)
			#print(link_anime)
		print(f"Reviews anime page {pageIndex} inserted into the database successfully.")

def startInsertAnimeMangaNews(START_PAGE, END_PAGE):
	for pageIndex in range(START_PAGE, END_PAGE + 1):
		linkList = f"https://myanimelist.net/news?p={pageIndex}"
		requestList = requests.get(linkList, headers={"User-Agent": "Mozilla/5.0"})
		soupList = BeautifulSoup(requestList.text, "html.parser")
		news = soupList.find_all("div", {"class": "news-unit clearfix rect"})

		for new in news:
			idNews = new.find("p", {"class": "title"}).find("a").get("href")
			time = (new.find("div", {"class": "information"}).find("p", {"class": "info di-ib"}).text).split(" by")[0].strip()
			time = convert_time(time)
			profile_user_post = new.find("div", {"class": "information"}).find("p", {"class": "info di-ib"}).find("a")["href"]
			title_news = new.find("p", {"class": "title"}).find("a").text
			images_poster = new.find("a", {"class": "image-link"}).find("img")["src"]
			descript_pro = new.find("div", {"class": "text"}).get_text(strip=True)
			insertAnimeMangaNewsIntoTable(idNews, time, profile_user_post, title_news, images_poster, descript_pro)
			# data = []
			# data = [idNews, time, profile_user_post, title_news, images_poster, descript_pro]
			# print(time)
		print(f"news anime page {pageIndex} inserted into the database successfully.")

def requestWeb(link, block, name):
	request = requests.get(link, headers={"User-Agent": "Mozilla/5.0"})
	soupLink = BeautifulSoup(request.text, "html.parser")
	result = soupLink.find(block, {"id": name})
	return result

def startInsertAllAnime(START_TOP, END_TOP):
	for pageIndex in range(START_TOP, END_TOP + 1):
		linkListAnime = f"https://myanimelist.net/topanime.php?limit={pageIndex-1}"
		requestListAnime = requests.get(linkListAnime, headers={"User-Agent": "Mozilla/5.0"})
		soupListAnime = BeautifulSoup(requestListAnime.text, "html.parser")
		animes = soupListAnime.find_all("table", {"class": "top-ranking-table"})

		for anime in animes:
			animeID = anime.find("a", {"class" : "hoverinfo_trigger"})["href"]
			name_anime = anime.find("h3", {"class" : "hoverinfo_trigger"}).text.strip()
			ani = requestWeb(animeID, "div", "contentWrapper")
			score = ani.find("div", {"class": "score-label score-9"}).text
			descript_pro = ani.find("p", {"itemprop" : "description"}).text
			ranked = ani.find("span", {"class" : "numbers ranked"}).text
			ranked = ''.join(filter(str.isdigit, ranked))
			popularity = ani.find("span", {"class" : "numbers popularity"}).text
			popularity  = ''.join(filter(str.isdigit, popularity))
			poster = ani.find("img")["data-src"]

			genress = ani.find_all('div', {"class" : "spaceit_pad"})
			genres_list = []
			for div in genress:
				span = div.find('span', {"class" : "dark_text"})
				if span and span.text.strip() == 'Genres:':
					genres_list += [a.text.strip() for a in div.find_all('a')]
			genres = ', '.join(genres_list)

			insertAllAnimeTable(animeID, name_anime, score, descript_pro, ranked, popularity, poster, genres)

def startInsertAllManga(START_TOP, END_TOP):
	for pageIndex in range(START_TOP, END_TOP + 1):
		linkListManga = f"https://myanimelist.net/topmanga.php?limit={pageIndex-1}"
		requestListManga = requests.get(linkListManga, headers={"User-Agent": "Mozilla/5.0"})
		soupListManga = BeautifulSoup(requestListManga.text, "html.parser")
		mangas = soupListManga.find_all("table", {"class": "top-ranking-table"})

		for manga in mangas:
			mangaID = manga.find("a", {"class" : "hoverinfo_trigger fs14 fw-b"})["href"]
			name_manga = manga.find("h3", {"class" : "manga_h3"}).text.strip()
			mangaa = requestWeb(mangaID, "div", "contentWrapper")
			score = mangaa.find("div", {"class": "score-label score-9"}).text
			descript_pro = mangaa.find("span", {"itemprop" : "description"}).text
			ranked = mangaa.find("span", {"class" : "numbers ranked"}).text
			ranked = ''.join(filter(str.isdigit, ranked))
			popularity = mangaa.find("span", {"class" : "numbers popularity"}).text
			popularity  = ''.join(filter(str.isdigit, popularity))
			poster = mangaa.find("img")["data-src"]

			genress = mangaa.find_all('div', {"class" : "spaceit_pad"})
			genres_list = []
			for div in genress:
				span = div.find('span', {"class" : "dark_text"})
				if span and span.text.strip() == 'Genres:':
					genres_list += [a.text.strip() for a in div.find_all('a')]
			genres = ', '.join(genres_list)

			insertAllMangaTable(mangaID, name_manga, score, descript_pro, ranked, popularity, poster, genres)



def start():
	startInsertReviewManga(1, 2) # Tính theo trang
	startInsertReviewAnime(1, 2) # Tính theo trang
	startInsertAnimeMangaNews(1, 2) # Tính theo trang
	startInsertAllAnime(1, 2) # Anime từ top 1 đến 2 ...
	startInsertAllManga(1, 2) # Manga từ top 1 đến 2 ...

start()
