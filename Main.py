import feedparser
import sqlite3
from sqlite3 import Error

#('https://www.nasa.gov/rss/dyn/breaking_news.rss', 'dc_identifier', 'NASA', 'published_parsed', 'Breaking News', 'title', 'link')
#('https://www.cbc.ca/cmlink/rss-topstories', 'id', 'CBC', 'published_parsed', 'Top Stories', 'title', 'link')
#('https://globalnews.ca/world/feed/', 'guid', 'GlobalNews', 'published_parsed', 'Global', 'title', 'link')
#('http://feeds.bbci.co.uk/news/rss.xml', 'guid', 'BBC', 'published_parsed', 'World', 'title', 'link')
#('https://www.ctvnews.ca/rss/ctvnews-ca-top-stories-public-rss-1.822009', 'id', 'CTV', 'published_parsed', 'Top Stories', 'title', 'link')



DB_FILE = "data.sqlite"


class Feeds:

    def __init__(self, link, id_name, news_site, pub_date, type_name, title, links):
        self.link = link
        self.id_name = id_name
        self.news_site = news_site
        self.pub_date = pub_date
        self.type_name = type_name
        self.title = title
        self.links = links

    def save_feed(self):
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.execute("INSERT INTO feeds ("
                         "link, "
                         "id_name, "
                         "news_site, "
                         "pub_date, "
                         "type_name, "
                         "title,"
                         "links)"
                         "VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(
                self.link,
                self.id_name,
                self.news_site,
                self.pub_date,
                self.type_name,
                self.title,
                self.links))
            conn.commit()
        except Error as e:
            print(e)
        finally:
            conn.close()


def main():
    create_connection()
    feeds = get_feeds()
    parse(feeds)


def create_connection():
    try:
        conn = sqlite3.connect(DB_FILE)
        stories_table = "CREATE TABLE IF NOT EXISTS stories (num INTEGER PRIMARY KEY AUTOINCREMENT, " \
                        "id_name text NOT NULL UNIQUE," \
                        "news_site text NOT NULL, " \
                        "pub_date text NOT NULL, " \
                        "type text, " \
                        "title text NOT NULL, " \
                        "link text NOT NULL" \
                        ");"""
        feeds_table = "CREATE TABLE IF NOT EXISTS feeds (num INTEGER PRIMARY KEY AUTOINCREMENT, " \
                      "link text NOT NULL UNIQUE, " \
                      "id_name text, " \
                      "news_site text, " \
                      "pub_date text, " \
                      "type_name text," \
                      "title text, " \
                      "links text" \
                      ");"""
        conn.execute(stories_table)
        conn.execute(feeds_table)
        conn.commit()
    except Error as e:
        print(e)
        exit()


def get_feeds():
    feeds = []
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT link, id_name, news_site, pub_date, type_name, title, links FROM feeds")
        rows = cursor.fetchall()
        for row in rows:
            feeds.append(row)
    except Error as e:
        print(e)
    finally:
        conn.close()
    return feeds


def parse(feeds):
    for f in feeds:
        feed = feedparser.parse(f[0])
        try:
            conn = sqlite3.connect(DB_FILE)  # TODO:Get list of feeds from DB
            for i in feed["items"]:
                try:
                    conn.execute("INSERT or IGNORE INTO stories ("
                                 "id_name, "
                                 "news_site, "
                                 "pub_date, "
                                 "type, "
                                 "title, "
                                 "link)"
                                 "VALUES (?,?,?,?,?,?)", (
                                     f[2] + i[f[1]],
                                     f[2],
                                     str(i[f[3]]),
                                     f[4],
                                     i[f[5]],
                                     i[f[6]]))
                    conn.commit()
                except Error as e:
                    print(e)
        except Error as e:
            print(e)
        finally:
            conn.close()


def test_feed(feed):
    feed = feedparser.parse(feed)
    for i in feed["items"]:
        print(i.keys())


def generate_feed_list():
    feeds = get_feeds()
    for f in feeds:
        print(f)


if __name__ == '__main__':
    generate_feed_list()
