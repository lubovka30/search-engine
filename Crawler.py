from pydoc import doc
from nltk.tokenize import word_tokenize
from urllib.parse import urlparse
import bs4
import requests
from database import *
import re
import urllib.request
import random


class Crawler:

    # 0. Конструктор Инициализация паука с параметрами БД
    def __init__(self, dbFileName):
        self.dbFileName = dbFileName
        self.connection = create_connection(
            self.dbFileName, "ponchik", "1221", "localhost", "5432"
        )

    # 0. Деструктор
    def __del__(self):
        self.connection.close()
        print('Деструктор')

    # 1. Индексирование одной страницы
    def addToIndex(self, soup, url):

        # если страница уже проиндексирована, то ее не индексируем
        if self.isIndexed(url):
            return

        text = self.getTextOnly(soup)
        # Получаем список слов из индексируемой страницы
        words = self.separateWords(text)

        # Получаем идентификатор URL
        urlId: int = self.getEntryId('urlList', 'url', url, True)

        # Связать каждое слово с этим URL
        for i in range(len(words)):
            word: str = words[i]
            # если слово не входит в список игнорируемых слов ignoreWords
            # if ((65 <= ord(word[0]) <= 90) or (97 <= ord(word[0]) <= 122) or (33 <= ord(word[0]) <= 47) or
            #         (58 <= ord(word[0]) <= 62) or (91 <= ord(word[0]) <= 96)):
            if re.fullmatch('[a-zA-Z.,₽&•$><*\'«»`’©β”“?„!+()\/–|‘—=;:@…~#{}\[\]\-]*', word):
                pass
            elif word[0] == "'":
                pass
            else:
                # то добавляем запись в таблицу wordlist
                id_wordlist = self.getEntryId('wordlist', 'word', word, True)
                # добавляем запись в wordlocation
                cursor = self.connection.cursor()
                cursor.execute("""INSERT INTO wordLocation (fk_word_id, fk_URL_id, location) VALUES (
                               (%s), (%s), (%s));""" % (id_wordlist, urlId, i))
                cursor.execute("""INSERT INTO linkWord (fk_word_id, fk_link_id) 
                                VALUES (%s, (SELECT rowid FROM linkBtwURL WHERE fk_FromURL_id = %s LIMIT 1));"""
                               % (id_wordlist, urlId))
                self.connection.commit()

    # 2. Получение текста страницы
    def getTextOnly(self, doc):
        text: str = ""
        all_tag = list(filter(None, [tag.get_text(strip=True, separator='\n') for tag in doc.find_all()]))
        for tag in all_tag:
            text += tag.replace("'", "").replace('"', '').replace('`', '') + "\n"
        # text.replace("'", "").replace('"', '').replace('`', '')
        return text

    # 3. Разбиение текста на слова
    def separateWords(self, text):
        # разделение строки str на отдельные слова с учетом возможных символов-разделителей
        return word_tokenize(text)

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)
    def isIndexed(self, url):
        cursor = self.connection.cursor()
        # Проверяем, есть ли страница в urllist
        cursor.execute("""SELECT EXISTS(SELECT * FROM URLlist WHERE URL = '%s');""" % (url,))
        if cursor.fetchall()[0][0]:
            # Если есть, то проверяем, что страница посещалась и есть слова
            # в таблице wordlocation, тогда возвращаем true
            cursor.execute("""SELECT EXISTS(SELECT * FROM wordLocation JOIN URLList ON 
            URLList.rowid = fk_URL_id WHERE URL = '%s');""" % (url,))
            if cursor.fetchall()[0][0]:
                return True
            #  Иначе возвращаем false
        return False

    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT rowid FROM URLList WHERE URL = '%s';""" % (urlFrom,))
        tmp1 = cursor.fetchall()[0][0]
        cursor.execute("""SELECT rowid FROM URLList WHERE URL = '%s';""" % (urlTo,))
        tmp2 = cursor.fetchall()[0][0]
        cursor.execute("""INSERT INTO linkBtwURL (fk_FromURL_id, fk_ToURL_id) 
                       VALUES ((%s), (%s));""" % (tmp1, tmp2))

    def statistica(self, table):
        cursor = self.connection.cursor()
        print("\n\nСтатистические данные:\n")
        print(f"Данные для построения графика: {table}\n")
        print("20 наиболее частых слов:")
        cursor.execute("""SELECT fk_word_id FROM wordlocation;""")
        wordlist = cursor.fetchall()
        top_word = []
        for word in list(set(wordlist)):
            cursor.execute("""SELECT word FROM wordlist WHERE rowid = '%s';""" % (word[0],))
            top_word += [(cursor.fetchall()[0][0], wordlist.count(word))]
        for i in range(0, 20):
            try:
                print(sorted(top_word, key=lambda word: word[1], reverse=True)[i][0], ' - ',
                      sorted(top_word, key=lambda word: word[1], reverse=True)[i][1])
            except Exception:
                continue

        print('\n20 наиболее частых доменов:')
        cursor.execute("""SELECT url FROM URLList;""")
        domens = []
        for url in cursor.fetchall():
            domens += [urlparse(url[0]).netloc]
        top_domen = []
        for domen in list(set(domens)):
            top_domen += [(domen, domens.count(domen))]
        for i in range(0, 20):
            try:
                print(sorted(top_domen, key=lambda domen: domen[1], reverse=True)[i][0], ' - ',
                      sorted(top_domen, key=lambda domen: domen[1], reverse=True)[i][1])
            except Exception:
                continue
        print('\n')

    # 6. Непосредственно сам метод сбора данных.
    # Начиная с заданного списка страниц, выполняет поиск в ширину
    # до заданной глубины, индексируя все встречающиеся по пути страницы
    def crawl(self, urlList, maxDepth):
        print("6. Обход страниц")

        newPageSet = set()
        table = []

        for currDepth in range(0, maxDepth):
            print("\033[32m{}".format(f"Глубина: {currDepth}"))
            # обход каждого url на текущей глубине
            for url in urlList:

                cursor = self.connection.cursor()
                urlFrom: int = self.getEntryId('urlList', 'url', url, True)
                cursor.execute("""SELECT COUNT(rowid) FROM linkBtwURL WHERE fk_FromURL_id = '%s';""" % (urlFrom,))

                if cursor.fetchall()[0][0]:
                    print("\033[0m{}".format("Страница уже обработана"))
                    cursor.execute("""SELECT fk_ToURL_id FROM linkBtwURL WHERE fk_FromURL_id = '%s';""" % (urlFrom,))

                    for id in cursor.fetchall():
                        cursor.execute(
                            """SELECT URL FROM urlList WHERE rowid = '%s';""" % (id[0],))
                        newPageSet.add(cursor.fetchall()[0][0])
                    continue
                # self.connection.commit()
                try:
                    response = urllib.request.urlopen(url)
                    status_code = response.getcode()
                    if status_code == 200:
                        print("\033[0m{}".format(f"Статус код = {status_code}. Страница {url} успешно открыта"))
                    else:
                        print("\033[33m{}".format(f"Страница вернула код состояния {status_code}"))
                        continue
                except urllib.error.HTTPError as e:
                    print("\033[33m{}".format(f"Ошибка HTTP: {e.code} - {e.reason}"))
                    continue
                except urllib.error.URLError as e:
                    print("\033[33m{}".format(f"Ошибка URL: {e.reason}"))
                    continue
                except Exception:
                    print("\033[33m{}".format("Неизвестная ошибка"))
                    continue
                # получить HTML-код страницы по текущему url
                html_doc = requests.get(url).text
                # использовать парсер для работы тегов
                soup = bs4.BeautifulSoup(html_doc, "html.parser")
                # вызвать функцию класса Crawler для добавления содержимого в индекс
                self.addToIndex(soup, url)

                for link in soup.findAll('a'):
                    if (link.get('href') is None
                            or link.get('href') == ''
                            or 'javascript' in link.get('href')
                            or "'" in link.get('href')
                            or 'facebook' in link.get('href')
                            or 'twitter' in link.get('href')
                            or '.jpg' in link.get('href')
                            or '.png' in link.get('href')
                            or '.jpeg' in link.get('href')
                            or '.gif' in link.get('href')):
                        continue
                    domen = 'https://' + urlparse(url).netloc
                    if link.get('href') == '#':
                        print("\033[0m{}".format(f"Найдена ссылка: {domen}"))
                        self.getEntryId('urlList', 'url', domen, True)
                        self.addLinkRef(url, domen)
                        newPageSet.add(domen)
                    elif link.get('href')[0] == '/':
                        print("\033[0m{}".format(f"Найдена ссылка: {domen + link.get('href')}"))
                        self.getEntryId('urlList', 'url', domen + link.get('href'), True)
                        self.addLinkRef(url, domen + link.get('href'))
                        newPageSet.add(domen + link.get('href'))
                    elif link.get('href')[0] != 'h':
                        print("\033[0m{}".format(f"Найдена ссылка: {domen + '/' + link.get('href')}"))
                        self.getEntryId('urlList', 'url', domen + '/' + link.get('href'), True)
                        self.addLinkRef(url, domen + '/' + link.get('href'))
                        newPageSet.add(domen + '/' + link.get('href'))
                    else:
                        print("\033[0m{}".format(f"Найдена ссылка: {link.get('href')}"))
                        self.getEntryId('urlList', 'url', link.get('href'), True)
                        self.addLinkRef(url, link.get('href'))
                        newPageSet.add(link.get('href'))
                cursor = self.connection.cursor()
                cursor.execute("""SELECT COUNT(rowid) FROM wordlist;""")
                count_wordlist = cursor.fetchall()[0][0]
                cursor.execute("""SELECT COUNT(rowid) FROM linkBtwURL;""")
                count_linkBtwURL = cursor.fetchall()[0][0]

                table += [(currDepth, count_wordlist, count_linkBtwURL)]
            self.connection.commit()
            k: int = round(len(newPageSet) * 0.1)
            if k == 0:
                k += 1
                urlList = list(random.sample(list(newPageSet), k))
                print("\033[0m{}".format(f"Найдено страниц: {len(newPageSet)}. Взято: {k}"))
            urlList = list(random.sample(list(newPageSet), k))
            print("\033[0m{}".format(f"Найдено страниц: {len(newPageSet)}. Взято 10%: {k}"))
            # конец обработки текущ url

        self.statistica(table)
        # конец обработки всех URL на данной глубине

    # 7. Инициализация таблиц в БД
    def initDB(self):
        create_database_query = "CREATE DATABASE " + self.dbFileName
        create_database(self.connection, create_database_query)

        execute_query(self.connection, create_wordList_table)
        execute_query(self.connection, create_URLList_table)
        execute_query(self.connection, create_linkBtwURL_table)
        execute_query(self.connection, create_wordLocation_table)
        execute_query(self.connection, create_linkWord_table)

        return print("Созданы пустые таблицы с необходимой структурой")

    # 8. Вспомогательная функция для получения идентификатора и
    # добавления записи, если такой еще нет
    def getEntryId(self, tableName, fieldName, value, createNew):

        cursor = self.connection.cursor()

        # проверить, есть ли значение value уже в таблице
        cursor.execute("""SELECT count (*) FROM %s WHERE %s = '%s';""" % (tableName, fieldName, value))

        if cursor.fetchall()[0][0] == 0:
            # 	// 2) Если нет, то вставить и вернуть id записи
            cursor.execute("""INSERT INTO %s (%s) VALUES ('%s') RETURNING rowid;""" % (tableName, fieldName, value))
            return cursor.fetchall()[0][0]
        else:
            # 	// 3) Если есть, то вернуть id выбранной записи
            cursor.execute("""SELECT rowid FROM %s WHERE %s = '%s';""" % (tableName, fieldName, value))
            return cursor.fetchall()[0][0]
