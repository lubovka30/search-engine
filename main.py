from Crawler import *

urlList = ["https://history.eco/arheologi-nashli-massivnoe-liczo/", "https://habr.com/ru/articles/544828/"]

if __name__ == '__main__':
    main = Crawler("aaa")
    main.initDB()

    main.crawl(urlList, 3)
    # main.statistica([0])
    print("Паук все отработал. Программа завершена")
