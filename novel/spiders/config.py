import pymysql

db = pymysql.connect('dev', 'root', 'root', 'novel')
cursor = db.cursor()
sql = """
    select
            * 
    from novel_spider_xpath_config
"""


def get_novel_spider_config_list():
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        novel_spider_config_list = []
        for row in results:
            novel_spider_config = {
                "id": row[0],
                "websiteName": row[1],
                "websiteUrl": row[2],
                "websiteImg": row[3],
                "searchUrl": row[4],
                "searchInfo": row[5],
                "novelType": row[6],
                "novelAuthor": row[7],
                "wordContent": row[8],
                "novelImgUrl": row[9],
                "novelDescription": row[10],
                "novelLatestChapter": row[11],
                "novelLatestChapterUrl": row[12],
                "novelStatus": row[13],
                "novelLastUpdateTime": row[14],
                "novelChaptersUrl": row[15],
                "chapterListInfo": row[16],
                "chapterName": row[17],
                "chapterUrl": row[18],
                "contentInfo": row[19],
                "contentPreciousPage": row[20],
                "contentNextPage": row[21],
                "novelName": row[22],
                "novelUrl": row[23],
                "code": row[24]
            }
            novel_spider_config_list.append(novel_spider_config)
        return novel_spider_config_list
    except Exception:
        return []


if __name__ == '__main__':
    print(get_novel_spider_config_list())
