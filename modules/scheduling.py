import sys
sys.path.append("C:\Python38\Lib\site-packages")
import schedule
import time

def lala():
    print("dummy")


# schedule.every(10).seconds.do(lala)
schedule.every().thursday.at("20:20").do(lala)

while 1:
    schedule.run_pending()
    time.sleep(1)