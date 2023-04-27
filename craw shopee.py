import selenium.webdriver.support.ui as ui
from selenium import webdriver
import numpy as np
import random
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, JavascriptException,WebDriverException
from time import sleep
import pandas as pd
import threading
from queue import Queue
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys

---------------- CHẠY 1 LUỒNG ----- LẤY CHI TIẾT THÔNG TIN CỦA TỪNG SẢN PHẨM ------------------

info_pro,quantity_like,price,class_name,rating,product_reviews,shop_info = [],[],[],[],[],[],[]

n = 9
def openMultiBrowsers1(n):
    drivers = []
    for i in range(7,n):
        driver = webdriver.Chrome('chromedriver.exe')
        driver.get('https://shopee.vn/Th%E1%BB%9Di-Trang-Tr%E1%BA%BB-Em-cat.11036382?page={}&sortBy=sales'.format(i))
        driver.maximize_window()
        a = ActionChains(driver)
        login = driver.find_element(By.XPATH, '//a[text()="Đăng Nhập"]')
        login.click()
        sleep(10)
        a.send_keys('tungquang2472').send_keys(Keys.TAB).perform()
        a.send_keys('0912957815Qt@').send_keys(Keys.ENTER).perform()
        height = driver.execute_script("return document.body.scrollHeight")
        for i in range(height):
            driver.execute_script('window.scrollBy(0,15)') 
            height = driver.execute_script("return document.body.scrollHeight")
            if i == 400 :
                break
        elems_offers = driver.find_elements(By.CLASS_NAME , "_8UN9uK")
        for offer in elems_offers[0:60]:
            a.key_down(Keys.CONTROL).click(offer).perform()
            driver.switch_to.window(driver.window_handles[1])
            for i in range(height):
                driver.execute_script('window.scrollBy(0,15)') 
                height = driver.execute_script("return document.body.scrollHeight")
                if i == 100 :
                    break
            try : 
                sleep(5)
                info_pro1 = driver.find_element(By.CLASS_NAME, "product-detail.page-product__detail")
                quantity_like1 = driver.find_element(By.CLASS_NAME, "IYjGwk")
                price1 = driver.find_element(By.CLASS_NAME, "pqTWkA")
                class_name1 = driver.find_element(By.CLASS_NAME, "flex.items-center.RnKf-X.page-product__breadcrumb")
                rating1 = driver.find_element(By.CLASS_NAME, "flex.X5u-5c")
                product_reviews1 = driver.find_element(By.CLASS_NAME, "product-rating-overview__filters")
                shop_info1 = driver.find_element(By.CLASS_NAME, "NLeTwo.page-product__shop") 
                info_pro.append(info_pro1.text)
                quantity_like.append(quantity_like1.text)
                price.append(price1.text)
                class_name.append(class_name1.text)
                rating.append(rating1.text)
                product_reviews.append(product_reviews1.text)
                shop_info.append(shop_info1.text)
                driver.close()
                driver.switch_to.window(driver.window_handles[0]) 
            except NoSuchElementException or WebDriverException or JavascriptException :
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                sleep(20)
        driver.close()
        drivers.append(driver)
    return drivers
    
còn page 7,8
          
    #drivers.append(driver) 
    return info_pro,quantity_like,price,class_name,rating,product_reviews,shop_info
openMultiBrowsers1(9)

df_3= pd.DataFrame({'info':info_pro,'price':price,'quantity_like':quantity_like,'rating':rating,'product_reviews':product_reviews,'shop_info':shop_info,'class_name':class_name})
df_3.to_csv('craw_shopee4.csv')

df1 = pd.read_csv('craw_shopee.csv')

----------------CHẠY ĐA LUỒNG ------ LẤY CÁC THÔNG TIN BỀ MẶT CỦA SẢN PHẨM ------------------

def openMultiBrowsers2(n):
    drivers = []
    for i in range(n):
        driver = webdriver.Chrome('chromedriver.exe')
        driver.get('https://shopee.vn/Th%E1%BB%9Di-Trang-N%E1%BB%AF-cat.11035639?page={}&sortBy=sales'.format(i))
        driver.maximize_window()
        height = driver.execute_script("return document.body.scrollHeight")
        for i in range(height):
            driver.execute_script('window.scrollBy(0,15)') # scroll by 20 on each iteration
            height = driver.execute_script("return document.body.scrollHeight")
            if i == 260 :
                break
        drivers.append(driver) 
    return drivers


def loadMultiPages(driver,n):
    driver.maximize_window()

    
def loadMultiBrowsers(drivers_rx,n):
    for driver in drivers_rx:
        t = threading.Thread(target=loadMultiPages ,args=(driver,n))
        t.start()
       
def getdata(driver):
    try:
        info1 = driver.find_elements(By.CLASS_NAME, 'ExZ-DZ')
        title1 = driver.find_elements(By.CLASS_NAME, 'MZeqgw')
        price1 = driver.find_elements(By.CLASS_NAME, 'AQ4KLF')
        qa1_permonth = driver.find_elements(By.CLASS_NAME, 'tysB0L')
        city1 = driver.find_elements(By.CLASS_NAME, 'mrz-bA')
        print('page is ready')
    except:
        print('pls retry')   

    for i in title1 :
        title.append(i.text)
    for i in price1 :
        price.append(i.text)
    for i in qa1_permonth :
        qa_permonth.append(i.text)
    for i in city1 :
        city.append(i.text)
    for i in info1 :
        info.append(i.text)
        sleep(10)
        print('craw done ! close browser', driver)
        return info,title,price,qa_permonth,city

def runinParallel( func, drivers_rx):
    for driver in drivers_rx:
        que = Queue()
        t1 = threading.Thread(target=lambda q, arg1: q.put(func(arg1)), args=(que, driver))
        t1.start()
    try:    
        ouput = que.get()
    except:
        ouput = [] 

    return ouput
    
drivers_r1 = openMultiBrowsers1(n)
drivers_r2 = openMultiBrowsers2(n)
loadMultiBrowsers(drivers_r1, n)
sleep(2)

link = runinParallel(getdata, drivers_r2)

info = link[0]
title = link[1]  
price = link[2]
qa_permonth = link[3]
city = link[4]

df = pd.DataFrame({'title':title,'price':price,'quantity_sell_permonth':qa_permonth,'city_of_sell':city})


    
    
driver = webdriver.Chrome('chromedriver.exe')
driver.get('https://shopee.vn/-M%C3%A3-FATRENDW3-gi%E1%BA%A3m-%C4%91%E1%BA%BFn-30K-%C4%91%C6%A1n-99K-T%E1%BA%A5t-tr%C6%A1n-cao-c%E1%BA%A5p-th%E1%BA%A5p-c%E1%BB%95-ulzzang-i.39923306.3553455649?sp_atk=a8beb17c-283c-44e2-8bb4-dbd86da230f6&xptdk=a8beb17c-283c-44e2-8bb4-dbd86da230f6')
sleep(random.randint(5, 10))
driver.maximize_window()
elems_name = driver.find_elements(By.CLASS_NAME,"product-detail.page-product__detail")
name_pro= [elem.text for elem in elems_name]  





driver = webdriver.Chrome('chromedriver.exe')
driver.get('https://shopee.vn/Th%E1%BB%9Di-Trang-N%E1%BB%AF-cat.11035639?page={}&sortBy=sales')
sleep(random.randint(5, 10))
driver.maximize_window()

height = driver.execute_script("return document.body.scrollHeight")
for i in range(height):
   driver.execute_script('window.scrollBy(0,20)') # scroll by 20 on each iteration
   height = driver.execute_script("return document.body.scrollHeight")
   if i == 260 :
       break
    
elems_offers = driver.find_elements(By.CLASS_NAME , "_8UN9uK")
a = ActionChains(driver)
quantity_like,class_name,rating,product_reviews,shop_info = [],[],[],[],[]
for offer in elems_offers[14:60]:
    a.key_down(Keys.CONTROL).click(offer).perform()
    driver.switch_to.window(driver.window_handles[1])
    sleep(10)
    quantity_like1 = driver.find_element(By.CLASS_NAME, "IYjGwk")
    class_name1 = driver.find_element(By.CLASS_NAME, "flex.items-center.RnKf-X.page-product__breadcrumb")
    rating1 = driver.find_element(By.CLASS_NAME, "product-rating-overview__score-wrapper")
    product_reviews1 = driver.find_element(By.CLASS_NAME, "product-rating-overview__filters")
    shop_info1 = driver.find_element(By.CLASS_NAME, "NLeTwo.page-product__shop") 
    quantity_like.append(quantity_like1.text)
    class_name.append(class_name1.text)
    rating.append(rating1.text)
    product_reviews.append(product_reviews1.text)
    shop_info.append(shop_info1.text)
    driver.close()
    driver.switch_to.window(driver.window_handles[0])
    



    
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    