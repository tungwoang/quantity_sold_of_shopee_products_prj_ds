import pandas as pd
import numpy as np
import os
import re
df_1 = pd.read_csv('craw_shopee.csv',index_col=[0])
df_2 = pd.read_csv('2craw_shopee.csv',index_col=[0])
df_3 = pd.read_csv('3craw_shopee.csv',index_col=[0])
df_4 = pd.read_csv('4craw_shopee.csv',index_col=[0])

df = pd.concat([df_1,df_2,df_3,df_4],ignore_index=True,sort=False)
    
# drop duplicate

df= df.drop_duplicates()
df= df.drop_duplicates(subset=['price','quantity_like','rating'])

# info
df['info'] = df['info'].apply(lambda x : x.split('MÔ TẢ SẢN PHẨM')[0])

df['Made_in']=df['info'].apply(lambda x : 'China' if 'trung quốc' in x.lower() else 'VietNam')

def send_from(x):
    if 'hà nội' in x.lower():
        return 'Hà Nội'
    elif 'hồ chí minh' in x.lower():
        return 'HCM'
    elif 'nước ngoài' in x.lower():
        return 'Nước ngoài'
    else : return 'Nơi khác'

df['Send_from']=df['info'].apply(lambda x : send_from(x))


# prỉce

df['price']=df['price'].apply(lambda x : x.replace('₫','').replace('.',''))
df['price1'] = df['price'].apply(lambda x : int(x.split('-')[0]) if '-' in x.lower() else int(x))
df['price2'] = df['price'].apply(lambda x : int(x.split('-')[1]) if '-' in x.lower() else 0)
df['price']=df.apply(lambda x : (x['price1']+x['price2'])/2,axis=1)
df.drop(['price1','price2'],axis=1,inplace=True)

# quantity_like

df['quantity_like']=df['quantity_like'].apply(lambda x : x.split('(')[1].replace(')','') if 'thích' in x.lower() else x )
df['k']=df['quantity_like'].apply(lambda x : 1 if 'k' in x.lower() else 0)

df['quantity_like']=df['quantity_like'].apply(lambda x : float(x.replace('k','').replace(',','.').strip()))
df['quantity_like']=df.apply(lambda x : x['quantity_like']*1000 if x['k']==1 else x['quantity_like'],axis=1)
df.drop('k',axis=1,inplace=True)

#rating 
df['rating']=df['rating'].apply(lambda x : x.replace('\n',' '))
df['rating1']=df['rating'].apply(lambda x : re.sub(r'[^\dktr., ]+','',x).strip())

df['rating_star']=df['rating1'].apply(lambda x : float(x.split(' ')[0]) if ' ' in x else 0)
df['evaluate'] = df['rating1'].apply(lambda x : x.split(' ')[1] if ' ' in x else 0 )
df['quantity_sold'] = df['rating1'].apply(lambda x : x.split('  ')[1] if ' ' in x else x )

col = df.columns[11:14]
for i in col:
    df[i]=df[i].astype('str')
         
def change_num(x,y,z):
    df[z]=df[x].apply(lambda x : 1 if 'tr' in x.lower() else 0)
    df[y]=df[x].apply(lambda x : 1 if 'k' in x.lower() else 0)
    df[x]=df[x].apply(lambda x : float(x.replace(',','.').replace('tr','').replace('k','')))
    df[x]=df.apply(lambda i : i[x]*1000 if i[y]==1 else i[x],axis=1)  
    df[x]=df.apply(lambda i : int(i[x]*1000000) if i[z]==1 else int(i[x]),axis=1) 
    df.drop([y,z],axis=1,inplace=True)
    
    
change_num('quantity_sold', 'k', 'tr')
change_num('evaluate', 'k2', 'tr2')

df.drop(df[df['rating_star']==0].index,inplace=True)
# product_reivews

df['product_reviews2']=df['product_reviews'].apply(lambda x : re.sub(r'[a-jA-JấảậóìẢướ]+|[l-zL-Z]+','',x))

df['rating5star']=df['product_reviews2'].apply(lambda x : x.split('  ')[1].replace('(','').replace(')4',''))
df['rating4star']=df['product_reviews2'].apply(lambda x : x.split('  ')[2].replace('(','').replace(')3',''))
df['rating3star']=df['product_reviews2'].apply(lambda x : x.split('  ')[3].replace('(','').replace(')2',''))
df['rating2star']=df['product_reviews2'].apply(lambda x : x.split('  ')[4].replace('(','').replace(')1',''))
df['rating1star']=df['product_reviews2'].apply(lambda x : x.split('  ')[5].replace('(','').replace(')',''))
df['quantity_comment']=df['product_reviews2'].apply(lambda x : x.split('   ')[1].replace('(','').replace(')',''))
df['quantity_pic_video']=df['product_reviews2'].apply(lambda x : x.split('  ')[8].replace('(','').replace(')',''))

col_rate = df.columns[14:21]
for i in col_rate:
    change_num(i, 'k', 'tr')

    
# shop_info

def type_store(x):
    if 'yêu thích+' in x.lower():
        return 'Favorite_store'
    elif 'yêu thích' in x.lower():
        return 'Standard_store'
    else : return 'Mall_store'
    
df['Type_store']=df['shop_info'].apply(lambda x : type_store(x))


df['shop_info']=df['shop_info'].apply(lambda x: x.strip())
df['shop_info']=df['shop_info'].apply(lambda x: x.replace('\n',''))

df_mall = df.loc[df['Type_store']=='Mall_store']

def ex1_size(x):
    p=re.compile(r"(á\d{1,},\d{1,}k)|(á\d{1,}k)|(á\d{1,},\d{1,}tr)|(á\d{1,})")
    p.search(x)[0]
    return p.search(x)[0]

def ex2_size(x):
    p=re.compile(r"(ồi\d{1,}%)")
    p.search(x)[0]
    return p.search(x)[0]

def ex3_size(x):
    p=re.compile(r"(Gia\d{1,}.\w)")
    p.search(x)[0]
    return p.search(x)[0]

def ex4_size(x):
    p=re.compile(r"(ẩm\d{1,})")
    p.search(x)[0]
    return p.search(x)[0]

def ex5_size(x):
    p=re.compile(r"(õi\d{1,},\d{1,}k)|(õi\d{1,}k)|(õi\d{1,},\d{1,}tr)|(õi\d{1,})")
    p.search(x)[0]
    return p.search(x)[0]

df['shop_response_rate']=df['shop_info'].apply(lambda x : int(ex2_size(x).replace('ồi','').replace('%','')))
df['shop_evaluates']=df['shop_info'].apply(lambda x : ex1_size(x).replace('á',''))
df['shop_follows']=df['shop_info'].apply(lambda x : ex5_size(x).replace('õi',''))
df['shop_join_time']=df['shop_info'].apply(lambda x : ex3_size(x).replace('Gia','').strip())
df['shop_product_number']=df['shop_info'].apply(lambda x : int(ex4_size(x).replace('ẩm','')))

col_rate2 = df.columns[23:25]
for i in col_rate:
    df[i]=df[i].astype('str')
for i in col_rate2:
    change_num(i, 'k', 'tr')

def change_time(x,y):
    df[x]=df[x].astype('str')
    df[y]=df[x].apply(lambda x : 1 if 'n' in x.lower() else 0)
    df[x]=df[x].apply(lambda x : int(x.replace('n','').replace('t','')))
    df[x]=df.apply(lambda i : i[x]*12 if i[y]==1 else i[x],axis=1)
    df.drop([y],axis=1,inplace=True)
change_time('shop_join_time', 'year')
    
for i in col_rate:
    df[i]=df[i].astype('int')
    
df['mall']=df.apply(lambda x : 1 if x['Type_store']=='Mall_store' and x['shop_response_rate']>75 and x['shop_product_number']>100 and x['rating5star']>1000 else 0 ,axis=1)

def mall_dis (x,y,z,m):
    df[z]=df[x].apply(lambda x : 1 if 'mall_store' in x.lower() else 0)
    df[m]=df.apply(lambda i : 'Mall_store' if i[z]==1 and i[y]==1 else 'Normal_store',axis=1)
    df[x]=df.apply(lambda i : 'Mall_store' if i[m]=='Mall_store' else i[x],axis=1)
    df[x]=df.apply(lambda i : 'Normal_store' if i[m]=='Normal_store' and i[z]==1 else i[x],axis=1)
    df.drop([y,z,m],axis=1,inplace=True)
    
mall_dis('Type_store','mall','mall_store','type_store2')
    
#class name

df['class_name']=df['class_name'].apply(lambda x : x.replace('\n','').strip())

def ex6_size(x):
    p=re.compile(r"(Thời.\w{1,}.\w{1,}ữ)|(Thời.\w{1,}.Trẻ.Em)|(Thời.\w{1,}.Nam)|(Đồ\w{1,}.Hồ.Trẻ.Em)|(Thời.\w{1,}.Thể.\w{1,}...Dã.Ngoại)")
    p.search(x)[0]
    return p.search(x)[0]

df['Type_product']=df['class_name'].apply(lambda x : ex6_size(x))

def loc_type_items(x):
    if 'đồ lót' in x.lower():
        return 'Đồ lót'
    if 'áo' in x.lower():
        return 'Áo các loại'
    if 'quần' in x.lower():
        return 'Quần các loại'
    if 'tất' in x.lower() or 'vớ' in x.lower():
        return 'Tất vớ'
    if 'vải len' in x.lower() or 'vải' in x.lower():
        return 'Vải len'
    if 'khác' in x.lower():
        return 'Sản phẩm khác'
    if 'đồ ngủ' in x.lower():
        return 'Đồ ngủ'
    if 'đầm' in x.lower() or 'váy' in x.lower():
        return 'Dầm/Váy'
    if 'bộđồ' in x.lower() or 'đồ bộ' in x.lower() or 'bộ quần áo' in x.lower():
        return 'Bộ Đồ'
    if 'đồ bầu' in x.lower():
        return 'Đồ Bầu'
    if 'thắt lưng' in x.lower():
        return 'Thắt Lưng'
    if 'trang sức' in x.lower():
        return 'Trang Sức'
    if 'phụ kiện' in x.lower():
        return 'Phụ kiện'
    if 'giày dép' in x.lower():
        return 'Giày dép'
    else : return 'Sản phẩm khác'
    

df['Type_items']=df['class_name'].apply(lambda x : loc_type_items(x))

pd.set_option('display.max_rows',None,'display.max_columns',None)
df_cls=df['class_name'].loc[df['Type_items']=='']

df.drop(['rating1','product_reviews2'],axis=1,inplace=True)


df.to_csv('clean&fe_shopee_data.csv')




    














