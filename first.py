import pymysql
import pandas

import google.cloud.storage
import os
import googleapiclient.discovery

# 建立连接
class Test_myqsl(object):
    #运行数据库和建立游标对象
    def __init__(self):
        self.connect = pymysql.connect(
    host="rm-bp1c5x1rhig246p88o.mysql.rds.aliyuncs.com",
    port=3306,
    user="adbug",
    password="2YeoyszrQoUhzubO",
    database="adbugnew",
    charset="utf8" )


        # 返回一个cursor对象,也就是游标对象
        self.cursor = self.connect.cursor(cursor=pymysql.cursors.DictCursor)
    #关闭数据库和游标对象
    def __del__(self):
        self.connect.close()
        self.cursor.close()
    def write(self):
        #将数据转化成DataFrame数据格式
        data = pandas.DataFrame(self.read())
        #把id设置成行索引
        data_1 = data.set_index("id",drop=True)
        for index,row in data_1.iterrows():
           data_1.loc[index,'url']= 'https://file.adbug.cn/m/image/' + data_1.loc[index,'url']
           print(index,row['title'],row['url'] )
        #写写入数据数据
        pandas.DataFrame.to_csv(data_1,"d:/mysql.csv",encoding="utf_8_sig")
        print("写入成功")
    def read(self):
        #读取数据库的所有数据
        like1 = "%admaster%"
        number1 = 1
        number2 = 100
        data = self.cursor.execute("select id, addata_2018.fingerprint , title, addata_material.url ,addata_material.md5 from addata_2018 left join addata_material on addata_2018.fingerprint = addata_material.fingerprint where addata_material.type ='image' and  trackers like %s limit %s,%s",[like1,number1,number2] )
        field_2 = self.cursor.fetchall()

        # pprint(field_2)
        return field_2
#封装
def main():
    write = Test_myqsl()
    write.write()

if __name__ == '__main__':
    main()


#conn = pymysql.connect(
 #   host="rm-bp1c5x1rhig246p88o.mysql.rds.aliyuncs.com",
  #  port=3306,
   # user="adbug",
    #password="2YeoyszrQoUhzubO",
   # database="adbugnew",
    #charset="utf8"
#)


# 获取一个光标
#cursor = conn.cursor()
# 定义将要执行的SQL语句
#sql = "select addata_2018.fingerprint , title, addata_material.url ,addata_material.md5 from addata_2018 left join addata_material on addata_2018.fingerprint = addata_material.fingerprint where trackers like %s limit %s,%s"
#like1="%admaster%"
#number1 = 1
#number2 = 100
# 并执行SQL语句
#cursor.execute(sql, [like1,number1,number2])
#data=cursor.fetchall()

# 涉及写操作注意要提交
#conn.commit()
# 关闭连接

# 建立连接
#newdf = pd.DataFrame.from_dict(data,orient='index')
#newdf.to_csv( 'C:/2.tsv',sep='\t')
#save = pd.DataFrame



#conn2 = pymysql.connect(
 #   host="localhost",
  #  port=3306,
   # user="root",
    #password="adbug168",
 #   database="adbug_machinelearning",
  #  charset="utf8"
#)

#cursor2 = conn.cursor()

# 获取最新的那一条数据的ID

#print("最后一条数据的ID是:", sql)

#cursor.close()
#conn.close()