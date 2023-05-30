import numpy as np
import psycopg2
import matplotlib.pyplot as plt
import fiona

con = psycopg2.connect("dbname=labb03 user=postgres password=password host=192.168.100.233 port=5432")
shp = fiona.open("ne_10m_coastline.shp")
i = 0
sql = 'INSERT INTO data.coastline (shape, segment, latitude, longitude) VALUES'
for feature in shp:
	arr = feature['geometry']['coordinates']
	x,y = np.array(arr).T
	
	for j in range (len(x)):
		sql += f'({i}, {j}, {x[j]}, {y[j]}),'
	i = i + 1
sql = sql[:-1] + ';'
cur = con.cursor()
cur.execute(sql)
cur.close()
con.commit()
con.close()
