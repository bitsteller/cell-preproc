import psycopg2

import util, config #local modules

util.db_login()
conn = util.db_connect()
cur = conn.cursor()

#create backup copy eant_pos_full that keeps all antennas even when eant_pos is clustered
print("Creating backup table eant_pos_original (takes a while)...")
cur.execute("DROP TABLE IF EXISTS eant_pos_original CASCADE")
conn.commit()
cur.execute("CREATE TABLE eant_pos_original (LIKE eant_pos);")
cur.execute("ALTER TABLE eant_pos_original ADD CONSTRAINT eant_pos_original_pkey PRIMARY KEY (id);")
cur.execute("INSERT INTO eant_pos_original SELECT * FROM eant_pos;")
conn.commit()



#create backup copy homebase_original before clustering
print("Creating backup table ehomebase_orignal (takes a while)...")
cur.execute("DROP TABLE IF EXISTS ehomebase_original CASCADE")
conn.commit()
cur.execute("CREATE TABLE ehomebase_original (LIKE ehomebase);")
cur.execute("ALTER TABLE ehomebase_original ADD CONSTRAINT ehomebase_original_pkey PRIMARY KEY (id);")
cur.execute("INSERT INTO ehomebase_original SELECT * FROM ehomebase;")
conn.commit()