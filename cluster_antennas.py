import psycopg2, signal, sys

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def connected_components(graph):
	"""Returns the vertices in each connected compontent of a given graph
	Args:
		graph: a graph given as a vertex-adjacency dictonary, keys are vertex ids and the value the list of their respective neighbors
	Return:
		A list of lists containing the vertex ids in each connected component
	"""
	components = []
	visited = set()

	def dfs(node):
		if node in visited:
			return []
		visited.add(node)
		nodes = [node]
		for sibling in graph[node]:
			nodes += dfs(sibling)
		return nodes

	for node in graph:
		if node not in visited:
			components.append(dfs(node))
	return components 

def join_cells(args):
	"""Joins all given antennas to one new antenna placed at the centroid of the original antennas.
	Args:
		args: A tuple (newid, cellids) where newid is the new cellid of 
			  the clustered antenna and cellids a list of antenna ids from eant_pos_original to be clustered
	"""
	global conn, cur
	newid, cellids = args

	#add new cell at centroid of the cluster
	cur.execute("WITH clustered_antennas AS (SELECT ST_Union(eant_pos_original.geom) AS geom FROM eant_pos_original WHERE eant_pos_original.id IN %(cluster)s) \
				 INSERT INTO eant_pos (id, lon, lat, geom) \
				 SELECT %(id)s AS id, \
				 		ST_X(ST_Centroid(clustered_antennas.geom)) AS lon, \
				 		ST_Y(ST_Centroid(clustered_antennas.geom)) AS lat, \
				 		ST_Centroid(clustered_antennas.geom) AS geom \
				 FROM clustered_antennas", {"cluster": tuple(cellids), "id": newid})
	conn.commit()

def update_homebase(userid):
	global conn, cur

	#fetch cellpath
	cur.execute("SELECT antenna_id FROM ehomebase_original WHERE id = %s", (userid,))
	oldantenna = cur.fetchone()[0]

	#update antenna id
	newantenna = newcells[oldantenna]

	#copy old homebase and update
	cur.execute("INSERT INTO ehomebase SELECT * FROM ehomebase_original WHERE id = %s", (userid,))
	data = (newantenna, userid)
	cur.execute("UPDATE ehomebase SET antenna_id = %s WHERE id = %s", data)
	conn.commit()

def fetch_ehomebase_ids():
	global mcur
	mcur.execute("SELECT id FROM ehomebase_original")
	for (ehomebase_id, ) in mcur:
		yield ehomebase_id

def signal_handler(signal, frame):
	global mapper, request_stop
	request_stop = True
	if mapper:
		mapper.stop()
	print("Aborting (can take a minute)...")
	sys.exit(1)

request_stop = False
mapper = None
cur = None
conn = None

if __name__ == '__main__':
	signal.signal(signal.SIGINT, signal_handler) #abort on CTRL-C
	util.db_login()
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Recreating eant_pos table...")
	mcur.execute(open("SQL/01_Loading/create_eant_pos.sql", 'r').read())
	mconn.commit()

	print("Recreating ehomebase table...")
	mcur.execute(open("SQL/01_Loading/create_ehomebase.sql", 'r').read())
	mconn.commit()

	print("Fetching antennas to join from database...")
	sql = '''
	SELECT w.id,
	ARRAY(SELECT id
		FROM eant_pos_original
		WHERE EXISTS(SELECT * FROM pop_statistic zone WHERE ST_WITHIN(w.geom,zone.geom)) AND
			ST_Within(eant_pos_original.geom, (SELECT zone.geom FROM pop_statistic zone WHERE ST_WITHIN(w.geom,zone.geom))) AND 
			id != w.id
		)
	FROM eant_pos_original AS w;
	'''
	mcur.execute(sql)
	graph = {node: edges for node, edges in mcur}

	print("Clustering...")
	components = connected_components(graph)

	newcells = dict()
	for newid, oldscells in enumerate(components):
		for oldid in oldscells:
			newcells[oldid] = newid

	print("Updating antennas...")
	mapper = util.ParMap(join_cells, initializer = init)
	mapper(enumerate(components), length = len(components))

	print("Updating homebase...")
	mapper = util.ParMap(update_homebase, initializer = init)
	mcur.execute("SELECT COUNT(*) FROM ehomebase_original")
	c = mcur.fetchone()[0]

	mapper(fetch_ehomebase_ids(), length = c, chunksize = 1000)
