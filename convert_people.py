import psycopg2, signal, sys, time

import util, config #local modules

def init():
	global conn, cur
	conn = util.db_connect()
	cur = conn.cursor()

def calculate_population(cellid):
	"""Converts population statisicts from a voronoi cell to statistics zones
	based on the share of the area that zones cells cover.
	Args:
		cellid: the cell to calculate population for
	Returns:
		A list of tuples (zoneid, population)
	"""
	global cur, conn
	result = []

	cur.execute("SELECT population FROM cell_est_pop WHERE cell_id = %s", (cellid,))
	cell_pop_row = cur.fetchone()
	if cell_pop_row == None:
		return result # cell does not exist, ignore
	population = cell_pop_row[0]

	cur.execute("SELECT zone_id, share FROM cell_zones WHERE cell_id = %s", (cellid,))
	coverages = cur.fetchall()

	if len(coverages) == 0:
			print("WARNING: Population of " + str(population) + " was lost, because no zones could be found inside cell " + str(cellid) + "!")

	#allocate population to the discovered zones
	share_sum = sum([share for zone_id, share in coverages])
	normalized_shares = [(zone_id, share/share_sum) for zone_id, share in coverages]
	result.extend([(zone_id, share * population) for zone_id, share in normalized_shares])

	return result

def upload_population(args):
	"""Aggregates population and adds the flows to the database, 
	by updateing an exisiting zone or createing a new one if no row exisits.
	Args:
		args: a tuple (zone_id, population)
	"""
	global interval, cur, conn

	zone_id, population = args
	population = sum(population)
	
	data = {"zone_id": zone_id,
			"flow": population}

	cur.execute(open("SQL/01a_Preprocessing/add_pop.sql", 'r').read(), data)
	conn.commit()

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
	mconn = util.db_connect()
	mcur = mconn.cursor()

	print("Creating cell_est_pop view...")
	mcur.execute(open("SQL/01a_Preprocessing/create_cell_est_pop_view.sql", 'r').read())
	mconn.commit()

	print("Creating est_pop table...")
	mcur.execute(open("SQL/01a_Preprocessing/create_est_pop.sql", 'r').read())
	mconn.commit()

	print("Creating cell_zones view...")
	mcur.execute(open("SQL/01a_Preprocessing/create_get_zones_for_cell.sql", 'r').read())
	mconn.commit()

	#fetch different interval values
	print("Converting population statistics...")

	#convert to zones
	mapper = util.MapReduce(calculate_population, upload_population, initializer = init)
	mapper(config.CELLS, pipe = True, out = False, chunksize = 1)

