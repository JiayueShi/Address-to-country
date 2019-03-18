class DBOperations:
    def __init__(self, username, password, dbname, host):
        self.username = username
        self.password = password
        self.dbname = dbname
        self.host = host
        self.conn = None
        self.cur = None

    def dict_to_json(self, data):
        for i in data:
            if isinstance(data[i].tolist()[0], dict):
                data[i] = data[i].map(lambda x: json.dumps(x))
        return data

    def geojson2postgis(self, filepath, table_name, geo_type):
        """
        Create table from geojson, and this function will auto generate id and geometry index
        :param filepath: the geojson path
        :param table_name: target table
        :param geo_type: the geometry type in geojson, such as 'Point'
        :return: None
        """
        map_data = gpd.GeoDataFrame.from_file(filepath)
        # Maybe you want to change link address
        link = "postgresql://{0}:{1}@{3}:5432/{2}".format(self.username, self.password, self.dbname, self.host)
        engine = create_engine(link, encoding='utf-8')
        map_data = self.dict_to_json(map_data)
        map_data['geometry'] = map_data['geometry'].apply(lambda x: WKTElement(x.wkt, 4326))
        # Maybe you want to change 'replace' to 'append' in the future
        map_data.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            dtype={'geometry': Geometry(geometry_type=geo_type, srid=4326)}
        )

    def create_conn_and_cursor(self):
        self.conn = psycopg2.connect("dbname=" + self.dbname + " user=" + self.username + " host=" +
                                     self.host + " password=" + self.password)
        self.cur = self.conn.cursor()

if __name__ == "__main__":
    # An example
    username = 'postgres'
    geo_type = "Point"

    my_db = DBOperations(username, password, dbname, host)
    my_db.geojson2postgis(filepath, table_name, geo_type)

    my_db.create_conn_and_cursor()

    table1, table2 = "t1", "t2"
    # los angles utm zone is 11S, so we need transform 4326 to 32711; Since we need get 'meter' distance
    # example sql
    sql1 = '''select t1_id, dis
            from
            (select {0}.index as t1_id, 
            min(st_distance(ST_Transform({1}.geometry, 32711), ST_Transform({0}.geometry, 32711))) dis
            from {1}, {0} group by {0}.index) as res
            where dis < 5;'''.format(table1, table2)

    try:
        my_db.cur.execute(sql1)
    except:
        print("I can't SELECT from ????")

    rows = my_db.cur.fetchall()
    print("Rows:")
    for row in rows:
        print("   ", row[1])

    my_db.cur.close()
    my_db.conn.close()
