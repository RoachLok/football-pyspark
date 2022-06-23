from flask import Flask, make_response, request
from flask_caching import Cache
from util.databridge import Databridge
from util.queries import SampleQuery
import schema.data_structs as schema
import data.paths as data_routes

config = {   
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 300,
}

data = Databridge(data_location='local[8]', name='API')
reader = data.get_reader()

dfs_info = [
    (data_routes.trmkt_appearences  , schema.trmkt_appearences  , 'trmkt_appearences'   ),
    (data_routes.trmkt_clubs        , schema.trmkt_clubs        , 'trmkt_clubs'         ),
    (data_routes.trmkt_competitions , schema.trmkt_competitions , 'trmkt_competitions'  ),
    (data_routes.trmkt_games        , schema.trmkt_games        , 'trmkt_games'         ),
    (data_routes.trmkt_leagues      , schema.trmkt_leagues      , 'trmkt_legues'        ),
    (data_routes.trmkt_players      , schema.trmkt_players      , 'trmkt_players'       )
]

data.add_dataframes([(reader.csv(path, header=True, schema=schema), id) for path, schema, id in dfs_info])


#data.add_dataframe(data.join_stored('cards', 'weather', on_join_tag='FECHA'), 'cards_weather')

app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)

@app.route('/')
def index():
    return "Hi", 200



"""
Sample query handling

@app.route('/expenditure')
@cache.cached(timeout=300, query_string=True)
def expenditure():
    query_by = request.args.get('query_by') or 'by_day'
    sector_filter = request.args.get('sector_filter') or None

    csv_str = expense.query(data, modifier=query_by, sector_filter=sector_filter)

    if not csv_str:
        return make_response('{"message": "Bad filter"}', 400, {'Content-type' : 'application/json'})

    return make_response(csv_str, 200, {'Content-Disposition' : 'attachment; filename=export.csv', 'Content-type' : 'text/csv'})
"""


if __name__ == '__main__':
    app.run(host='0.0.0.0')