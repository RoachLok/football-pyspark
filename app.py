from flask import Flask, make_response, request, jsonify
from flask_caching import Cache

from util.databridge import Databridge
from transform.penalty_cards_agg import penalty_cards_agg

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


app = Flask(__name__)
app.config.from_mapping(config)
cache = Cache(app)


@app.route('/')
def index():
    return make_response('{"response": "PONG!"}', 200, {'Content-type' : 'application/json'})

@app.route('/api/penalty_cards')
@cache.cached(timeout=300, query_string=True)
def cards():
    order_by = request.args.get('order_by') or 'yellow_cards'
    player_positon = request.args.get('position') or None
    format_csv = request.args.get('json') == 'false'

    if not player_positon:
        return '{"response": "Bad Request"}', 400, {'Content-type' : 'application/json'}


    response_str = penalty_cards_agg(
        player_app_df   = data.join_stored('trmkt_appearences', 'trmkt_players', 'player_id'),
        lasts_keys      = ['position', 'sub_position'],
        sums_keys       = ['yellow_cards', 'red_cards'],
        pl_position     = player_positon,
        order_by        = order_by,
        csv = format_csv
    )


    response_meta = {'Content-Disposition' : 'attachment; filename=export.csv', 'Content-type' : 'text/csv'}

    return make_response(response_str, 200, response_meta) if format_csv \
        else response_str, 200, {'Content-type' : 'application/json'}



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