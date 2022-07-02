from flask import Flask, make_response, request, jsonify
from flask_caching import Cache

from util.databridge import Databridge
from transform.jobs import penalty_cards_agg, shots_pivot_results

import pandas as pd
import schema.data_structs as schema
import data.paths as data_routes

config = {   
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 300,
}

data = Databridge(data_location='local[8]', name='API')
reader = data.get_reader()

mrkt_val = pd.read_excel(data_routes.akarsh_mrkt_val, sheet_name='Sheet1').fillna('N/A')
data.add_dataframe(mrkt_val, 'akarsh_mrkt_val')

dfs_info = [
    (data_routes.trmkt_appearences  , schema.trmkt_appearences  , 'trmkt_appearences'   ),
    (data_routes.trmkt_clubs        , schema.trmkt_clubs        , 'trmkt_clubs'         ),
    (data_routes.trmkt_competitions , schema.trmkt_competitions , 'trmkt_competitions'  ),
    (data_routes.trmkt_games        , schema.trmkt_games        , 'trmkt_games'         ),
    (data_routes.trmkt_leagues      , schema.trmkt_leagues      , 'trmkt_legues'        ),
    (data_routes.trmkt_players      , schema.trmkt_players      , 'trmkt_players'       ),
    (data_routes.tech_players       , schema.tech_players       , 'tech_players'        ),
    (data_routes.tech_shots         , schema.tech_shots         , 'tech_shots'          )
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
    order_by        = request.args.get('order_by') or 'yellow_cards'
    player_positon  = request.args.get('position')
    format_csv      = request.args.get('json'    ) 


    if not player_positon:
        return '{"response": "Bad Request"}', 400, {'Content-type' : 'application/json'}


    response_str = penalty_cards_agg(
        player_app_df   = data.join_stored('trmkt_appearences', 'trmkt_players', 'player_id'),
        lasts_keys      = ['position', 'sub_position'],
        sums_keys       = ['yellow_cards', 'red_cards'],
        pl_position     = player_positon,
        order_by        = order_by,
        csv = format_csv == 'false' if format_csv else True
    )


    response_meta = {'Content-Disposition' : 'attachment; filename=export.csv', 'Content-type' : 'text/csv'}

    return make_response(response_str, 200, response_meta) if format_csv \
        else response_str, 200, {'Content-type' : 'application/json'}

@app.route('/api/shots')
@cache.cached(timeout=300, query_string=True)
def shots():
    situation       = request.args.get('situation'    )
    order_by        = request.args.get('order_by'     ) or 'Goal'
    player_names    = request.args.get('player_names' )
    only_goalers    = request.args.get('only_goalers' )
    percentage      = request.args.get('percentage'   )
    format_csv      = request.args.get('json'         ) 
    
    player_names    = player_names   == 'true' if player_names  else False
    only_goalers    = only_goalers   == 'true' if only_goalers  else False
    percentage      = percentage     == 'true' if percentage    else False
    
    format_csv = format_csv == 'false' if format_csv else True

    if not situation:
        return '{"response": "Bad Request"}', 400, {'Content-type' : 'application/json'}

    names_df = None 
    if player_names:
        names_df = data.get_dataframe('tech_players')


    response_str = shots_pivot_results(
        shots_df        = data.get_dataframe('tech_shots'),
        situation       = situation,
        players_df      = names_df,
        order_by        = order_by,
        only_goalers    = only_goalers,
        percentage      = percentage,
        csv = format_csv
    )


    response_meta = {'Content-Disposition' : 'attachment; filename=export.csv', 'Content-type' : 'text/csv'}

    return make_response(response_str, 200, response_meta) if format_csv \
        else response_str, 200, {'Content-type' : 'application/json'}


@app.route('/api/get_player/<player_name>')
@cache.cached(timeout=300, query_string=True)
def get_player(player_name):
    vals = data.get_dataframe('akarsh_mrkt_val')

    return make_response(vals[vals.Player.isin([player_name])].to_csv(), 200, {'Content-Disposition' : 'attachment; filename=export.csv', 'Content-type' : 'text/csv'})


if __name__ == '__main__':
    app.run(host='0.0.0.0')