import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

parser = argparse.ArgumentParser()
parser.add_argument('cache', type=Path)
parser.add_argument('output', type=Path)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
manifest = json.loads((args.cache / 'MANIFEST.json').read_text())
con = duckdb.connect()
con.execute("SET memory_limit='2GB'")
con.execute('SET threads=2')
con.execute("SET TimeZone='UTC'")
result = {'dataset': 'CRUISEResearchGroup/TripWorld',
          'revision': '421bc1dc63068bb398055b1ce987265fe22415db',
          'checked_on': datetime.now(timezone.utc).date().isoformat(), 'manifest_generated_at': manifest['generated_at'],
          'duckdb': duckdb.__version__, 'scope': 'Five complete tables; no review bodies',
          'tables': {}}


def query(sql):
    cursor = con.execute(sql)
    names = [x[0] for x in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


for name, file, key in [
    ('regions', 'region_labels.parquet', ['region_id']),
    ('pois', 'pois.parquet', ['fsq_place_id']),
    ('metadata', 'metadata/metadata_all.parquet', ['fsq_place_id']),
    ('attributes', 'metadata/place_attributes.parquet', ['fsq_place_id']),
    ('travel', 'travel_behaviors.parquet', ['user_id', 'r_h', 'r_o']),
]:
    path = args.cache / file
    entry = next(x for x in manifest['files'] if x['path'] == file)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert digest == entry['sha256'], file
    assert path.stat().st_size == entry['size_bytes'], file
    con.read_parquet(str(path)).create_view(name)
    schema = query(f'DESCRIBE {name}')
    rows = con.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
    assert rows == entry['rows'], file
    fields = [x['column_name'] for x in schema]
    null_sql = ', '.join(f'count(*) FILTER (WHERE "{c}" IS NULL) AS "{c}"' for c in fields)
    empty_sql = ', '.join(f'count(*) FILTER (WHERE trim("{c}") = \'\') AS "{c}"'
                          for c in fields if next(x for x in schema if x['column_name'] == c)['column_type'] == 'VARCHAR')
    key_sql = ', '.join(key)
    result['tables'][name] = {
        'file': file, 'bytes': path.stat().st_size, 'sha256': digest, 'rows': rows,
        'schema': {x['column_name']: x['column_type'] for x in schema},
        'nulls': query(f'SELECT {null_sql} FROM {name}')[0],
        'empty_strings': query(f'SELECT {empty_sql} FROM {name}')[0],
        'duplicate_rows_excess': rows - con.execute(f'SELECT count(*) FROM (SELECT DISTINCT * FROM {name})').fetchone()[0],
        'key': key, 'duplicate_keys_excess': rows - con.execute(f'SELECT count(*) FROM (SELECT DISTINCT {key_sql} FROM {name})').fetchone()[0],
    }
    print(f'Checked {name}: {rows} rows', flush=True)

result['joins'] = query('''
SELECT
 (SELECT count(*) FROM pois p ANTI JOIN metadata m USING (fsq_place_id)) AS pois_without_metadata,
 (SELECT count(*) FROM metadata m ANTI JOIN pois p USING (fsq_place_id)) AS metadata_without_pois,
 (SELECT count(*) FROM attributes a ANTI JOIN pois p USING (fsq_place_id)) AS attributes_without_pois,
 (SELECT count(*) FROM pois p ANTI JOIN regions r ON p.locality=r.region_id) AS pois_unknown_region,
 (SELECT count(*) FROM travel t ANTI JOIN regions r ON t.r_o=r.region_id) AS travel_unknown_destination
''')[0]
result['poi_quality'] = query('''
SELECT count(*) AS rows,
 count(*) FILTER (WHERE fsq_latitude IS NULL OR fsq_longitude IS NULL) AS missing_coordinates,
 count(*) FILTER (WHERE NOT isfinite(fsq_latitude) OR NOT isfinite(fsq_longitude)
   OR abs(fsq_latitude)>90 OR abs(fsq_longitude)>180) AS invalid_coordinates,
 count(*) FILTER (WHERE fsq_latitude=0 AND fsq_longitude=0) AS zero_coordinates,
 count(*) FILTER (WHERE fsq_name IS NULL OR trim(fsq_name)='') AS missing_fsq_name,
 count(*) FILTER (WHERE coalesce(nullif(trim(fsq_name),''), nullif(trim(google_name),'')) IS NULL) AS missing_fsq_and_google_names,
 count(*) FILTER (WHERE coalesce(nullif(trim(fsq_name),''), nullif(trim(google_name),''), nullif(trim(benchmark_name),'')) IS NULL) AS missing_all_name_fields,
 count(*) FILTER (WHERE regexp_full_match(benchmark_name, '[0-9a-f]{24}')) AS benchmark_name_is_hex_id,
 count(*) FILTER (WHERE fsq_date_closed IS NOT NULL AND trim(fsq_date_closed)!='') AS has_closed_date,
 min(fsq_date_refreshed) AS min_refreshed, max(fsq_date_refreshed) AS max_refreshed,
 count(*) FILTER (WHERE try_cast(fsq_date_refreshed AS DATE)>current_date) AS refreshed_after_run_date
FROM metadata
''')[0]
result['google_metadata_flags'] = query('''SELECT
 count(*) FILTER (WHERE has_google_metadata) AS flagged,
 count(*) FILTER (WHERE has_google_metadata != (google_name IS NOT NULL)) AS flag_name_disagreement
 FROM pois''')[0]
result['countries'] = query('''SELECT r.country_name, count(*) AS pois
 FROM pois p LEFT JOIN regions r ON p.locality=r.region_id
 GROUP BY 1 ORDER BY pois DESC, country_name''')
result['cities'] = query('''SELECT r.region_id, r.city_name, r.country_name, count(*) AS pois
 FROM pois p LEFT JOIN regions r ON p.locality=r.region_id
 GROUP BY 1,2,3 ORDER BY pois DESC, region_id''')
result['categories'] = query('''SELECT venue_schema, count(*) AS pois FROM pois
 GROUP BY 1 ORDER BY pois DESC, venue_schema''')
result['russian_cities'] = query('''SELECT r.region_id, r.city_name, count(*) AS pois
 FROM pois p JOIN regions r ON p.locality=r.region_id
 WHERE r.country_name='Russia' GROUP BY 1,2 ORDER BY pois DESC, region_id''')
result['region_label_quality'] = query('''SELECT
 count(*) FILTER (WHERE city_name=region_id) AS city_label_equals_qid,
 count(DISTINCT country_name) AS countries FROM regions''')[0]
result['spb_bbox'] = query('''SELECT count(*) AS pois FROM metadata
 WHERE fsq_latitude BETWEEN 59.7 AND 60.15 AND fsq_longitude BETWEEN 29.5 AND 30.7''')[0]
result['name_collisions'] = query('''WITH names AS (
 SELECT lower(trim(fsq_name)) AS name, count(*) AS n FROM metadata
 WHERE nullif(trim(fsq_name),'') IS NOT NULL GROUP BY 1)
 SELECT count(*) FILTER (WHERE n>1) AS repeated_names,
 coalesce(sum(n) FILTER (WHERE n>1),0) AS rows_with_repeated_names FROM names''')[0]
result['same_name_coordinates'] = query('''SELECT count(*) AS groups, coalesce(sum(n-1),0) AS excess
 FROM (SELECT lower(trim(fsq_name)), fsq_latitude, fsq_longitude, count(*) n
 FROM metadata WHERE nullif(trim(fsq_name),'') IS NOT NULL
 AND fsq_latitude IS NOT NULL AND fsq_longitude IS NOT NULL
 GROUP BY 1,2,3 HAVING count(*)>1)''')[0]
result['travel_summary'] = query('''SELECT count(*) AS records, count(DISTINCT user_id) AS users,
 count(DISTINCT r_h) AS home_regions, count(DISTINCT r_o) AS destination_regions,
 count(*) FILTER (WHERE r_h=r_o) AS home_equals_destination,
 count(*) FILTER (WHERE n_home_ci!=len(c_h) OR n_travel_ci!=len(c_o)) AS length_mismatch,
 min(len(c_o)) AS min_destination_checkins, median(len(c_o)) AS median_destination_checkins,
 max(len(c_o)) AS max_destination_checkins FROM travel''')[0]
result['home_sequence_reuse'] = query('''SELECT count(*) AS reused_user_home_pairs,
 coalesce(sum(n),0) AS records FROM (SELECT user_id,r_h,count(*) n FROM travel
 GROUP BY 1,2 HAVING count(*)>1)''')[0]
result['all_checkin_dates'] = query('''SELECT
 least(min(list_min(list_transform(c_h,x->x.ts))),min(list_min(list_transform(c_o,x->x.ts))))::VARCHAR AS earliest,
 greatest(max(list_max(list_transform(c_h,x->x.ts))),max(list_max(list_transform(c_o,x->x.ts))))::VARCHAR AS latest
 FROM travel''')[0]
print('Expanding destination check-ins', flush=True)
con.execute('CREATE TEMP TABLE events AS SELECT user_id, r_o, unnest(c_o, recursive := true) FROM travel')
result['destination_events'] = query('''SELECT count(*) AS rows, count(DISTINCT trail_id) AS trails,
 count(DISTINCT venue_id) AS pois, min(ts)::VARCHAR AS earliest, max(ts)::VARCHAR AS latest,
 count(*) FILTER (WHERE ts IS NULL) AS missing_ts,
 count(*) FILTER (WHERE venue_id IS NULL) AS missing_venue,
 count(*) FILTER (WHERE venue_schema IS NULL OR trim(venue_schema)='') AS missing_category,
 count(*) FILTER (WHERE trail_id IS NULL) AS missing_trail,
 count(*) - count(DISTINCT (user_id, trail_id, venue_id, ts)) AS duplicate_event_keys_excess
 FROM events''')[0]
result['event_unknown_pois'] = query('''SELECT count(*) AS rows FROM events e
 ANTI JOIN pois p ON e.venue_id=p.fsq_place_id''')[0]
con.execute('''CREATE TEMP TABLE trails AS SELECT user_id, trail_id, r_o,
 count(*) AS n, count(DISTINCT venue_id) AS unique_pois,
 epoch(max(ts)-min(ts))/3600 AS hours FROM events GROUP BY 1,2,3''')
result['trails'] = query('''SELECT count(*) AS trails,
 count(*) FILTER (WHERE n=1) AS single_checkin_trails,
 count(*) FILTER (WHERE unique_pois=1) AS single_poi_trails,
 count(*) FILTER (WHERE hours>24) AS longer_than_24h,
 min(n) AS min_checkins, median(n) AS median_checkins, max(n) AS max_checkins,
 median(hours) AS median_hours, max(hours) AS max_hours FROM trails''')[0]
result['trail_multiple_regions'] = query('''SELECT count(*) AS trails FROM (
 SELECT user_id, trail_id FROM events GROUP BY 1,2 HAVING count(DISTINCT r_o)>1)''')[0]
result['event_time_ties'] = query('''SELECT count(*) AS groups, coalesce(sum(n-1),0) AS excess
 FROM (SELECT user_id, trail_id, ts, count(*) n FROM events GROUP BY 1,2,3 HAVING count(*)>1)''')[0]
result['destinations'] = query('''SELECT r.city_name, r.country_name, count(*) AS records
 FROM travel t JOIN regions r ON t.r_o=r.region_id GROUP BY 1,2
 ORDER BY records DESC, city_name''')
result['attributes_quality'] = query('''SELECT min(fetched_at) AS earliest_fetch, max(fetched_at) AS latest_fetch,
 count(*) FILTER (WHERE match_dist_m>1000) AS matches_over_1km,
 count(*) FILTER (WHERE match_name_sim<0.5) AS name_similarity_under_05,
 count(*) FILTER (WHERE hours_week IS NULL OR trim(hours_week)='') AS missing_week_hours
 FROM attributes''')[0]
result['examples'] = query('''SELECT r.city_name, r.country_name, m.fsq_name, p.google_name,
 m.fsq_latitude, m.fsq_longitude, p.venue_schema, p.n_checkins
 FROM pois p JOIN metadata m USING(fsq_place_id) JOIN regions r ON p.locality=r.region_id
 WHERE nullif(trim(m.fsq_name),'') IS NOT NULL
 QUALIFY row_number() OVER (PARTITION BY r.country_name ORDER BY p.n_checkins DESC, p.fsq_place_id)=1
 ORDER BY p.n_checkins DESC, r.country_name LIMIT 8''')
result['collision_examples'] = query('''SELECT fsq_name, fsq_country, fsq_latitude, fsq_longitude
 FROM metadata WHERE lower(trim(fsq_name)) IN (
 SELECT lower(trim(fsq_name)) FROM metadata WHERE nullif(trim(fsq_name),'') IS NOT NULL
 GROUP BY 1 HAVING count(*)>1 ORDER BY count(*) DESC, 1 LIMIT 1)
 ORDER BY fsq_country, fsq_latitude, fsq_longitude LIMIT 5''')
print('Evaluating user-held-out popularity baseline', flush=True)
result['popularity_baseline'] = query('''WITH popularity AS (
 SELECT r_o,venue_id,count(DISTINCT user_id) AS visitors,count(*) AS visits
 FROM events WHERE user_id%5!=0 GROUP BY 1,2),
 ranked AS (SELECT *,row_number() OVER (
 PARTITION BY r_o ORDER BY visitors DESC,visits DESC,venue_id) AS rank FROM popularity),
 targets AS (SELECT user_id,trail_id,r_o,arg_max(venue_id,ts) AS target
 FROM events WHERE user_id%5=0 GROUP BY 1,2,3 HAVING count(*)>=2)
 SELECT count(*) AS test_segments,
 count(*) FILTER (WHERE ranked.rank=1) AS hits_at_1,
 count(*) FILTER (WHERE ranked.rank<=5) AS hits_at_5,
 count(*) FILTER (WHERE ranked.rank IS NULL) AS target_unseen_in_train,
 count(DISTINCT targets.user_id) AS test_users
 FROM targets LEFT JOIN ranked ON targets.r_o=ranked.r_o AND targets.target=ranked.venue_id''')[0]
result['baseline_proposal'] = {
 'poi_retrieval': 'Name dictionary plus city filter; no NER gold in TripWorld, so no extraction F1.',
 'next_poi': 'Popularity by destination, held-out users; Recall@k. Deduplicate sessions before splitting.',
 'routing': 'Nearest-neighbour order over confirmed coordinates; travel time requires road graph and mode.',
}
(args.output / 'metrics.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n')
print(f'Saved {args.output / "metrics.json"}', flush=True)
