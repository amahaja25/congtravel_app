#!/bin/bash

# Ensure the databases directory exists
mkdir -p databases
mkdir -p databases/backup

# Create DB filepaths
DB_FILE="databases/congtravel_master.db"
BACKUP_FILE="databases/backup/congtravel_master.bak"

# Check if the database file exists and create it if it doesn't
if [ -f "$DB_FILE" ]; then
    echo "Database exists. Creating backup, deleting the original, and recreating the database file..."
    mv "$DB_FILE" "$BACKUP_FILE"
    sqlite3 "$DB_FILE" ""
else
    echo "Database does not exist. Creating an empty database file..."
    sqlite3 "$DB_FILE" ""
fi

sqlite-utils insert "$DB_FILE" house_trips datasette/raw_data/csv/house_current.csv --csv
sqlite-utils transform "$DB_FILE" house_trips --pk doc_id
sqlite-utils schema "$DB_FILE" house_trips

sqlite-utils insert "$DB_FILE" trip_sponsors datasette/raw_data/csv/final_sponsors_current.csv --csv
sqlite-utils add-foreign-key "$DB_FILE" trip_sponsors doc_id house_trips doc_id
sqlite-utils schema "$DB_FILE" trip_sponsors

sqlite-utils insert "$DB_FILE" trip_destinations datasette/raw_data/csv/destinations_current.csv --csv
sqlite-utils add-foreign-key "$DB_FILE" trip_destinations doc_id house_trips doc_id
sqlite-utils schema "$DB_FILE" trip_destinations

sqlite-utils extract "$DB_FILE" trip_destinations destination --table destinations
sqlite-utils schema "$DB_FILE" destinations

sqlite-utils extract "$DB_FILE" trip_sponsors sponsor --table sponsors
sqlite-utils schema "$DB_FILE" sponsors

sqlite-utils insert "$DB_FILE" senate_trips datasette/raw_data/csv/clean_propub_senate_current.csv --csv
sqlite-utils transform "$DB_FILE" senate_trips --pk file_name
sqlite-utils schema "$DB_FILE" senate_trips

sqlite-utils insert "$DB_FILE" mecea_trips datasette/raw_data/csv/mecea_trips.csv --csv
sqlite-utils schema "$DB_FILE" mecea_trips

sqlite-utils index-foreign-keys "$DB_FILE"

###
# Create a member table
###
# Issue: by using select distinct, we end up with multiple instances of the same member_id having different values in the other column, because of name difference, district differences, tate differnces
# Instead we group by member id, which will give the first? value of the other columns. But we need test who is being dropped
sqlite-utils create-table "$DB_FILE" member member_name text member_id text state text party text district text
sqlite-utils query "$DB_FILE" "INSERT INTO member (member_name, member_id, state, party, district) SELECT member_name, member_id, state, party, district FROM house_trips GROUP BY member_id"

sqlite-utils query "$DB_FILE" "
    UPDATE member
    SET member_name =
        TRIM(SUBSTR(member_name, INSTR(member_name, ',') + 2) || ' ' ||
             SUBSTR(member_name, 1, INSTR(member_name, ',') - 1))
"
sqlite-utils transform "$DB_FILE" member --pk member_id

###
# Need to create a Senate member table
###


###
# Create the text.json tables
###

# sqlite-utils insert "$DB_FILE" senate_text datasette/raw_data/text_json/senate_text.json
# sqlite-utils schema "$DB_FILE" senate_text

python3 -c "
import json, sys
d = json.load(open('datasette/raw_data/text_json/house_text.json'))
cols = d['columns']
print(json.dumps([dict(zip(cols, row)) for row in d['rows']]))
" | sqlite-utils insert "$DB_FILE" house_text -
sqlite-utils schema "$DB_FILE" house_text

# sqlite-utils insert "$DB_FILE" legistorm_text datasette/raw_data/text_json/legistorm_text.json
# sqlite-utils schema "$DB_FILE" legistorm_text


#sqlite-utils enable-fts "$DB_FILE" house_text text
#sqlite-utils enable-fts "$DB_FILE" senate_text text
#sqlite-utils enable-fts "$DB_FILE" legistorm_text text
# sqlite-utils add-foreign-key "$DB_FILE" house_text doc_id house_trips doc_id
# sqlite-utils add-foreign-key "$DB_FILE" senate_text file_name senate_trips file_name

###
# Create the House Trip page
###

sqlite-utils create-table "$DB_FILE" house_trip_page doc_id text cleaned_filer_names text member_name text member_id text party text state text destinations text sponsors text departure_date text return_date text trip_length integer document_link text
sqlite-utils query "$DB_FILE" "
WITH dedup_dests AS (
    SELECT doc_id,
           '[' || GROUP_CONCAT(
               '{\"destinations_id\": ' || destinations_id || ', \"destination\": \"' || replace(destination, '\"', '\"\"') || '\"}'
           , ', ') || ']' AS destinations
    FROM (
        SELECT DISTINCT td.doc_id, d.id AS destinations_id, d.destination
        FROM trip_destinations td
        JOIN destinations d ON td.destinations_id = d.id
    )
    GROUP BY doc_id
),
dedup_sponsors AS (
    SELECT doc_id,
           '[' || GROUP_CONCAT(
               '{\"sponsors_id\": ' || sponsors_id || ', \"sponsor\": \"' || replace(sponsor, '\"', '\"\"') || '\"}'
           , ', ') || ']' AS sponsors
    FROM (
        SELECT DISTINCT ts.doc_id, s.id AS sponsors_id, s.sponsor
        FROM trip_sponsors ts
        JOIN sponsors s ON ts.sponsors_id = s.id
    )
    GROUP BY doc_id
)
INSERT INTO house_trip_page (
    doc_id, cleaned_filer_names, member_name, member_id, party, state, destinations, sponsors,
    departure_date, return_date, trip_length, document_link
)
SELECT ht.doc_id, ht.cleaned_filer_names, m.member_name, m.member_id, m.party, m.state,
       dd.destinations, ds.sponsors,
       DATE(ht.departure_date), DATE(ht.return_date), ht.trip_length, ht.document_link
FROM house_trips ht
JOIN member m ON ht.member_id = m.member_id
LEFT JOIN dedup_dests dd ON ht.doc_id = dd.doc_id
LEFT JOIN dedup_sponsors ds ON ht.doc_id = ds.doc_id
"


###
# Traveler page
### 
sqlite-utils create-table "$DB_FILE" traveler_info cleaned_filer_names text member_name text member_id text total_trips integer 
sqlite-utils query "$DB_FILE" "
INSERT INTO traveler_info (cleaned_filer_names, member_name, member_id, total_trips)
SELECT 
    ht.cleaned_filer_names,
    TRIM(SUBSTR(m.member_name, INSTR(m.member_name, ',') + 1)) || ' ' || TRIM(SUBSTR(m.member_name, 1, INSTR(m.member_name, ',') - 1)) AS member_name,
    m.member_id,
    COUNT(ht.doc_id) AS total_trips
FROM house_trips ht
JOIN member m ON ht.member_id = m.member_id
GROUP BY ht.cleaned_filer_names, m.member_id
"  


sqlite-utils create-table "$DB_FILE" all_traveler_trips cleaned_filer_names text member_name text member_id text party text state text doc_id text destination text sponsor text departure_date text return_date text trip_length integer sponsors_id text destination_id text 
sqlite-utils query "$DB_FILE" "
INSERT INTO all_traveler_trips (cleaned_filer_names, member_name, member_id, party, state, doc_id, destination, sponsor, departure_date, return_date, trip_length, sponsors_id, destination_id)
WITH sponsor_agg AS (
    SELECT
        ht.doc_id,
        '[' || GROUP_CONCAT(DISTINCT '{\"sponsor\": \"' || REPLACE(s.sponsor, '\"', '\"\"') || '\", \"id\": \"' || s.id || '\"}') || ']' AS sponsor,
        '[' || GROUP_CONCAT(DISTINCT '\"' || s.id || '\"') || ']' AS sponsors_id
    FROM house_trips ht
    JOIN trip_sponsors ts ON ht.doc_id = ts.doc_id
    JOIN sponsors s ON s.id = ts.sponsors_id
    GROUP BY ht.doc_id
),
destination_agg AS (
    SELECT
        ht.doc_id,
        '[' || GROUP_CONCAT(DISTINCT '{\"destination\": \"' || REPLACE(d.destination, '\"', '\"\"') || '\", \"id\": \"' || d.id || '\"}') || ']' AS destination,
        '[' || GROUP_CONCAT(DISTINCT '\"' || d.id || '\"') || ']' AS destination_id
    FROM house_trips ht
    JOIN trip_destinations td ON ht.doc_id = td.doc_id
    JOIN destinations d ON td.destinations_id = d.id
    GROUP BY ht.doc_id
)
SELECT
    ht.cleaned_filer_names,
    m.member_name,
    m.member_id,
    m.party,
    m.state,
    ht.doc_id,
    da.destination,
    sa.sponsor,
    ht.departure_date,
    ht.return_date,
    ht.trip_length,
    sa.sponsors_id,
    da.destination_id
FROM house_trips ht 
JOIN member m ON ht.member_id = m.member_id
JOIN sponsor_agg sa ON ht.doc_id = sa.doc_id
JOIN destination_agg da ON ht.doc_id = da.doc_id
GROUP BY ht.cleaned_filer_names, ht.doc_id, m.member_id;
"
###
# Sponsor top destinations and members + trips per year 
###

sqlite-utils create-table "$DB_FILE" sponsor_top_destinations sponsor_id text sponsor text top_destinations text top_members text trips_per_year text total_trips integer unique_offices integer

sqlite-utils query "$DB_FILE" "
WITH ranked_dest AS (
  SELECT 
    s.id AS sponsor_id,
    s.sponsor,
    d.id AS destination_id,
    d.destination,
    COUNT(td.doc_id) AS trip_count,
    ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY COUNT(td.doc_id) DESC) AS rn
  FROM trip_destinations td
  JOIN destinations d ON d.id = td.destinations_id
  JOIN house_trips ht ON ht.doc_id = td.doc_id
  JOIN trip_sponsors ts ON ts.doc_id = td.doc_id
  JOIN sponsors s ON s.id = ts.sponsors_id  
  GROUP BY s.id, d.id
),
top_destinations AS (
  SELECT 
    sponsor_id,
    sponsor,
    '[' || GROUP_CONCAT(
      '{\"destination_id\": ' || destination_id || 
      ', \"destination\": \"' || replace(destination, '\"', '\"\"') || 
      '\", \"count\": ' || trip_count || 
      ', \"label\": \"' || trip_count || ' ' || 
        CASE 
          WHEN trip_count = 1 THEN 'trip' 
          ELSE 'trips' 
        END || '\"}'
      , ', '
    ) || ']' AS top_destinations
  FROM ranked_dest
  WHERE rn <= 5
  GROUP BY sponsor_id, sponsor
),
ranked_members AS (
  SELECT 
    s.id AS sponsor_id,
    s.sponsor,
    m.member_id,
    TRIM(SUBSTR(m.member_name, INSTR(m.member_name, ',') + 1)) || ' ' || TRIM(SUBSTR(m.member_name, 1, INSTR(m.member_name, ',') - 1)) AS member_name,
    COUNT(ts.doc_id) AS member_trip_count,
    ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY COUNT(ht.doc_id) DESC) AS rn
  FROM house_trips ht
  JOIN trip_sponsors ts ON ts.doc_id = ht.doc_id
  JOIN sponsors s ON s.id = ts.sponsors_id  
  JOIN member m ON ht.member_id = m.member_id
  GROUP BY s.id, m.member_id
),
top_members AS (
  SELECT 
    sponsor_id,
    sponsor,
    '[' || GROUP_CONCAT(
      '{\"member_id\": \"' || member_id || 
      '\", \"member_name\": \"' || 
      replace(TRIM(SUBSTR(member_name, INSTR(member_name, ',') + 1)) || ' ' || TRIM(SUBSTR(member_name, 1, INSTR(member_name, ',') - 1)), '\"', '\"\"') ||
      '\", \"count\": ' || member_trip_count || 
      ', \"label\": \"' || member_trip_count || ' ' || 
        CASE 
          WHEN member_trip_count = 1 THEN 'trip' 
          ELSE 'trips' 
        END || '\"}'
      , ', '
    ) || ']' AS top_members
  FROM ranked_members
  WHERE rn <= 5
  GROUP BY sponsor_id, sponsor
),
sponsor_trips_year AS (
  SELECT 
    s.id AS sponsor_id,
    strftime('%Y', ht.departure_date) AS year,
    COUNT(ht.doc_id) AS trip_count
  FROM house_trips ht
  JOIN trip_sponsors ts ON ts.doc_id = ht.doc_id
  JOIN sponsors s ON s.id = ts.sponsors_id
  GROUP BY s.id, year
),
trips_per_year AS (
  SELECT 
    sponsor_id,
    '[' || GROUP_CONCAT(
      '{\"year\": ' || year || 
      ', \"trip_count\": ' || trip_count || '}'
      , ', '
    ) || ']' AS trips_per_year
  FROM sponsor_trips_year
  GROUP BY sponsor_id
),
total_trips AS (
  SELECT 
    s.id AS sponsor_id,
    COUNT(ht.doc_id) AS total_trips
  FROM house_trips ht
  JOIN trip_sponsors ts ON ts.doc_id = ht.doc_id
  JOIN sponsors s ON s.id = ts.sponsors_id
  GROUP BY s.id
),
unique_offices AS (
  SELECT 
    s.id AS sponsor_id,
    COUNT(DISTINCT ht.member_id) AS unique_offices
  FROM house_trips ht
  JOIN trip_sponsors ts ON ts.doc_id = ht.doc_id
  JOIN sponsors s ON s.id = ts.sponsors_id
  GROUP BY s.id
)
INSERT INTO sponsor_top_destinations (sponsor_id, sponsor, top_destinations, top_members, trips_per_year, total_trips, unique_offices)
SELECT 
  td.sponsor_id,
  td.sponsor,
  td.top_destinations,
  tm.top_members,
  ty.trips_per_year,
  tt.total_trips,
  uo.unique_offices
FROM top_destinations td
LEFT JOIN top_members tm ON td.sponsor_id = tm.sponsor_id
LEFT JOIN trips_per_year ty ON td.sponsor_id = ty.sponsor_id
LEFT JOIN total_trips tt ON td.sponsor_id = tt.sponsor_id
LEFT JOIN unique_offices uo ON td.sponsor_id = uo.sponsor_id;"







###
# All sponsor trips
sqlite-utils create-table "$DB_FILE" sponsor_trips sponsor text doc_id text member_name text member_id text departure_date text return_date text trip_length integer destination text cleaned_filer_names text party text state text sponsors_id text

sqlite-utils query "$DB_FILE" "
INSERT INTO sponsor_trips (
    member_name, member_id, doc_id, destination, party, state, cleaned_filer_names,
    sponsor, departure_date, return_date, trip_length, sponsors_id
)
WITH all_sponsors AS (
    SELECT 
        ht.doc_id,
        '[' || GROUP_CONCAT(
            '{\"sponsor\": \"' || REPLACE(s.sponsor, '\"', '\"\"') || '\", \"id\": \"' || s.id || '\"}'
        , ', ') || ']' AS sponsor
    FROM house_trips ht
    JOIN trip_sponsors ts ON ht.doc_id = ts.doc_id
    JOIN sponsors s ON ts.sponsors_id = s.id
    GROUP BY ht.doc_id
)
SELECT 
    TRIM(SUBSTR(m.member_name, INSTR(m.member_name, ',') + 1)) || ' ' ||
    TRIM(SUBSTR(m.member_name, 1, INSTR(m.member_name, ',') - 1)) AS member_name,
    m.member_id,
    ht.doc_id,
    '[' || GROUP_CONCAT(
        '{\"destination\": \"' || REPLACE(d.destination, '\"', '\"\"') || '\", \"id\": \"' || d.id || '\"}'
    , ', ') || ']' AS destination, -- This will correctly be under the destination column
    m.party,
    m.state,
    ht.cleaned_filer_names,
    '[' || GROUP_CONCAT(
        '{\"sponsor\": \"' || REPLACE(s.sponsor, '\"', '\"\"') || '\", \"id\": \"' || s.id || '\"}'
    , ', ') || ']' AS sponsor, -- This will correctly be under the sponsor column
    ht.departure_date,
    ht.return_date,
    ht.trip_length,
    s.id AS sponsors_id
FROM house_trips ht
JOIN member m ON ht.member_id = m.member_id
JOIN trip_sponsors ts ON ht.doc_id = ts.doc_id
JOIN sponsors s ON s.id = ts.sponsors_id
JOIN trip_destinations td ON ht.doc_id = td.doc_id
JOIN destinations d ON d.id = td.destinations_id
GROUP BY ht.doc_id, s.id
"


###

###
# Destination top sponsors and members + trips per year
###


sqlite-utils create-table "$DB_FILE" destination_top_sponsors destination_id text destination text top_sponsors text top_members text trips_per_year text total_trips integer average_trip_length text unique_offices text

sqlite-utils query "$DB_FILE" "
WITH ranked_sponsors AS (
  SELECT 
    d.id AS destination_id,
    d.destination,
    s.id AS sponsor_id,
    s.sponsor,
    COUNT(ts.doc_id) AS trip_count,
    ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY COUNT(ts.doc_id) DESC) AS rn
  FROM trip_sponsors ts
  JOIN sponsors s ON s.id = ts.sponsors_id
  JOIN house_trips ht ON ht.doc_id = ts.doc_id
  JOIN trip_destinations td ON ts.doc_id = td.doc_id
  JOIN destinations d ON d.id = td.destinations_id
  GROUP BY d.id, s.id
),
top_sponsors AS (
  SELECT 
    destination_id,
    destination,
    '[' || GROUP_CONCAT(
      '{\"sponsor_id\": ' || sponsor_id || 
      ', \"sponsor\": \"' || replace(sponsor, '\"', '\"\"') || 
      '\", \"count\": ' || trip_count || 
      ', \"label\": \"' || trip_count || ' ' || 
        CASE 
          WHEN trip_count = 1 THEN 'trip' 
          ELSE 'trips' 
        END || '\"}'
      , ', '
    ) || ']' AS top_sponsors
  FROM ranked_sponsors
  WHERE rn <= 5
  GROUP BY destination_id, destination
),
ranked_members AS (
  SELECT 
    d.id AS destination_id,
    d.destination,
    m.member_id,
    TRIM(SUBSTR(m.member_name, INSTR(m.member_name, ',') + 1)) || ' ' || TRIM(SUBSTR(m.member_name, 1, INSTR(m.member_name, ',') - 1)) AS member_name,
    COUNT(ht.doc_id) AS member_trip_count,
    ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY COUNT(ht.doc_id) DESC) AS rn
  FROM house_trips ht
  JOIN trip_destinations td ON ht.doc_id = td.doc_id
  JOIN destinations d ON d.id = td.destinations_id
  JOIN member m ON ht.member_id = m.member_id
  GROUP BY d.id, m.member_id
),
top_members AS (
  SELECT 
    destination_id,
    destination,
    '[' || GROUP_CONCAT(
      '{\"member_id\": \"' || member_id || 
      '\", \"member_name\": \"' || replace(member_name, '\"', '\"\"') || 
      '\", \"count\": ' || member_trip_count || 
      ', \"label\": \"' || member_trip_count || ' ' || 
        CASE 
          WHEN member_trip_count = 1 THEN 'trip' 
          ELSE 'trips' 
        END || '\"}'
      , ', '
    ) || ']' AS top_members
  FROM ranked_members
  WHERE rn <= 5
  GROUP BY destination_id, destination
),
destination_trips_year AS (
  SELECT 
    d.id AS destination_id,
    strftime('%Y', ht.departure_date) AS year,
    COUNT(ht.doc_id) AS trip_count
  FROM house_trips ht
  JOIN trip_destinations td ON ht.doc_id = td.doc_id
  JOIN destinations d ON d.id = td.destinations_id
  GROUP BY d.id, year
),
trips_per_year AS (
  SELECT 
    destination_id,
    '[' || GROUP_CONCAT(
      '{\"year\": ' || year || 
      ', \"trip_count\": ' || trip_count || '}'
      , ', '
    ) || ']' AS trips_per_year
  FROM destination_trips_year
  GROUP BY destination_id
),
total_trips AS (
  SELECT 
    d.id AS destination_id,
    COUNT(ht.doc_id) AS total_trips 
  FROM house_trips ht
  JOIN trip_destinations td ON td.doc_id = ht.doc_id
  JOIN destinations d ON d.id = td.destinations_id
  GROUP BY d.id
),
average_trip_length AS (
  SELECT 
    d.id AS destination_id,
    ROUND(AVG(ht.trip_length), 0) AS average_trip_length,
    CASE 
      WHEN ROUND(AVG(ht.trip_length), 0) < 10 THEN 
        CASE
          WHEN ROUND(AVG(ht.trip_length), 0) = 1 THEN 'one'
          WHEN ROUND(AVG(ht.trip_length), 0) = 2 THEN 'two'
          WHEN ROUND(AVG(ht.trip_length), 0) = 3 THEN 'three'
          WHEN ROUND(AVG(ht.trip_length), 0) = 4 THEN 'four'
          WHEN ROUND(AVG(ht.trip_length), 0) = 5 THEN 'five'
          WHEN ROUND(AVG(ht.trip_length), 0) = 6 THEN 'six'
          WHEN ROUND(AVG(ht.trip_length), 0) = 7 THEN 'seven'
          WHEN ROUND(AVG(ht.trip_length), 0) = 8 THEN 'eight'
          WHEN ROUND(AVG(ht.trip_length), 0) = 9 THEN 'nine'
          ELSE ROUND(AVG(ht.trip_length), 0)  -- For decimals less than 10
        END
      ELSE ROUND(AVG(ht.trip_length), 0)  -- For 10 or above, just use the number
    END || ' ' || 
    CASE 
      WHEN ROUND(AVG(ht.trip_length), 0) = 1 THEN 'day'
      ELSE 'days'
    END AS average_trip_length_label
  FROM house_trips ht
  JOIN trip_destinations td ON td.doc_id = ht.doc_id
  JOIN destinations d ON d.id = td.destinations_id
  WHERE ht.trip_length IS NOT NULL
  GROUP BY d.id
),
unique_offices AS (
  SELECT 
    d.id AS destination_id,
    COUNT(DISTINCT ht.member_id) AS unique_offices
  FROM house_trips ht
  JOIN trip_destinations td ON td.doc_id = ht.doc_id
  JOIN destinations d ON d.id = td.destinations_id
  GROUP BY d.id
)
INSERT INTO destination_top_sponsors (destination_id, destination, top_sponsors, top_members, trips_per_year, total_trips, average_trip_length, unique_offices)
SELECT 
  ts.destination_id,
  ts.destination,
  ts.top_sponsors,
  tm.top_members,
  ty.trips_per_year,
  tt.total_trips,
  atl.average_trip_length_label,
  uo.unique_offices
FROM top_sponsors ts 
LEFT JOIN top_members tm ON ts.destination_id = tm.destination_id
LEFT JOIN trips_per_year ty ON ts.destination_id = ty.destination_id
LEFT JOIN total_trips tt ON ts.destination_id = tt.destination_id
LEFT JOIN average_trip_length atl ON ts.destination_id = atl.destination_id
LEFT JOIN unique_offices uo ON ts.destination_id = uo.destination_id
GROUP BY ts.destination_id, ts.destination;
"

###
# destination trips
###

sqlite-utils create-table "$DB_FILE" destination_trips destination text doc_id text member_name text member_id text departure_date text return_date text trip_length integer sponsor text cleaned_filer_names text party text state text destinations_id text

sqlite-utils query "$DB_FILE" "
INSERT INTO destination_trips (
    member_name, member_id, doc_id, destination, party, state, cleaned_filer_names,
    sponsor, departure_date, return_date, trip_length, destinations_id
)
WITH all_destinations AS (
    SELECT 
        ht.doc_id,
        '[' || GROUP_CONCAT(
            '{\"destination\": \"' || REPLACE(d.destination, '\"', '\"\"') || '\", \"id\": \"' || d.id || '\"}'
        , ', ') || ']' AS destination
    FROM house_trips ht
    JOIN trip_destinations td ON ht.doc_id = td.doc_id
    JOIN destinations d ON td.destinations_id = d.id
    GROUP BY ht.doc_id
)
SELECT 
    TRIM(SUBSTR(m.member_name, INSTR(m.member_name, ',') + 1)) || ' ' ||
    TRIM(SUBSTR(m.member_name, 1, INSTR(m.member_name, ',') - 1)) AS member_name,
    m.member_id,
    ht.doc_id,
    '[' || GROUP_CONCAT(
        '{\"destination\": \"' || REPLACE(d.destination, '\"', '\"\"') || '\", \"id\": \"' || d.id || '\"}'
    , ', ') || ']' AS destination,
    m.party,
    m.state,
    ht.cleaned_filer_names,
    '[' || GROUP_CONCAT(
        '{\"sponsor\": \"' || REPLACE(s.sponsor, '\"', '\"\"') || '\", \"id\": \"' || s.id || '\"}'
    , ', ') || ']' AS sponsor,
    ht.departure_date,
    ht.return_date,
    ht.trip_length,
    d.id AS destinations_id
FROM house_trips ht
JOIN member m ON ht.member_id = m.member_id
JOIN trip_destinations td ON ht.doc_id = td.doc_id
JOIN destinations d ON td.destinations_id = d.id
JOIN trip_sponsors ts ON ht.doc_id = ts.doc_id
JOIN sponsors s ON s.id = ts.sponsors_id
GROUP BY ht.doc_id, d.id
"





###
# home page
###

sqlite-utils create-table "$DB_FILE" home_table top_sponsors text top_members text top_destinations text dest_trips_per_year text sponsor_trips_per_year text total_trips text first_year integer

sqlite-utils query "$DB_FILE" "
INSERT INTO home_table (top_sponsors, top_members, top_destinations, dest_trips_per_year, sponsor_trips_per_year, total_trips, first_year)
WITH ranked_sponsors AS (
  SELECT 
    s.id AS sponsor_id,
    s.sponsor,
    COUNT(ts.doc_id) AS trip_count
  FROM trip_sponsors ts
  JOIN sponsors s ON s.id = ts.sponsors_id
  GROUP BY s.id
  ORDER BY trip_count DESC
  LIMIT 5
),
top_sponsors AS (
  SELECT
    '[' || GROUP_CONCAT(
      '{\"sponsor_id\": ' || sponsor_id || 
      ', \"sponsor\": \"' || replace(sponsor, '\"', '\"\"') || 
      '\", \"count\": ' || trip_count || '}'
    , ', ') || ']' AS top_sponsors
  FROM ranked_sponsors
),
ranked_dest AS (
  SELECT 
    d.id AS destination_id,
    d.destination,
    COUNT(td.doc_id) AS trip_count
  FROM trip_destinations td
  JOIN destinations d ON d.id = td.destinations_id
  GROUP BY d.id
  ORDER BY trip_count DESC
  LIMIT 5
),
top_destinations AS (
  SELECT
    '[' || GROUP_CONCAT(
      '{\"destination_id\": ' || destination_id || 
      ', \"destination\": \"' || replace(destination, '\"', '\"\"') || 
      '\", \"count\": ' || trip_count || '}'
    , ', ') || ']' AS top_destinations
  FROM ranked_dest
),
ranked_members AS (
  SELECT 
    ht.member_id,
    TRIM(SUBSTR(ht.member_name, INSTR(ht.member_name, ',') + 1)) || ' ' || TRIM(SUBSTR(ht.member_name, 1, INSTR(ht.member_name, ',') - 1)) AS member_name,
    COUNT(ht.doc_id) AS member_trip_count
  FROM house_trips ht
  GROUP BY ht.member_id
  ORDER BY member_trip_count DESC
  LIMIT 5
),
top_members AS (
  SELECT 
    '[' || GROUP_CONCAT(
      '{\"member_id\": \"' || member_id || 
      '\", \"member_name\": \"' || replace(member_name, '\"', '\"\"') || 
      '\", \"count\": ' || member_trip_count || '}'
    , ', ') || ']' AS top_members
  FROM ranked_members
),
dest_trips_per_year AS (
  SELECT
    '[' || GROUP_CONCAT(
      '{\"year\": ' || year || 
      ', \"count\": ' || trip_count || '}'
    , ', ') || ']' AS total_trips
  FROM (
    SELECT 
      strftime('%Y', ht.departure_date) AS year,
      COUNT(*) AS trip_count
    FROM trip_destinations td
    JOIN house_trips ht ON ht.doc_id = td.doc_id
    GROUP BY year
  )
),
sponsor_trips_per_year AS (
  SELECT
    '[' || GROUP_CONCAT(
      '{\"year\": ' || year || 
      ', \"count\": ' || sponsor_count || '}'
    , ', ') || ']' AS total_trips
  FROM (
    SELECT 
      strftime('%Y', ht.departure_date) AS year,
      COUNT(DISTINCT ts.sponsors_id) AS sponsor_count  
    FROM house_trips ht
    JOIN trip_sponsors ts ON ht.doc_id = ts.doc_id 
    GROUP BY year
  )
),
total_trips AS (
  SELECT 
    COUNT(ht.doc_id) AS total_trips
  FROM house_trips ht
),
first_year AS (
  SELECT 
    MIN(strftime('%Y', ht.departure_date)) AS first_year
  FROM house_trips ht
)
SELECT 
  ts.top_sponsors,
  tm.top_members,
  td.top_destinations,
  dtt.total_trips,
  stt.total_trips,
  tt.total_trips,
  fy.first_year
FROM top_sponsors ts, top_members tm, top_destinations td, dest_trips_per_year dtt, sponsor_trips_per_year stt, total_trips tt, first_year fy;
"

###
# Member page
###

sqlite-utils create-table "$DB_FILE" member_top_sponsors_destinations member_id text member_name text top_sponsors text top_destinations text trips_per_year text total_trips text state text district text
sqlite-utils query "$DB_FILE" "
INSERT INTO member_top_sponsors_destinations (member_id, member_name, top_sponsors, top_destinations, trips_per_year, total_trips, state, district)
WITH member_info AS (
  SELECT DISTINCT 
    member_id, 
    TRIM(SUBSTR(member_name, INSTR(member_name, ',') + 1)) || ' ' || TRIM(SUBSTR(member_name, 1, INSTR(member_name, ',') - 1)) AS member_name
  FROM house_trips
),
ranked_sponsors AS (
  SELECT 
    ht.member_id,
    mi.member_name,
    s.id AS sponsor_id,
    s.sponsor,
    COUNT(ts.doc_id) AS sponsor_trip_count,
    ROW_NUMBER() OVER (PARTITION BY ht.member_id ORDER BY COUNT(ts.doc_id) DESC) AS rn
  FROM trip_sponsors ts
  JOIN sponsors s ON s.id = ts.sponsors_id
  JOIN house_trips ht ON ht.doc_id = ts.doc_id
  JOIN member_info mi ON mi.member_id = ht.member_id
  GROUP BY ht.member_id, s.id
),
top_sponsors AS (
  SELECT
    member_id,
    member_name,
    '[' || GROUP_CONCAT(
      '{\"sponsor_id\": ' || sponsor_id || 
      ', \"sponsor\": \"' || REPLACE(sponsor, '\"', '\"\"') || 
      '\", \"count\": ' || sponsor_trip_count || 
      ', \"label\": \"' || sponsor_trip_count || ' ' || 
        CASE 
          WHEN sponsor_trip_count = 1 THEN 'trip' 
          ELSE 'trips' 
        END || '\"}'
      , ', '
    ) || ']' AS top_sponsors
  FROM ranked_sponsors
  WHERE rn <= 5
  GROUP BY member_id, member_name
),
ranked_dest AS (
  SELECT 
    ht.member_id,
    mi.member_name,
    d.id AS destination_id,
    d.destination,
    COUNT(td.doc_id) AS destination_trip_count,
    ROW_NUMBER() OVER (PARTITION BY ht.member_id ORDER BY COUNT(td.doc_id) DESC) AS rn
  FROM trip_destinations td
  JOIN destinations d ON d.id = td.destinations_id
  JOIN house_trips ht ON ht.doc_id = td.doc_id
  JOIN member_info mi ON mi.member_id = ht.member_id
  GROUP BY ht.member_id, d.id
),
top_destinations AS (
  SELECT
    member_id,
    member_name,
    '[' || GROUP_CONCAT(
      '{\"destination_id\": ' || destination_id || 
      ', \"destination\": \"' || REPLACE(destination, '\"', '\"\"') || 
      '\", \"count\": ' || destination_trip_count || 
      ', \"label\": \"' || destination_trip_count || ' ' || 
        CASE 
          WHEN destination_trip_count = 1 THEN 'trip' 
          ELSE 'trips' 
        END || '\"}'
      , ', '
    ) || ']' AS top_destinations
  FROM ranked_dest
  WHERE rn <= 5
  GROUP BY member_id, member_name
),
member_trips_year AS (
  SELECT 
    ht.member_id,
    strftime('%Y', ht.departure_date) AS year,
    COUNT(ht.doc_id) AS trip_count
  FROM house_trips ht
  GROUP BY ht.member_id, year
),
trips_per_year AS (
  SELECT 
    member_id,
    '[' || GROUP_CONCAT(
      '{\"year\": ' || year || 
      ', \"trip_count\": ' || trip_count || '}'
    , ', ') || ']' AS trips_per_year
  FROM member_trips_year
  GROUP BY member_id
),
total_trips AS (
  SELECT 
    ht.member_id,
    COUNT(ht.doc_id) AS total_trips
  FROM house_trips ht
  GROUP BY ht.member_id
)
SELECT
  ts.member_id,
  TRIM(
    TRIM(SUBSTR(ts.member_name, INSTR(ts.member_name, ',') + 1)) || ' ' ||
    TRIM(SUBSTR(ts.member_name, 1, INSTR(ts.member_name, ',') - 1))
  ) AS member_name,
  ts.top_sponsors,
  td.top_destinations,
  ty.trips_per_year,
  tt.total_trips,
  m.state,
  m.district
FROM top_sponsors ts
JOIN top_destinations td ON td.member_id = ts.member_id
JOIN trips_per_year ty ON ty.member_id = ts.member_id
JOIN total_trips tt ON tt.member_id = ts.member_id
JOIN member m ON m.member_id = ts.member_id;
"


###
# All member trips
###
sqlite-utils create-table "$DB_FILE" member_trips member_id text member_name text departure_date text return_date text trip_length integer doc_id text destination text sponsor text cleaned_filer_names text sponsors_id text destination_id text
sqlite-utils query "$DB_FILE" "
INSERT INTO member_trips (
    member_id, member_name, departure_date, return_date, trip_length,
    doc_id, destination, sponsor, cleaned_filer_names, sponsors_id, destination_id
)
WITH sponsor_agg AS (
    SELECT
        ht.doc_id,
        '[' || GROUP_CONCAT(DISTINCT '{\"sponsor\": \"' || REPLACE(s.sponsor, '\"', '\"\"') || '\", \"id\": \"' || s.id || '\"}') || ']' AS sponsor,
        '[' || GROUP_CONCAT(DISTINCT '\"' || s.id || '\"') || ']' AS sponsors_id
    FROM house_trips ht
    JOIN trip_sponsors ts ON ht.doc_id = ts.doc_id
    JOIN sponsors s ON s.id = ts.sponsors_id
    GROUP BY ht.doc_id
),
destination_agg AS (
    SELECT
        ht.doc_id,
        '[' || GROUP_CONCAT(DISTINCT '{\"destination\": \"' || REPLACE(d.destination, '\"', '\"\"') || '\", \"id\": \"' || d.id || '\"}') || ']' AS destination,
        '[' || GROUP_CONCAT(DISTINCT '\"' || d.id || '\"') || ']' AS destination_id
    FROM house_trips ht
    JOIN trip_destinations td ON ht.doc_id = td.doc_id
    JOIN destinations d ON td.destinations_id = d.id
    GROUP BY ht.doc_id
)
SELECT
    m.member_id,
    m.member_name,
    ht.departure_date,
    ht.return_date,
    ht.trip_length,
    ht.doc_id,
    da.destination,
    sa.sponsor,
    ht.cleaned_filer_names,
    sa.sponsors_id,
    da.destination_id
FROM house_trips ht
JOIN member m ON ht.member_id = m.member_id
JOIN sponsor_agg sa ON ht.doc_id = sa.doc_id
JOIN destination_agg da ON ht.doc_id = da.doc_id;
"

###
# Search table
###
sqlite-utils create-table "$DB_FILE" search_data \
    id text \
    name text \
    type text --pk=id

sqlite-utils query "$DB_FILE" "
INSERT OR REPLACE INTO search_data (id, name, type)
SELECT CAST(id AS TEXT), sponsor, 'sponsor' FROM sponsors
UNION ALL
SELECT CAST(id AS TEXT), destination, 'destination' FROM destinations
UNION ALL
SELECT member_id, member_name, 'member' FROM member
"

  

###
# drop unnecessary tables
###

sqlite-utils drop-table "$DB_FILE" senate_trips
sqlite-utils drop-table "$DB_FILE" mecea_trips
sqlite-utils drop-table "$DB_FILE" trip_destinations
sqlite-utils drop-table "$DB_FILE" trip_sponsors
sqlite-utils drop-table "$DB_FILE" house_trips

###
# Publish dataset
###

# datasette "$DB_FILE" --cors