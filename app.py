#########
# Setup #
#########

# Load libraries
import re
import requests
import json # for parsing json from datasette endpoints, parsing nested json in colums
from flask import Flask, render_template, request, jsonify # for flask app
from pandas import * # 
import sys 
from urllib.parse import urljoin # to append endpoint paths
from num2words import num2words # formatting numbers to words for display
from datetime import datetime # formatting dates for display


### for embeddings + full-text search ###
from gensim.models.doc2vec import Doc2Vec
import pickle
from scripts.search_utils import search # function that performs search each time

# main datasete url that we can get json enpoints for the data from
DATASETTE_URL = "https://congtrav-05-14-2025-648704443537.us-east1.run.app/"

# function to pull json from datasette API endpoints
def pull_json(path):
    all_rows = []
    # handle pagination by checking if there's a next url and fetching until no more next url
    next_path = path + "&_size=1000" if "?" in path else path + "?_size=1000"

    while next_path:
        url = urljoin(DATASETTE_URL + "/", next_path)
        print("Fetching:", url)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        all_rows.extend(data.get("rows", []))
        next_path = data.get("next_url", None)

        # if next path then just get path for next request, not full url since we're going to join w/ base url
        if next_path:
            next_path = next_path.replace(DATASETTE_URL + "/", "")  

    return {"rows": all_rows}


# URL base path
#URL_BASE_PATH = '/interactives/fall-2024/congressional_travel_explorer'

# Define URL_BASE_PATH dynamically based on the command-line argument
if len(sys.argv) > 1 and sys.argv[1] == 'freeze':
    URL_BASE_PATH = '/interactives/fall-2024/congressional_travel_explorer'
else:
    URL_BASE_PATH = ''

# File system base path

# define app
app = Flask(__name__, static_url_path=URL_BASE_PATH + '/static')

# specify the URL for the text data for doc2vec
house_text_url = 'https://congtrav-05-14-2025-648704443537.us-east1.run.app/congtravel_master/house_text.json'
response = requests.get(house_text_url)
# get all the full text as json 
data = response.json()


model = Doc2Vec.load("models/house_text_doc2vec.model")
with open("models/doc2vec_embeddings.pkl", "rb") as f:
    data = pickle.load(f)
    embeddings = data["embeddings"]
    documents = data["documents"]

# Load valid doc_ids once at startup instead of on each search
print("Loading valid doc_ids at startup...")
from scripts.search_utils import get_valid_doc_ids
valid_doc_ids_cache = get_valid_doc_ids()
print(f"Cached {len(valid_doc_ids_cache.get('doc_ids', set()))} valid doc_ids")



###### FULL TEXT SEARCH SETUP ######
# create json for full text search

print(f"Embeddings shape: {embeddings.shape}")
print(f"Number of documents: {len(documents)}")

# search queries 
# this isn't an endpoint, this is function to get the user query
@app.route("/search-json", methods=["GET"])
def search_endpoint():
    query = request.args.get("q")
    if not query:
        return jsonify({"error": "Missing query parameter 'q'"}), 400

    try:
        # use search function to tokenize and embed query, then compare to document embeddings
        # cap at top 500 results
        results = search(query, model, embeddings, documents, valid_doc_ids_cache, top_k=500)
        print(f"Returning {len(results)} results")
        return jsonify(results)
    except Exception as e:
        print(f"Search error: {e}")
        return jsonify({"error": str(e)}), 500

# this is the actual search page endpoint the user interacts with 
@app.route("/document-search.html")
def search_page():
    return render_template("document-search.html")


##########################################
# Define logic to populate each template #
##########################################


########################
# Individual trip page #
########################


# URL pattern


# endpoint for individual trip page -- one page for each document
@app.route('/trip/<doc_id>.html')
# Function to ingest data for trip page
def trip(doc_id):
    try:
        # first get the info as json from datasette endpoint for that specific doc id
        trip_info =  pull_json(f"congtravel_master/house_trip_page.json?doc_id={doc_id}")

        formatted_info = []
        for row in trip_info["rows"]:
            try:
                
                # parse sponsor and destination info from the json in each columns
                # the sponsor and destination info are nested json objects, so need to parse to get specific info 
                sponsor_data = json.loads(row[8])  
                destination_data = json.loads(row[7])  

                trip_length = row[11]
                # format trip length for display: AP style rules 
                format_trip_length = f"{num2words(int(trip_length))} day" if int(trip_length) == 1 else f"{int(trip_length):,} days" if int(trip_length) >= 10 else f"{num2words(int(trip_length))} days"
                trip_length = format_trip_length

                departure_date = row[9]
                return_date = row[10]

                departure_date_obj = datetime.strptime(departure_date, '%Y-%m-%d')
                return_date_obj = datetime.strptime(return_date, '%Y-%m-%d')

                departure_date = departure_date_obj.strftime('%B %d, %Y')
                return_date = return_date_obj.strftime('%B %d, %Y')


                formatted_info.append({
                    "doc_id": row[1],
                    "cleaned_filer_names": row[2],
                    "member_name": row[3],
                    "member_id": row[4],
                    "party": row[5],
                    "state": row[6],
                    "destination_info": destination_data,
                    "sponsor_info": sponsor_data,
                    "departure_date": departure_date,
                    "return_date": return_date,
                    "trip_length": trip_length,
                    "document_link": row[12]
            })
            except json.JSONDecodeError as e:
                print(f"JSON decoding error: {e}, row: {row}", flush=True)
            except IndexError as e:
                print(f"Index error: {e}, row: {row}", flush=True)

        

        return render_template('x_trip.html',
                               trip_info=formatted_info) #map=m

    except Exception:
        return render_template('trip.html', trip=None)


###########################
# Individual sponsor page #
###########################

@app.route('/sponsor/<sponsors_id>.html')
# Function to ingest data for trip page
def sponsor(sponsors_id):
    try:
        sponsor_trips = pull_json(f"congtravel_master/sponsor_trips.json?sponsors_id={sponsors_id}")
        print(f"Fetched {len(sponsor_trips)} sponsor trips", flush=True)
        sponsor_top_destinations = pull_json(f"congtravel_master/sponsor_top_destinations.json?sponsor_id={sponsors_id}")
        
        print(f"Fetching data for sponsors_id: {sponsors_id}", flush=True)
        print(f"Full JSON response: {sponsor_trips}", flush=True)
        print(json.dumps(sponsor_trips, indent=2), flush=True)


        if sponsor_trips['rows']:
            sponsor_data = json.loads(sponsor_trips['rows'][0][1])  
            sponsor_name = sponsor_data[0]['sponsor'] 
            print(f"Sponsor name: {sponsor_name}", flush=True) # Extract sponsor name
        else:
            sponsor_name = "Unknown Sponsor"
            sponsor_data = []

        formatted_trips = []
        for row in sponsor_trips["rows"]:
            try:
        
                sponsor_data = json.loads(row[1])  
                destination_data = json.loads(row[8])  

                formatted_trips.append({
                    "departure_date": row[5],
                    "return_date": row[6],
                    "cleaned_filer_names": row[9],
                    "member_id": row[4],
                    "member_name": row[3],
                    "destination_info": destination_data,  
                    "trip_length": row[7],
                    "doc_id": row[2]
            })
            except json.JSONDecodeError as e:
                print(f"JSON decoding error: {e}, row: {row}", flush=True)
            except IndexError as e:
                print(f"Index error: {e}, row: {row}", flush=True)

        # Debug output
        print(f"Processed formatted_trips: {json.dumps(formatted_trips, indent=2)}", flush=True)
        
        top_destinations = []
        top_members = []
        
        for row in sponsor_top_destinations['rows']:
            total_trips = row[-1]
        
        raw_member_data = row[4]
        fixed_member_data = re.sub(r'(\w+)(?=:)', r'"\1"', raw_member_data)

        for row in sponsor_top_destinations['rows']:
            try:
        # Parse destinations
                destinations_data = json.loads(row[3])  # Assuming column 3 stores top_destinations JSON
                for dest in destinations_data:
                    destination_count = dest['count']
                    formatted_dest_count = f"{destination_count:,} trip" if destination_count == 1 else f"{destination_count:,} trips"
                    top_destinations.append({
                        "destination_id": dest.get("destination_id"),  # Ensure this key exists
                        "destination": dest.get("destination"),
                        "count": formatted_dest_count,
                        "label": dest.get("label")  
                    })
                    
        # Parse members (Ensure this is outside of the destinations loop)
                members_data = json.loads(fixed_member_data)  # Assuming column 4 stores top_members JSON
                for member in members_data:
                    if 'member_id' in member and 'member_name' in member and 'count' in member:
                        count = member['count']
                        formatted_count = f"{count:,} trip" if count == 1 else f"{count:,} trips"
                        top_members.append({
                            "member_id": member['member_id'],
                            "member_name": member['member_name'],
                            "count": formatted_count,
                            "label": member["label"]
                        })  
                    else:
                        print(f"Missing expected keys in member data: {member}", flush=True)

                num = row[7]
                if num is not None and num < 10:
                    word = num2words(num)
                    label = "congressional office" if num == 1 else "congressional offices"
                    unique_offices = f"{word} {label}"
                elif num is not None and num >= 10:
                    word = num
                    label = "congressional offices"
                    unique_offices = f"{word} {label}"
                else:
                    unique_offices = 'no offices'

                total_trips = (f"{num2words(int(row[6]))} trip" if int(row[6]) == 1
                            else f"{int(row[6]):,} trips" if int(row[6]) >= 10
                            else f"{num2words(int(row[6]))} trips")


            except (json.JSONDecodeError, IndexError, KeyError) as e:
                print(f"Error processing row: {row}, error: {e}", flush=True)

        # Trips per year for graphic
        trips_per_year = []
        min_year, max_year = 2012, 2023  
          # Initialize total_trips with a default value

        for row in sponsor_top_destinations['rows']:
            if isinstance(row[5], str) and row[5].strip():
                try:
                    parsed_data = json.loads(row[5])
                    parsed_dict = {int(item["year"]): int(item["trip_count"]) for item in parsed_data}
            
                    yearly_data = [{"year": year, "trip_count": parsed_dict.get(year, 0)} for year in range(min_year, max_year + 1)]
                    trips_per_year.extend(yearly_data)  # Append instead of overwriting

                    print(f"Final trips_per_year: {trips_per_year}", flush=True)
                except json.JSONDecodeError as e:
                    print(f"JSON Decode Error: {e}", flush=True)
            

        
        return render_template('x_sponsor.html', sponsor_trips=formatted_trips, trips_per_year=trips_per_year,top_members=top_members,top_destinations=top_destinations,sponsor_name=sponsor_name, total_trips=total_trips, sponsor_top_destinations=sponsor_top_destinations, unique_offices=unique_offices)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}", flush=True)
        sponsor_trips = []
        sponsor_top_destinations = []
        return render_template('x_sponsor.html', sponsor=None, house_trips=None)

###############################
# Individual destination page #
###############################

# URL pattern
#destinations_id = 99

@app.route('/destination/<destinations_id>.html')
# Function to ingest data for trip page
def destination(destinations_id):
    try:
        destination_trips = pull_json(f"congtravel_master/destination_trips.json?destinations_id={destinations_id}")
        destination_top_sponsors = pull_json(f"congtravel_master/destination_top_sponsors.json?destination_id={destinations_id}")

        print(f"Fetching data for destinations_id: {destinations_id}", flush=True)

        if destination_trips['rows']:
            destination_data = json.loads(destination_trips['rows'][0][1])
            destination_name = destination_data[0]['destination']
            print(f"Destination name: {destination_name}", flush=True)
        else:
            destination_name = "Unknown Destination"

        formatted_trips = []
        for row in destination_trips["rows"]:
            try:
                sponsor_data = json.loads(row[8])  # Corrected variable usage
                formatted_trips.append({
                    "departure_date": row[5],
                    "return_date": row[6],
                    "cleaned_filer_names": row[9],
                    "member_id": row[4],
                    "member_name": row[3],
                    "sponsor_info": sponsor_data,
                    "trip_length": row[7],
                    "doc_id": row[2],
                })
            except (json.JSONDecodeError, IndexError) as e:
                print(f"Error processing row: {row}, error: {e}", flush=True)

        top_sponsors = []
        top_members = []


        for row in destination_top_sponsors['rows']:
            try:
                # Parse sponsors
                sponsors_data = json.loads(row[3])
                for dest in sponsors_data:
                    top_sponsors.append({
                        "sponsor_id": dest.get("sponsor_id"),
                        "sponsor": dest.get("sponsor"),
                        "count": dest.get("count"),
                        "label": dest.get("label")
                    })

                # Parse members
                members_data = json.loads(row[4])  # Fixed parsing logic
                for member in members_data:
                    if 'member_id' in member and 'member_name' in member and 'count' in member:
                        top_members.append({
                            "member_id": member['member_id'],
                            "member_name": member['member_name'],
                            "count": member['count'],
                            "label": member.get("label")  
                        })
                
                average_trip_length_label = row[7]


                total_trips = (f"{num2words(int(row[6]))} sponsored trip" if int(row[6]) == 1
                            else f"{int(row[6]):,} sponsored trips" if int(row[6]) >= 10
                            else f"{num2words(int(row[6]))} sponsored trips")

                num = row[8]
                if num is not None:
                    word = num2words(num).capitalize()
                    label = "congressional office" if num == 1 else "congressional offices"
                    unique_offices = f"{word} {label}"
                else:
                    unique_offices = 'No offices'
            except (json.JSONDecodeError, IndexError, KeyError) as e:
                print(f"Error processing row: {row}, error: {e}", flush=True)

        # Trips per year for graphic
        trips_per_year = []
        min_year, max_year = 2012, 2023

        for row in destination_top_sponsors['rows']:
            if isinstance(row[5], str) and row[5].strip():
                try:
                    parsed_data = json.loads(row[5])
                    parsed_dict = {int(item["year"]): int(item["trip_count"]) for item in parsed_data}
                    yearly_data = [{"year": year, "trip_count": parsed_dict.get(year, 0)} for year in range(min_year, max_year + 1)]
                    trips_per_year.extend(yearly_data)
                except json.JSONDecodeError as e:
                    print(f"JSON Decode Error: {e}", flush=True)

        

        return render_template(
            'x_destination.html',
            destination_trips=formatted_trips,
            trips_per_year=trips_per_year,
            total_trips=total_trips,
            destination_name=destination_name,
            destination_top_sponsors=destination_top_sponsors,
            top_members=top_members,
            top_sponsors=top_sponsors,
            average_trip_length_label=average_trip_length_label,
            unique_offices=unique_offices
        )

    except Exception as e:
        print(f"Error in destination function: {e}", flush=True)
        return render_template('x_destination.html', destination=None)


##########################
# Individual member page #
##########################


# URL pattern

# member_id = "S001189"

@app.route('/member/<member_id>.html')
# Function to ingest data for trip page
def member(member_id):
    try:
        
        member_trips = pull_json(f"congtravel_master/member_trips.json?member_id={member_id}")

        
        print("member_trips raw rows:", member_trips.get("rows", []), flush=True)

        member_top_sponsors_destinations = pull_json(f"congtravel_master/member_top_sponsors_destinations.json?member_id={member_id}")

        member_state = None
        member_party = None
        member_district = None
        member_info = pull_json(f"congtravel_master/member.json?member_id__exact={member_id}")
        if member_info['rows']:
            r = member_info['rows'][0]
            print("member row:", r, flush=True)
            member_state = r[2] if len(r) > 2 and r[2] else None
            member_party = r[3] if len(r) > 3 and r[3] else None
            member_district = r[4] if len(r) > 4 and r[4] else None
        
        formatted_trips = []

        for row in member_trips["rows"]:
            try:
        
                sponsor_data = json.loads(row[8])  
                destination_data = json.loads(row[7])  

                formatted_trips.append({
                    "doc_id": row[6],
                    "cleaned_filer_names": row[9],
                    "member_id": row[1],
                    "destination_info": destination_data,
                    "sponsor_info": sponsor_data,
                    "departure_date": row[3],
                    "return_date": row[4],
                    "trip_length": row[5],
                    "member_name": row[2]

            })
            except json.JSONDecodeError as e:
                print(f"JSON decoding error: {e}, row: {row}", flush=True)
            except IndexError as e:
                print(f"Index error: {e}, row: {row}", flush=True)
        
        for trip in formatted_trips:
            print(f"Trip sponsors: {trip['sponsor_info']}, Trip destinations: {trip['destination_info']}")

            
        top_sponsors = []
        top_destinations = []

        for row in member_top_sponsors_destinations['rows']:
            try:
                sponsors_data = json.loads(row[3])
                for member in sponsors_data:
                    top_sponsors.append({
                        "sponsor_id": member.get("sponsor_id"),
                        "sponsor": member.get("sponsor"),
                        "count": member.get("count"),
                        "label": member.get("label")
                    })
                destinations_data = json.loads(row[4])
                for member in destinations_data:
                    top_destinations.append({
                        "destination_id": member.get("destination_id"),
                        "destination": member.get("destination"),
                        "count": member.get("count"),
                        "label": member.get("label")
                    })

                
                total_trips = (f"{num2words(int(row[6]))} sponsored trip" if int(row[6]) == 1
                            else f"{int(row[6]):,} sponsored trips" if int(row[6]) >= 10
                            else f"{num2words(int(row[6]))} sponsored trips")
                member_name = row[2]
            except (json.JSONDecodeError, IndexError, KeyError) as e:
                print(f"Error processing row: {row}, error: {e}", flush=True)
                
        trips_per_year = []
        min_year, max_year = 2012, 2023

        for row in member_top_sponsors_destinations['rows']:
            if isinstance(row[5], str) and row[5].strip():
                try:
                    parsed_data = json.loads(row[5])
                    parsed_dict = {int(item["year"]): int(item["trip_count"]) for item in parsed_data}
                    yearly_data = [{"year": year, "trip_count": parsed_dict.get(year, 0)} for year in range(min_year, max_year + 1)]
                    trips_per_year.extend(yearly_data)
                except json.JSONDecodeError as e:
                    print(f"JSON Decode Error: {e}", flush=True)
        

        ##
        # Ingest needed table(s), filter based on value in url to only show one member on template
        ##

        # This will filter to return a dataframe of all trips associated with a given member, including staff trips from that office
        
        return render_template('x_member.html', 
                               member_trips=formatted_trips,
                               member_top_sponsors_destinations=member_top_sponsors_destinations,
                               trips_per_year=trips_per_year,
                               total_trips=total_trips,
                               top_sponsors=top_sponsors,
                               top_destinations=top_destinations,
                               member_id=member_id,
                               member_name=member_name,
                               member_state=member_state,
                               member_party=member_party,
                               member_district=member_district)

    except (KeyError, TypeError, ValueError) as e:
        print(f"Error loading member data for {member_id}: {e}", flush=True)
    return render_template('x_member.html', member=None)


# traveler-member page


#example URLs:  http://127.0.0.1:5000/member/S001189/filer/Matthew%20Hodge, http://127.0.0.1:5000/member/S001189/filer/Jessica%20Robertson

@app.route('/member/<member_id>/filer/<filer_name>.html')
def filer_in_office(member_id, filer_name):
    try:

        traveler_total = pull_json(f"congtravel_master/traveler_info.json?cleaned_filer_names__exact={filer_name}&member_id={member_id}")

        for row in traveler_total["rows"]: 
            total_trips = row[4]
            # format trips with AP style rules for display
            format_total_trips = f"{num2words(int(total_trips))} total trip" if int(total_trips) == 1 else f"{int(total_trips):,} total trips" if int(total_trips) >= 10 else f"{num2words(int(total_trips))} total trips"
            total_trips = format_total_trips


        traveler_trips = pull_json(f"congtravel_master/all_traveler_trips.json?cleaned_filer_names__exact={filer_name}&member_id={member_id}")

        formatted_trips = []
        for row in traveler_trips["rows"]:
            try:
        
                sponsor_data = json.loads(row[8])  
                destination_data = json.loads(row[7])  

                formatted_trips.append({
                    "cleaned_filer_names": row[1],
                    "member_name": row[2],
                    "member_id": row[3],
                    "party": row[4],
                    "state": row[5],
                    "doc_id": row[6],
                    "destination_info": destination_data,
                    "sponsor_info": sponsor_data,
                    "departure_date": row[9],
                    "return_date": row[10],
                    "trip_length": row[11],
                    "total_trips": row[12],
                    

            })
            except json.JSONDecodeError as e:
                print(f"JSON decoding error: {e}, row: {row}", flush=True)
            except IndexError as e:
                print(f"Index error: {e}, row: {row}", flush=True)
            
            print(f"Formatted trips: {formatted_trips}", flush=True)


        return render_template(
            'filer_member_office.html',
            traveler_trips=formatted_trips,
            total_trips=total_trips
        )

    except Exception:
        return render_template('filer_member_office.html', filer=None)
    

##############
# Index Page #
##############

# root/index page
@app.route('/')
def index_page():
    try:
        # we created a specific table in datasette that has all the info we want to show on homepage
        home_info = pull_json("/congtravel_master/home_table.json")
        print(f"Full JSON response: {home_info}", flush=True)

        row = home_info["rows"][0]

        top_five_sponsors = json.loads(row[1])
        top_five_members = json.loads(row[2])
        top_five_destinations = json.loads(row[3])
        trips_per_year = json.loads(row[4])
        sponsors_per_year = json.loads(row[5])
        total_trips = f"{int(row[6]):,}"
        first_year = row[7]

        print("Data loaded:", top_five_sponsors, top_five_members, top_five_destinations, flush=True)


        return render_template(
            'index.html',
            home_info=home_info,
            top_five_sponsors=top_five_sponsors,
            top_five_members=top_five_members,
            top_five_destinations=top_five_destinations,
            trips_per_year=trips_per_year,
            sponsors_per_year=sponsors_per_year,
            total_trips=total_trips,
            first_year=first_year)

    except Exception as e:
        print(f"Error in index_page: {e}", flush=True)
        return render_template('index.html', home_info=None)

    

@app.route('/about.html')
def about_page():
    return render_template('about.html')
    
@app.route('/all-sponsors.html')
def sponsor_page():
    sponsors = pull_json("congtravel_master/sponsors.json")
    rows = sponsors.get("rows", [])
    return render_template('all-sponsors.html', sponsor_results=rows)
    
@app.route('/all-destinations.html')
def destination_page():
    destinations = pull_json("congtravel_master/destinations.json")
    print(f"Sponsor JSON: {destinations}", flush=True)
    rows = destinations.get("rows", [])
    print(f"Parsed sponsor rows: {rows}", flush=True)

    
    return render_template('all-destinations.html', destination_results=rows)
    
@app.route('/all-members.html')
def member_page():
    members = pull_json("congtravel_master/member.json")
    rows = members.get("rows", [])
    return render_template('all-members.html', member_results=rows)

if __name__ == '__main__':
    app.run(debug=True)
