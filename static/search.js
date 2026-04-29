// hardcode state abbreviations to full names so if I search Maryland or MD it will also give me results in that state
const usStates = {
    "AL": "Alabama","AK": "Alaska","AZ": "Arizona","AR": "Arkansas",
    "CA": "California","CO": "Colorado","CT": "Connecticut","DE": "Delaware",
    "FL": "Florida","GA": "Georgia","HI": "Hawaii","ID": "Idaho",
    "IL": "Illinois","IN": "Indiana","IA": "Iowa","KS": "Kansas",
    "KY": "Kentucky","LA": "Louisiana","ME": "Maine","MD": "Maryland",
    "MA": "Massachusetts","MI": "Michigan","MN": "Minnesota","MS": "Mississippi",
    "MO": "Missouri","MT": "Montana","NE": "Nebraska","NV": "Nevada",
    "NH": "New Hampshire","NJ": "New Jersey","NM": "New Mexico","NY": "New York",
    "NC": "North Carolina","ND": "North Dakota","OH": "Ohio","OK": "Oklahoma",
    "OR": "Oregon","PA": "Pennsylvania","RI": "Rhode Island","SC": "South Carolina",
    "SD": "South Dakota","TN": "Tennessee","TX": "Texas","UT": "Utah",
    "VT": "Vermont","VA": "Virginia","WA": "Washington","WV": "West Virginia",
    "WI": "Wisconsin","WY": "Wyoming"
};

// base path to construct urls for search results
let searchableItems = [];
let fuse;

// get all of the search data from the datasette endpoing
async function fetchAllData() {
    try {
        let allData = [];
        let nextUrl = 'https://congtrav-05-14-2025-648704443537.us-east1.run.app/congtravel_master/search_data.json?_size=1000';

        // loop thru paginated results
        while (nextUrl) {
            const response = await fetch(nextUrl);
            const data = await response.json();
            // append new rows
            if (data.rows) {
                allData.push(...data.rows);
            }
            // get next url for pagination
            nextUrl = data.next_url || null;
            if (allData.length > 10000) break;
        }

        searchableItems = [];
        // process each row to construct searchable items
        for (const row of allData) {
            const [rowid, name, type, id] = row;
            // construct url based on type and id
            let url;
            if (type === 'sponsor') {
                url = `${BASE_PATH}/sponsor/${id}.html`;
            } else if (type === 'destination') {
                url = `${BASE_PATH}/destination/${id}.html`;
            } else if (type === 'member') {
                url = `${BASE_PATH}/member/${id}.html`;
            }
            // if we have a url and name, add to searchable items with lowercase name f
            // this is so if i search "maryland" it will also match MD and show results within maryland
            if (url && name) {
                let state_abbr = "";
                let state_full = "";
                if (type === 'destination') {
                    const parts = name.split(', ');
                    const abbr = parts[parts.length - 1].trim().toUpperCase();
                    if (usStates[abbr]) {
                        state_abbr = abbr.toLowerCase();
                        state_full = usStates[abbr].toLowerCase();
                    }
                }
                searchableItems.push({
                    id: id,
                    name: name.toLowerCase(),
                    displayName: name,
                    type: type,
                    url: url,
                    state_full: state_full,
                    state_abbr: state_abbr
                });
            }
        }
        const seen = new Set();
        searchableItems = searchableItems.filter(item => {
            const key = `${item.id}-${item.type}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });
        // name has most weight
        fuse = new Fuse(searchableItems, {
            keys: [
                { name: 'name', weight: 0.7 },
                { name: 'state_full', weight: 0.2 },
                { name: 'state_abbr', weight: 0.1 }
            ],
            threshold: 0.6, // adjust threshold if needed
            includeScore: true,
            shouldSort: true, // sort by best match
            ignoreLocation: true,
            minMatchCharLength: 2
        });

    } catch (error) {
        console.error("Error fetching search data:", error);
    }
}
// call the function to fetch data on page load
fetchAllData();
// get user's search input and make dropdown
const searchInput = document.getElementById('searchInput');
const resultsDropdown = document.getElementById('resultsDropdown');

function displayResults(results) {
    if (!results.length) {
        resultsDropdown.innerHTML = '<div class="result-item">No results found</div>';
        resultsDropdown.classList.add('active');
        return;
    }
    // for the top 20, show the name and the url 
    resultsDropdown.innerHTML = results
        .map(({ item }) => `
            <a href="${item.url}" class="result-item">
                <div>${item.displayName}</div>
                <div class="result-type">${item.type}</div>
            </a>
        `)
        .join('');

    resultsDropdown.classList.add('active');
}
// add event listener to search input to perform search and show results
searchInput.addEventListener('input', e => {
    const term = e.target.value.trim().toLowerCase();
    if (!term || !fuse) {
        resultsDropdown.innerHTML = '';
        resultsDropdown.classList.remove('active');
        return;
    }

    const results = fuse.search(term).slice(0, 20);
    displayResults(results);
});
// hide dropdown if user clicks outside of it or the search input
document.addEventListener('click', e => {
    if (!searchInput.contains(e.target) && !resultsDropdown.contains(e.target)) {
        resultsDropdown.classList.remove('active');
    }
});
// show dropdown if user focuses on input and there is an input
searchInput.addEventListener('focus', () => {
    if (searchInput.value.trim() && fuse) {
        resultsDropdown.classList.add('active');
    }
});
