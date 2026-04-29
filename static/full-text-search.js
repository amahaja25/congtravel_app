let lastResults = []; // store the most recent search results
let scrollPositions = {}; // store scroll positions for each dropdown
let lastParsedQuery = null; // store the most recent advanced query

// loading indicator 
function showLoadingIndicator() {
    // Remove any existing loading 
    const existingOverlay = document.getElementById('loading-overlay');
    if (existingOverlay) {
        existingOverlay.remove();
    }

    // make loading spinner
    const loadingOverlay = document.createElement('div');
    loadingOverlay.id = 'loading-overlay';
    loadingOverlay.className = 'loading-overlay';
    
    loadingOverlay.innerHTML = `
        <div class="loading-content">
            <div class="loading-spinner"></div>
            <div class="loading-text">Searching documents...</div>
        </div>
    `;
    
    // put into results container instead of body
    const resultsContainer = document.getElementById('results');
    resultsContainer.insertBefore(loadingOverlay, resultsContainer.firstChild);
    
    // show with animation
    setTimeout(() => {
        loadingOverlay.classList.add('show');
    }, 10);
}

function hideLoadingIndicator() {
    const loadingOverlay = document.getElementById('loading-overlay');
    if (loadingOverlay) {
        loadingOverlay.classList.remove('show');
        // remove from DOM after animation completes
        setTimeout(() => {
            if (loadingOverlay.parentNode) {
                loadingOverlay.parentNode.removeChild(loadingOverlay);
            }
        }, 300);
    }
}

// fetch search results 
async function fetchSearchAll(rawQuery) {
    // built url with user's query in search json endpoint
    const url = `/search-json?q=${encodeURIComponent(rawQuery)}`;
    console.log(`Fetching search: ${url}`);
    
    try {
        const resp = await fetch(url);
        if (!resp.ok) {
            throw new Error(`HTTP error! status: ${resp.status}`);
        }
        const data = await resp.json();
        
        if (Array.isArray(data)) {
            console.log(`Received ${data.length} search results`);
            return data;
        }
        
        console.error('Unexpected response format:', data);
        return [];
    } catch (err) {
        console.error('Search fetch error:', err);
        throw err;
    }
}

document.addEventListener("DOMContentLoaded", async () => {
    console.log("Search page ready - metadata will be loaded from search results");

    // Attach search handlers
    document.getElementById('document-search-button').addEventListener('click', function() {
        console.log('🖱️ Search button clicked to initiate search');
        handleSearch();
    });
    document.getElementById('document-search-input').addEventListener('keypress', function(event) {
        if (event.key === 'Enter') {
            console.log('Enter key pressed to initiate search');
            handleSearch();
        }
    });
});

async function handleSearch() {
    // Start timing the search operation
    const searchStartTime = performance.now();
    console.log('Search initiated at:', new Date().toISOString());
    
    const query = document.getElementById('document-search-input').value.trim();
    const container = document.getElementById('results');

    if (query.length === 0) {
        container.innerHTML = "";
        return;
    }

    // Show loading indicator
    showLoadingIndicator();

    try {
        // Fetch a larger set of results and follow pagination if available
        const results = await fetchSearchAll(query);
        const searchResponseTime = performance.now();
        console.log(`Search response received in: ${(searchResponseTime - searchStartTime).toFixed(2)}ms`);

        // Parse advanced query (quoted phrases, +/-, AND/OR/NOT operators)
        const parsedQuery = parseAdvancedQuery(query);
        // remember parsed query globally so client-side filters can reuse it
        lastParsedQuery = parsedQuery;

        // Apply client-side advanced boolean/phrase filtering to server results
        const filteredResults = applyAdvancedFilters(results, parsedQuery);

        lastResults = filteredResults;
        renderFilterControls(filteredResults);
        renderTable(filteredResults, searchStartTime, parsedQuery);
    } catch (error) {
        console.error("Semantic search error:", error);
        // Hide loading indicator on error
        hideLoadingIndicator();
        // Alternative: use hideLoadingBar() if you're using the progress bar style
    }
}

// --- Advanced query parsing and client-side filtering ---
// Supports:
// - Quoted phrases: "new york"
// - Prefix +term meaning MUST include
// - Prefix -term meaning MUST NOT include
// - Operators AND / OR / NOT (basic left-to-right parsing)
function parseAdvancedQuery(rawQuery) {
    const must = [];    // MUST include
    const should = [];  // OR / optional
    const mustNot = []; // MUST NOT include

    if (!rawQuery || !rawQuery.trim()) return { must, should, mustNot };

    // Tokenize preserving quoted phrases
    const tokenRegex = /"([^"]+)"|(\S+)/g;
    let match;
    // Current operator context (default to OR/should)
    let currentOp = 'OR';

    while ((match = tokenRegex.exec(rawQuery)) !== null) {
        const phrase = match[1];
        const word = match[2];
        const token = phrase !== undefined ? phrase : word;
        if (!token) continue;

        const upper = token.toUpperCase();
        // Recognize explicit boolean operator tokens
        if (upper === 'AND' || upper === 'OR' || upper === 'NOT') {
            currentOp = upper;
            continue;
        }

        // Check for leading + or - on token itself
        if (token.startsWith('+')) {
            const cleaned = token.slice(1).trim();
            if (cleaned) must.push(cleaned);
            currentOp = 'OR';
            continue;
        }
        if (token.startsWith('-')) {
            const cleaned = token.slice(1).trim();
            if (cleaned) mustNot.push(cleaned);
            currentOp = 'OR';
            continue;
        }

        // If this token came from a quoted phrase, treat it as a MUST (exact phrase)
        const isPhrase = phrase !== undefined;

        // Otherwise dispatch according to current operator
        if (isPhrase) {
            must.push(token);
        } else if (currentOp === 'AND') {
            must.push(token);
        } else if (currentOp === 'NOT') {
            mustNot.push(token);
        } else {
            // OR or default
            should.push(token);
        }

        // Reset operator to default OR after consuming a term
        currentOp = 'OR';
    }

    // If there are no explicit must terms and there are should terms,
    // keep should as-is. If there are only plain tokens and no operators,
    // treat them as should (OR). Consumer can treat should as at-least-one.
    return { must, should, mustNot };
}

function applyAdvancedFilters(results, parsed) {
    if (!parsed) return results;
    const { must, should, mustNot } = parsed;

    // If nothing to filter, return original results
    if ((!must || must.length === 0) && (!should || should.length === 0) && (!mustNot || mustNot.length === 0)) {
        return results;
    }

    // Prepare normalized checks
    function docContains(docText, term) {
        if (!docText) return false;
        const lower = docText.toLowerCase();
        if (term.indexOf(' ') >= 0) {
            // phrase
            return lower.includes(term.toLowerCase());
        }
        // word boundary match for single words
        try {
            const re = new RegExp('\\b' + term.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&') + '\\b', 'i');
            return re.test(docText);
        } catch (e) {
            return lower.includes(term.toLowerCase());
        }
    }

    return (results || []).filter(r => {
        const docText = (r.doc || '').toString();

        // must: all must terms/phrases must be present
        for (const t of must) {
            if (!docContains(docText, t)) return false;
        }

        // mustNot: none of these should be present
        for (const t of mustNot) {
            if (docContains(docText, t)) return false;
        }

        // should: if there are should terms, at least one must match
        if (should && should.length > 0) {
            let found = false;
            for (const t of should) {
                if (docContains(docText, t)) { found = true; break; }
            }
            if (!found) return false;
        }

        return true;
    });
}

// Build list of highlight terms from parsed query (used by renderTable)
function getHighlightTerms(parsed) {
    if (!parsed) return [];
    const terms = [];
    (parsed.must || []).forEach(t => terms.push(t));
    (parsed.should || []).forEach(t => terms.push(t));
    return terms;
}

// --- Create dynamic filter dropdowns ---
function renderFilterControls(results) {
    const container = document.getElementById('results');

    // Extract unique names from search result metadata
    const members = new Set();
    const destinations = new Set();
    const sponsors = new Set();

    results.forEach(r => {
        // Use metadata that comes with each search result
        if (r.member_name) members.add(r.member_name);
        
        // Process destinations from result metadata
        if (r.destinations && Array.isArray(r.destinations)) {
            r.destinations.forEach(d => {
                if (d.name) destinations.add(d.name);
            });
        }
        
        // Process sponsors from result metadata
        if (r.sponsors && Array.isArray(r.sponsors)) {
            r.sponsors.forEach(s => {
                if (s.name) sponsors.add(s.name);
            });
        }
    });

    const filterHTML = `
        <div id="filter-controls" style="margin-bottom: 1em; margin-top: 1em; display: flex; gap: 1em; flex-wrap: wrap;">
            <div>
                <label for="member-filter">Filter by Member: <span id="member-count">(0 selected)</span></label>
                <button id="clear-members" class="clear-all-button" style="margin-left: 10px; margin-top: 8px; font-size: 0.8em; padding: 4px 8px; border-radius: 6px; cursor: pointer; display: inline-block;">Clear</button><br>
                <select id="member-filter" multiple style="padding: 4px; height: 100px; width: 280px; margin-top: 5px;">
                    ${[...members].sort().map(m => `<option value="${m}">${m}</option>`).join("")}
                </select>
            </div>

            <div>
                <label for="destination-filter">Filter by Destination: <span id="destination-count">(0 selected)</span></label>
                <button id="clear-destinations" class="clear-all-button" style="margin-left: 10px; margin-top: 8px; font-size: 0.8em; padding: 4px 8px; border-radius: 6px; cursor: pointer; display: inline-block;">Clear</button><br>
                <select id="destination-filter" multiple style="padding: 4px; height: 100px; width: 300px; margin-top: 5px;">
                    ${[...destinations].sort().map(d => `<option value="${d}">${d}</option>`).join("")}
                </select>
            </div>

            <div>
                <label for="sponsor-filter">Filter by Sponsor: <span id="sponsor-count">(0 selected)</span></label>
                <button id="clear-sponsors" class="clear-all-button" style="margin-left: 10px; margin-top: 8px; font-size: 0.8em; padding: 4px 8px; border-radius: 6px; cursor: pointer; display: inline-block;">Clear</button><br>
                <select id="sponsor-filter" multiple style="padding: 4px; height: 100px; width: 280px; margin-top: 5px;">
                    ${[...sponsors].sort().map(s => `<option value="${s}" style="white-space: normal; word-wrap: break-word;">${s}</option>`).join("")}
                </select>
            </div>

            <div style="flex-basis:100%; display:flex; justify-content:flex-start; margin-top:6px;">
                <button id="clear-all-filters" style="font-size:0.9em; padding:6px 10px; border-radius:6px; cursor:pointer;">Clear All</button>
            </div>
        </div>
        <div id="filter-instructions" style="margin-bottom: 1em; font-size: 0.9em; color: #666;">
            <em>Click to select or deselect multiple options and filter within your query. Use quotation marks for exact phrases, and + or - prefixes for required or excluded words.</em>
        </div>
    `;

    // Remove old filters and instructions if they exist
    const oldFilters = document.getElementById("filter-controls");
    const oldInstructions = document.getElementById("filter-instructions");
    if (oldFilters) oldFilters.remove();
    if (oldInstructions) oldInstructions.remove();
    
    // Insert filter controls at the beginning of the results container
    container.insertAdjacentHTML("afterbegin", filterHTML);

    // Attach event listeners
    document.getElementById("member-filter").addEventListener("change", function() {
        updateSelectionCount("member-filter", "member-count");
        applyFilters();
    });
    document.getElementById("destination-filter").addEventListener("change", function() {
        updateSelectionCount("destination-filter", "destination-count");
        applyFilters();
    });
    document.getElementById("sponsor-filter").addEventListener("change", function() {
        updateSelectionCount("sponsor-filter", "sponsor-count");
        applyFilters();
    });
    
    // Add clear button event listeners
    document.getElementById("clear-members").addEventListener("click", function() {
        clearSelection("member-filter", "member-count");
    });
    document.getElementById("clear-destinations").addEventListener("click", function() {
        clearSelection("destination-filter", "destination-count");
    });
    document.getElementById("clear-sponsors").addEventListener("click", function() {
        clearSelection("sponsor-filter", "sponsor-count");
    });

    // Clear all filters button
    const clearAllBtn = document.getElementById("clear-all-filters");
    if (clearAllBtn) {
        clearAllBtn.addEventListener('click', function() {
            clearAllFilters();
        });
    }
    
    // Initialize selection counts
    updateSelectionCount("member-filter", "member-count");
    updateSelectionCount("destination-filter", "destination-count");
    updateSelectionCount("sponsor-filter", "sponsor-count");
    
    // Enable simple click for multiple selection (no Ctrl/Cmd needed)
    enableSimpleMultiSelect("member-filter", "member-count");
    enableSimpleMultiSelect("destination-filter", "destination-count");
    enableSimpleMultiSelect("sponsor-filter", "sponsor-count");
    
    // Initialize cascading filters with no selections (show all options)
    updateCascadingFilterDropdowns([], [], []);
}

// --- Function to update selection count display ---
function updateSelectionCount(selectId, countId) {
    const select = document.getElementById(selectId);
    const countSpan = document.getElementById(countId);
    if (select && countSpan) {
        const selectedCount = Array.from(select.selectedOptions).length;
        countSpan.textContent = `(${selectedCount} selected)`;
    }
}

// --- Function to clear all selections for a filter ---
function clearSelection(selectId, countId) {
    const select = document.getElementById(selectId);
    if (select) {
        // Deselect all options
        Array.from(select.options).forEach(option => option.selected = false);
        updateSelectionCount(selectId, countId);
        applyFilters();
    }
}

// --- Clear all three filters at once ---
function clearAllFilters() {
    const memberSelect = document.getElementById("member-filter");
    const destSelect = document.getElementById("destination-filter");
    const sponsorSelect = document.getElementById("sponsor-filter");

    if (memberSelect) Array.from(memberSelect.options).forEach(o => o.selected = false);
    if (destSelect) Array.from(destSelect.options).forEach(o => o.selected = false);
    if (sponsorSelect) Array.from(sponsorSelect.options).forEach(o => o.selected = false);

    // Update counts in the UI
    updateSelectionCount("member-filter", "member-count");
    updateSelectionCount("destination-filter", "destination-count");
    updateSelectionCount("sponsor-filter", "sponsor-count");

    // Reapply filters once (will update cascading dropdowns and table)
    applyFilters();
}

// --- Enable simple click for multiple selection (no Ctrl/Cmd needed) ---
function enableSimpleMultiSelect(selectId, countId) {
    const select = document.getElementById(selectId);
    if (!select) return;
    
    // Check if event listeners are already attached to avoid duplicates
    if (select.hasAttribute('data-multiselect-enabled')) {
        return;
    }
    select.setAttribute('data-multiselect-enabled', 'true');
    
    // Track scroll position changes
    select.addEventListener('scroll', function() {
        scrollPositions[selectId] = select.scrollTop;
    });
    
    select.addEventListener('mousedown', function(e) {
        e.preventDefault();
        e.stopPropagation();
        const option = e.target;
        if (option.tagName === 'OPTION') {
            // Store the current scroll position
            const currentScrollTop = select.scrollTop;
            scrollPositions[selectId] = currentScrollTop;
            // Toggle behavior. For destination-filter we enforce single-selection
            // but allow clicking the selected option again to deselect (no selection).
            const wasSelected = option.selected;
            if (selectId === 'destination-filter') {
                if (wasSelected) {
                    // Was selected -> clicking again clears all
                    Array.from(select.options).forEach(o => o.selected = false);
                } else {
                    // Was not selected -> make it the only selected option
                    Array.from(select.options).forEach(o => o.selected = false);
                    option.selected = true;
                }
            } else {
                // Multi-selects: toggle the clicked option
                option.selected = !wasSelected;
            }
            
            // Immediately restore scroll position before any DOM updates
            select.scrollTop = currentScrollTop;
            
            updateSelectionCount(selectId, countId);
            applyFilters();
            
            // Ensure scroll position is maintained after all operations
            setTimeout(() => {
                select.scrollTop = currentScrollTop;
                scrollPositions[selectId] = currentScrollTop;
            }, 0);
        }
    });
    
    // Prevent all default behaviors that might cause scrolling
    select.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
    });
    
    select.addEventListener('focus', function(e) {
        // Restore scroll position when select gets focus
        if (scrollPositions[selectId] !== undefined) {
            select.scrollTop = scrollPositions[selectId];
        }
    });
}

// --- Apply dropdown filters dynamically ---
function applyFilters() {
    const memberSelect = document.getElementById("member-filter");
    const destSelect = document.getElementById("destination-filter");
    const sponsorSelect = document.getElementById("sponsor-filter");

    // Get selected values as arrays
    const selectedMembers = Array.from(memberSelect.selectedOptions).map(option => option.value);
    const selectedDestinations = Array.from(destSelect.selectedOptions).map(option => option.value);
    const selectedSponsors = Array.from(sponsorSelect.selectedOptions).map(option => option.value);

    const filtered = lastResults.filter(r => {
        // Use metadata from search result
        
        // Member matching - OR logic within selected members
        const memberMatch = selectedMembers.length === 0 || 
            selectedMembers.some(memberVal => r.member_name === memberVal);
        
        // Destination matching - OR logic within selected destinations
        const destinations = r.destinations || [];
        const destMatch = selectedDestinations.length === 0 || 
            selectedDestinations.some(destVal => 
                destinations.some(d => 
                    d.name === destVal || d.name.includes(destVal) || destVal.includes(d.name)
                )
            );
        
        // Sponsor matching - OR logic within selected sponsors
        const sponsors = r.sponsors || [];
        const sponsorMatch = selectedSponsors.length === 0 || 
            selectedSponsors.some(sponsorVal => 
                sponsors.some(s => 
                    s.name === sponsorVal || s.name.includes(sponsorVal) || sponsorVal.includes(s.name)
                )
            );

        // AND logic between different filter categories
        return memberMatch && destMatch && sponsorMatch;
    });

    // Update filter dropdowns to show only relevant options based on current selections
    updateCascadingFilterDropdowns(selectedMembers, selectedDestinations, selectedSponsors);
    
    // Render the filtered table, preserving the most recent parsed query so
    // quoted-phrase highlighting remains consistent after filtering.
    renderTable(filtered, null, lastParsedQuery);
}

// --- Update filter dropdown options based on current results ---
function updateFilterDropdowns(results) {
    const memberSelect = document.getElementById("member-filter");
    const destSelect = document.getElementById("destination-filter");
    const sponsorSelect = document.getElementById("sponsor-filter");
    
    if (!memberSelect || !destSelect || !sponsorSelect) return;

    // Store current selections (multiple values)
    const currentMembers = Array.from(memberSelect.selectedOptions).map(option => option.value);
    const currentDests = Array.from(destSelect.selectedOptions).map(option => option.value);
    const currentSponsors = Array.from(sponsorSelect.selectedOptions).map(option => option.value);

    // Extract unique names from filtered results using metadata
    const members = new Set();
    const destinations = new Set();
    const sponsors = new Set();

    results.forEach(r => {
        if (r.member_name) members.add(r.member_name);
        
        // Process destinations from result metadata
        if (r.destinations && Array.isArray(r.destinations)) {
            r.destinations.forEach(d => {
                if (d.name) destinations.add(d.name);
            });
        }
        
        // Process sponsors from result metadata
        if (r.sponsors && Array.isArray(r.sponsors)) {
            r.sponsors.forEach(s => {
                if (s.name) sponsors.add(s.name);
            });
        }
    });

    // Update member dropdown
    memberSelect.innerHTML = [...members].sort().map(m => 
        `<option value="${m}" ${currentMembers.includes(m) ? 'selected' : ''}>${m}</option>`
    ).join("");

    // Update destination dropdown
    destSelect.innerHTML = [...destinations].sort().map(d => 
        `<option value="${d}" ${currentDests.includes(d) ? 'selected' : ''}>${d}</option>`
    ).join("");

    // Update sponsor dropdown
    sponsorSelect.innerHTML = [...sponsors].sort().map(s => 
        `<option value="${s}" ${currentSponsors.includes(s) ? 'selected' : ''}>${s}</option>`
    ).join("");
    
    // Update selection counts after refreshing options
    updateSelectionCount("member-filter", "member-count");
    updateSelectionCount("destination-filter", "destination-count");
    updateSelectionCount("sponsor-filter", "sponsor-count");
}

// --- Update cascading filter dropdown options based on current selections ---
function updateCascadingFilterDropdowns(selectedMembers, selectedDestinations, selectedSponsors) {
    const memberSelect = document.getElementById("member-filter");
    const destSelect = document.getElementById("destination-filter");
    const sponsorSelect = document.getElementById("sponsor-filter");
    
    if (!memberSelect || !destSelect || !sponsorSelect) return;

    // Simpler, more reliable approach:
    // - Compute the subset of `lastResults` that matches the current selections
    // - Derive available members/destinations/sponsors from that subset
    // This guarantees that selecting one filter will narrow the other dropdowns
    // to only values that actually occur together in the current results.
    const availableMembers = new Set();
    const availableDestinations = new Set();
    const availableSponsors = new Set();

    const filteredBySelection = lastResults.filter(r => {
        // Use metadata from search result
        const memberName = r.member_name || "";
        const destinations = r.destinations || [];
        const sponsors = r.sponsors || [];

        const memberMatch = selectedMembers.length === 0 || selectedMembers.includes(memberName);

        // Destination match: require that a trip includes ALL selected destinations
        const destMatch = selectedDestinations.length === 0 || selectedDestinations.every(destVal =>
            destinations.some(d => d.name === destVal || d.name.includes(destVal) || destVal.includes(d.name))
        );

        const sponsorMatch = selectedSponsors.length === 0 || selectedSponsors.some(sponsorVal =>
            sponsors.some(s => s.name === sponsorVal || s.name.includes(sponsorVal) || sponsorVal.includes(s.name))
        );

        // AND across filter categories: a result must satisfy all currently selected categories
        return memberMatch && destMatch && sponsorMatch;
    });

    // Build available option sets from the filtered results
    filteredBySelection.forEach(r => {
        const memberName = r.member_name;
        const destinations = r.destinations || [];
        const sponsors = r.sponsors || [];

        if (memberName) availableMembers.add(memberName);
        destinations.forEach(d => availableDestinations.add(d.name));
        sponsors.forEach(s => availableSponsors.add(s.name));
    });

    // Helper function to update select options without losing event listeners
    function updateSelectOptions(selectElement, availableOptions, selectedOptions, selectId) {
        const currentScrollTop = selectElement.scrollTop;
        const sortedOptions = [...availableOptions].sort();
        
        // Only update if the options have actually changed
        const currentOptions = Array.from(selectElement.options).map(opt => opt.value).sort();
        const optionsChanged = JSON.stringify(currentOptions) !== JSON.stringify(sortedOptions);
        
        if (optionsChanged) {
            // Clear existing options
            selectElement.innerHTML = '';
            
            // Add new options
            sortedOptions.forEach(optionValue => {
                const option = document.createElement('option');
                option.value = optionValue;
                option.textContent = optionValue;
                option.selected = selectedOptions.includes(optionValue);
                selectElement.appendChild(option);
            });
            
            // Restore scroll position
            selectElement.scrollTop = currentScrollTop;
            scrollPositions[selectId] = currentScrollTop;
        } else {
            // Just update selections if options haven't changed
            Array.from(selectElement.options).forEach(option => {
                option.selected = selectedOptions.includes(option.value);
            });
        }
    }

    // Update dropdowns using the helper function
    updateSelectOptions(memberSelect, availableMembers, selectedMembers, 'member-filter');
    updateSelectOptions(destSelect, availableDestinations, selectedDestinations, 'destination-filter');
    updateSelectOptions(sponsorSelect, availableSponsors, selectedSponsors, 'sponsor-filter');
    
    // Update selection counts after refreshing options
    updateSelectionCount("member-filter", "member-count");
    updateSelectionCount("destination-filter", "destination-count");
    updateSelectionCount("sponsor-filter", "sponsor-count");
}

// --- Display results table ---
function displayResults(results) {
    renderTable(results, null, lastParsedQuery);
}

// --- Render just the table (without filter controls) ---
function renderTable(results, searchStartTime = null, parsedQuery = null) {
    const container = document.getElementById('results');
    
    // Find existing table container or create one
    let tableContainer = document.getElementById('table-container');
    if (!tableContainer) {
        tableContainer = document.createElement('div');
        tableContainer.id = 'table-container';
        container.appendChild(tableContainer);
    }

    const originalQuery = document.getElementById('document-search-input').value.trim();
    const query = originalQuery.toLowerCase();
    const highlightTerms = getHighlightTerms(parsedQuery);
    const phraseTerms = (parsedQuery?.must || []).filter(t => t.indexOf(' ') >= 0);

    tableContainer.innerHTML = `
        <table id="results-table" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>Trip</th>
                    <th>Document text</th>
                    <th>Destination</th>
                    <th>Office</th>
                    <th>Sponsor</th>
                    <th style="display:none;">Score</th>
                </tr>
            </thead>
            <tbody>
                ${results.map(r => {
                    const docText = r.doc || "";
                    const docId = r.doc_id || "N/A";
                    const score = r.score?.toFixed(2) ?? "N/A";

                    // Highlight snippet: prefer exact quoted phrases (must), then words
                    const lowerDoc = docText.toLowerCase();
                    let snippet = "";

                    // 1) Try phrase terms first (exact substring match)
                    let foundPhrase = null;
                    for (const phrase of phraseTerms) {
                        const idx = lowerDoc.indexOf(phrase.toLowerCase());
                        if (idx !== -1) { foundPhrase = { phrase, index: idx }; break; }
                    }

                    if (foundPhrase) {
                        const matchIndex = foundPhrase.index;
                        const snippetStart = Math.max(0, matchIndex - 100);
                        const snippetEnd = Math.min(docText.length, matchIndex + foundPhrase.phrase.length + 100);
                        const snippetRaw = docText.substring(snippetStart, snippetEnd);
                        const escapedPhrase = foundPhrase.phrase.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                        const re = new RegExp(`(${escapedPhrase})`, "gi");
                        snippet = snippetRaw.replace(re, '<mark>$1</mark>');
                    } else {
                        // 2) Fall back to highlighting individual terms (from parsedQuery if present), otherwise original query words
                        const queryWords = (highlightTerms && highlightTerms.length > 0) ? highlightTerms : originalQuery.split(/\s+/).filter(w => w.length > 2);
                        let bestMatch = { index: -1, word: "" };

                        for (const word of queryWords) {
                            const wordIndex = lowerDoc.indexOf(word.toLowerCase());
                            if (wordIndex !== -1 && (bestMatch.index === -1 || wordIndex < bestMatch.index)) {
                                bestMatch = { index: wordIndex, word };
                            }
                        }

                        if (bestMatch.index !== -1) {
                            const snippetStart = Math.max(0, bestMatch.index - 100);
                            const snippetEnd = Math.min(docText.length, bestMatch.index + 100);
                            let snippetRaw = docText.substring(snippetStart, snippetEnd);

                            for (const word of queryWords) {
                                const escapedWord = word.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&');
                                const re = new RegExp(`(${escapedWord})`, "gi");
                                snippetRaw = snippetRaw.replace(re, '<mark>$1</mark>');
                            }
                            snippet = snippetRaw;
                        } else {
                            snippet = docText.substring(0, 200) + "...";
                        }
                    }

                    // Get metadata from search result
                    const memberId = r.member_id || "";
                    const memberName = r.member_name || "N/A";
                    const destinations = r.destinations || [];
                    let sponsors = r.sponsors || [];

                    // Deduplicate sponsors
                    if (sponsors.length > 1) {
                        const seen = new Set();
                        sponsors = sponsors.filter(s => {
                            const key = s.name.toLowerCase().trim();
                            if (seen.has(key)) return false;
                            seen.add(key);
                            return true;
                        });
                    }

                    // HTML for lists
                    const destHTML = destinations.map(d => {
                        const destId = d.id || d.destination_id;
                        return destId 
                            ? `<li><a href="/destination/${encodeURIComponent(destId)}.html" target="_blank">${d.name}</a></li>` 
                            : `<li>${d.name}</li>`;
                    }).join("");

                    const sponsorHTML = sponsors.map(s => {
                        const sponsorId = s.id || s.sponsor_id;
                        return sponsorId
                            ? `<li><a href="/sponsor/${encodeURIComponent(sponsorId)}.html" target="_blank">${s.name}</a></li>` 
                            : `<li>${s.name}</li>`;
                    }).join("");

                    const memberLink = memberId ? 
                        `<a href="/member/${encodeURIComponent(memberId)}.html" target="_blank">${memberName}</a>` : 
                        memberName;

                    return `
                        <tr>
                            <td><a href="/trip/${docId}.html" target="_blank">View</a></td>
                            <td>${snippet}</td>
                            <td><ul style="padding-left:15px; margin:0;">${destHTML}</ul></td>
                            <td>${memberLink}</td>
                            <td><ul style="padding-left:15px; margin:0;">${sponsorHTML}</ul></td>
                            <td style="display:none;">${score}</td>
                        </tr>
                    `;
                }).join("")}
            </tbody>
        </table>
    `;

    // Reinitialize DataTable
    if ($.fn.DataTable.isDataTable('#results-table')) {
        $('#results-table').DataTable().destroy();
    }

    const dataTableInitStart = performance.now();
    const dataTable = $('#results-table').DataTable({
        pageLength: 10,
        order: [], // No initial sorting
        ordering: false, // Disable all sorting capabilities
        searching: false,
        columnDefs: [{ targets: [5], visible: false, searchable: false }],
        initComplete: function() {
            const dataTableInitEnd = performance.now();
            const dataTableInitTime = dataTableInitEnd - dataTableInitStart;
            console.log(`📋 DataTable initialized in: ${dataTableInitTime.toFixed(2)}ms`);
            
            if (searchStartTime) {
                const totalSearchTime = dataTableInitEnd - searchStartTime;
                console.log(`⏱️ TOTAL SEARCH TIME (search to DataTable load): ${totalSearchTime.toFixed(2)}ms`);
                console.log(`📈 Search Performance Summary:
                - Search response: ${searchStartTime ? ((dataTableInitStart - searchStartTime) - dataTableInitTime).toFixed(2) : 'N/A'}ms
                - DataTable init: ${dataTableInitTime.toFixed(2)}ms
                - Total time: ${totalSearchTime.toFixed(2)}ms`);
            }
            
            // Hide loading indicator when DataTable is fully loaded
            hideLoadingIndicator();
            // Alternative: use hideLoadingBar() if you're using the progress bar style
        },
        drawCallback: function() {
            // Also hide loading indicator on subsequent draws (e.g., filtering, pagination)
            // This ensures the loading indicator is hidden even if initComplete already ran
            hideLoadingIndicator();
        }
    });
    
    // Fallback: Hide loading indicator after a reasonable timeout to prevent it from getting stuck
    setTimeout(() => {
        hideLoadingIndicator();
    }, 5000); // 5 second fallback timeout
}
