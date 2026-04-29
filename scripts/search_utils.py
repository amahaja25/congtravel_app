from gensim.utils import simple_preprocess
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import requests
import re
import json
import os

# embed query using average of the word vectors from the model's word vectors
def embed_query(query, model):
    # average available word vectors from the model's wv.
    tokens = simple_preprocess(query)
    vecs = []
    try:
        for t in tokens:
            if t in model.wv:
                # for each token, if it is in the model's vocab, get its vector and add to list
                vecs.append(model.wv[t])
    except Exception:
        
        vecs = []

    # if there are vectors, then average them to get avg vector for query
    if vecs:
        arr = np.array(vecs)
        avg = np.mean(arr, axis=0)
        return avg
    # use infer_vector as fallback
    return model.infer_vector(tokens)

def get_valid_doc_ids():
    
    # filepath for valid document ids
    cache_file = "static/valid_doc_ids_cache.pkl"
    
    # load from cache first
    try:
        if os.path.exists(cache_file):
            print(f"Loading cached doc_ids from {cache_file}")
            import pickle
            with open(cache_file, 'rb') as f:
                cache_data = pickle.load(f)
                print(f"Loaded {len(cache_data.get('doc_ids', set()))} cached doc_ids")
                return cache_data
    except Exception as e:
        print(f"Could not load cache: {e}")
    
    # if cache doesn't exist, fetch from API
    print("Fetching doc_ids from API ...")
    try:
        # get all doc_ids and metadata — destination, sponsor, office — from the member_trips API with pagination
        url = "https://congtrav-05-14-2025-648704443537.us-east1.run.app/congtravel_master/member_trips.json"
        valid_doc_ids = set()
        metadata = {}
        
        # start with the first page
        current_url = f"{url}?_size=1000"
        
    
        while current_url:
            print(f"Fetching: {current_url}")
            response = requests.get(current_url, timeout=30)
            data = response.json()
            
            # extract doc_ids and metadata from this page
            for row in data.get("rows", []):
                doc_id = row[6]
                valid_doc_ids.add(doc_id)
                
                member_name = row[2]
                member_id = row[1]
                
                # parse destinations with IDs
                destinations = []
                try:
                    dest_data = json.loads(row[7])
                    for d in dest_data:
                        dest_id = d.get('destination_id') or d.get('id')
                        dest_name = d.get('destination') or d.get('name', '')
                        if dest_name:
                            destinations.append({'name': dest_name, 'id': dest_id})
                except:
                    pass
                
                # parse sponsors with IDs
                sponsors = []
                try:
                    sponsor_data = json.loads(row[8])
                    for s in sponsor_data:
                        sponsor_id = s.get('sponsor_id') or s.get('id')
                        sponsor_name = s.get('sponsor') or s.get('name', '')
                        if sponsor_name:
                            sponsors.append({'name': sponsor_name, 'id': sponsor_id})
                except:
                    pass
                
                # save metadata for each doc_id
                metadata[doc_id] = {
                    'member_name': member_name,
                    'member_id': member_id,
                    'destinations': destinations,
                    'sponsors': sponsors
                }
            
            # get next page URL
            next_url = data.get("next_url")
            if next_url:
                current_url = next_url
            else:
                current_url = None
        
        result = {'doc_ids': valid_doc_ids, 'metadata': metadata}
        
        # Save to cache for next time
        try:
            import pickle
            with open(cache_file, 'wb') as f:
                pickle.dump(result, f)
            print(f"Saved {len(valid_doc_ids)} doc_ids to cache at {cache_file}")
        except Exception as e:
            print(f"Could not save cache: {e}")
        
        print(f"Loaded {len(valid_doc_ids)} valid doc_ids with complete trip metadata")
        return result
    except Exception as e:
        print(f"Error loading valid doc_ids: {e}")
        return {'doc_ids': set(), 'metadata': {}}




#### This is the actual search function that we use for each document search query ####
def search(query, model, embeddings, documents, valid_doc_ids_cache, top_k=500):
    
    # embed the query using the same method as document embeddings 
    query_vec = embed_query(query, model).reshape(1, -1)
    # calculate cosine similarity between query and all of the document embeddings that we have saved earlier in the cache
    similarities = cosine_similarity(query_vec, embeddings)[0]
    
    # Get top candidates by similarity first to reduce processing
    if len(similarities) > top_k * 3:
        # Get top candidates 
        top_candidate_indices = np.argpartition(similarities, -top_k * 3)[-top_k * 3:]
        # Sort just these top candidates
        top_candidate_indices = top_candidate_indices[np.argsort(similarities[top_candidate_indices])[::-1]]
    else:
        # If fewer documents, just sort all
        top_candidate_indices = np.argsort(similarities)[::-1]
    
    # Prepare query for matching
    # Strip surrounding quotes for exact-phrase checks, normalize whitespace
    query_stripped = query.strip()
    if (query_stripped.startswith('"') and query_stripped.endswith('"')) or (query_stripped.startswith("'") and query_stripped.endswith("'")):
        query_stripped = query_stripped[1:-1]
    query_lower = re.sub(r'\s+', ' ', query_stripped).lower().strip()
    query_terms = [term.lower().strip() for term in re.split(r"\s+", query_stripped) if len(term) > 2]
    
    # Use pre-cached metadata from valid_doc_ids_cache
    # extract the doc_ids set and metadata dict so we can link to the correct pages in search results
    if isinstance(valid_doc_ids_cache, dict):
        valid_doc_ids = valid_doc_ids_cache.get('doc_ids', set())
        metadata_cache = valid_doc_ids_cache.get('metadata', {})
    else:
        # fallback 
        valid_doc_ids = valid_doc_ids_cache
        metadata_cache = {}
    
    # Advanced boolean query parsing (quoted phrases treated as MUST, support +/-, AND/OR/NOT)
    def parse_advanced_query(raw_query):
        must = []
        should = []
        must_not = []
        if not raw_query or not raw_query.strip():
            return {"must": must, "should": should, "must_not": must_not}

        token_regex = re.compile(r'"([^\"]+)"|(\S+)')
        current_op = 'OR'
        for m in token_regex.finditer(raw_query):
            phrase = m.group(1)
            word = m.group(2)
            token = phrase if phrase is not None else word
            if not token:
                continue
            up = token.upper()
            if up in ('AND', 'OR', 'NOT'):
                current_op = up
                continue

            if token.startswith('+'):
                cleaned = token[1:].strip()
                if cleaned:
                    must.append(cleaned)
                current_op = 'OR'
                continue
            if token.startswith('-'):
                cleaned = token[1:].strip()
                if cleaned:
                    must_not.append(cleaned)
                current_op = 'OR'
                continue

            is_phrase = (phrase is not None)
            if is_phrase:
                must.append(token)
            elif current_op == 'AND':
                must.append(token)
            elif current_op == 'NOT':
                must_not.append(token)
            else:
                should.append(token)

            current_op = 'OR'

        return {"must": must, "should": should, "must_not": must_not}

    parsed = parse_advanced_query(query)
    
    results = []
    # process only top candidates instead of all documents
    for i in top_candidate_indices:
        doc_entry = documents[i]  # dict with 'doc_id' and 'doc' fields
        doc_id = doc_entry.get("doc_id", f"doc_{i}")
        
        # only include documents that have complete trip metadata -- a sponsor, destination and member
        if doc_id in valid_doc_ids:
            # normalize document text: strip simple HTML, collapse whitespace, make all lowercase
            raw_text = doc_entry.get("text", "") or doc_entry.get("doc", "") or ''
            doc_text = re.sub(r'<[^>]+>', ' ', raw_text)
            doc_text = re.sub(r'\s+', ' ', doc_text).lower().strip()
            base_score = float(similarities[i])

            # presence of term or phrase in normalized doc_text
            def doc_contains(term):
                if not term:
                    return False
                term_norm = re.sub(r'\s+', ' ', term).lower().strip()
                if ' ' in term_norm:
                    return term_norm in doc_text
                try:
                    return re.search(r'\b' + re.escape(term_norm) + r'\b', doc_text, flags=re.I) is not None
                except Exception:
                    return term_norm in doc_text

            # apply boolean filters: MUST, MUST_NOT, SHOULD
            must_terms = parsed.get('must', [])
            must_not_terms = parsed.get('must_not', [])
            should_terms = parsed.get('should', [])

            # Enforce MUST
            failed = False
            for t in must_terms:
                if not doc_contains(t):
                    failed = True
                    break
            if failed:
                continue

            # Enforce MUST_NOT
            violated = any(doc_contains(t) for t in must_not_terms)
            if violated:
                continue

            # if there are SHOULD terms but no MUST terms, require at least one SHOULD
            if should_terms and not must_terms:
                if not any(doc_contains(t) for t in should_terms):
                    continue

            # calculate match type and boost
            exact_phrase_match = False
            if query_lower:
                exact_phrase_match = query_lower in doc_text
            else:
                # if no global query, treat any MUST phrase as exact phrase for scoring
                if any(' ' in t for t in must_terms):
                    exact_phrase_match = any(doc_contains(t) for t in must_terms if ' ' in t)

            individual_word_matches = sum(1 for term in query_terms if (re.search(r'\b' + re.escape(term) + r'\b', doc_text, flags=re.I) is not None))
            total_query_words = len(query_terms)

            boost = 0
            match_type = "semantic_only"

            if exact_phrase_match:
                boost = 0.5
                match_type = "exact_phrase"
            elif total_query_words > 0 and individual_word_matches == total_query_words:
                boost = 0.3
                match_type = "all_words"
            elif individual_word_matches > 0:
                boost = 0.1 * (individual_word_matches / total_query_words)
                match_type = f"partial_words_{individual_word_matches}/{total_query_words}"

            final_score = min(1.0, base_score + boost)
            
            # Add metadata if available
            doc_metadata = metadata_cache.get(doc_id, {})

            results.append({
                "doc_id": doc_id,
                "doc": doc_entry.get("text", ""),
                "score": final_score,
                "base_score": base_score,
                "boost": boost,
                "match_type": match_type,
                "individual_matches": individual_word_matches,
                "total_words": total_query_words,
                "member_name": doc_metadata.get('member_name', ''),
                "member_id": doc_metadata.get('member_id', ''),
                "destinations": doc_metadata.get('destinations', []),
                "sponsors": doc_metadata.get('sponsors', [])
            })
    
    # sort descending by final score, then by match type priority
    def sort_key(x):
        # primary sort: final score 
        # Secondary sort: match type priority (exact phrase > all words > partial words > semantic only)
        match_priority = {
            "exact_phrase": 4,
            "all_words": 3,
            "semantic_only": 0
        }
        # partial word matches
        if x["match_type"].startswith("partial_words"):
            match_priority[x["match_type"]] = 2
        
        priority = match_priority.get(x["match_type"], 0)
        return (x["score"], priority)
    
    results.sort(key=sort_key, reverse=True)
    
    # top results
    results = results[:top_k]
    return results