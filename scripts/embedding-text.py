import requests
import json
import logging
from gensim.utils import simple_preprocess
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

# load text data and parse thru w pagination
def fetch_all_documents(base_url):
    all_documents = []
    current_url = f"{base_url}?_size=1000"
    page_count = 0
    
    while current_url:
        page_count += 1
        print(f"Fetching page {page_count} from: {current_url}")
        
        try:
            response = requests.get(current_url)
            data = response.json()
            
            # check structure of response
            if isinstance(data, list):
                documents_in_page = data
                print(f"Page {page_count} returned {len(documents_in_page)} documents (array format)")
                all_documents.extend(documents_in_page)
                
                # no next_url in array format, so we're done
                current_url = None
            else:
                # response should have 'rows' field and possibly 'next_url'
                if 'rows' in data:
                    documents_in_page = data['rows']
                    print(f"Page {page_count} returned {len(documents_in_page)} documents (object format)")
                    all_documents.extend(documents_in_page)
                    
                    # use next_url if available, otherwise stop
                    current_url = data.get('next_url')
                    if current_url:
                        print(f"Next URL: {current_url}")
                    else:
                        print("No next_url found, stopping pagination")
                else:
                    print(f"Unexpected response format on page {page_count}: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                    break
                    
        except Exception as e:
            print(f"Error fetching page {page_count}: {e}")
            break
    
    print(f"Total documents fetched: {len(all_documents)}")
    
    # print sample doc_ids to verify we're getting different documents
    if all_documents and len(all_documents) > 0:
        print("Sample document IDs:")
        sample_size = min(10, len(all_documents))
        for i in range(0, len(all_documents), len(all_documents) // sample_size):
            if i < len(all_documents) and 'doc_id' in all_documents[i]:
                print(f"  Document {i}: {all_documents[i]['doc_id']}")
    
    return all_documents

# endpoint with all of the text data
house_text_url = 'https://congtrav-05-14-2025-648704443537.us-east1.run.app/congtravel_master/house_text.json'
documents = fetch_all_documents(house_text_url)

# check the structure of the first few documents
print("Document structure analysis:")
if documents:
    print(f"First document type: {type(documents[0])}")
    if isinstance(documents[0], dict):
        print(f"First document keys: {list(documents[0].keys())}")
    elif isinstance(documents[0], list):
        print(f"First document (list) length: {len(documents[0])}")
        print(f"First few elements: {documents[0][:5] if len(documents[0]) >= 5 else documents[0]}")
    else:
        print(f"First document content: {documents[0]}")

# extract and tokenize documents 
tokenized_documents = []
processed_documents = []

for i, doc in enumerate(documents):
    try:
        if isinstance(doc, dict):
            # Dictionary format - look for text field
            text_content = doc.get("text", "") or doc.get("doc", "") or str(doc)
            doc_id = doc.get("doc_id", f"doc_{i}")
        elif isinstance(doc, list):
            # List format - based on debug output: [rowid, doc_id, text_content]
            if len(doc) >= 3:
                doc_id = doc[1]  # Second element is doc_id
                text_content = doc[2]  # Third element is text content
            elif len(doc) >= 2:
                doc_id = doc[0]
                text_content = doc[1]
            else:
                doc_id = f"doc_{i}"
                text_content = str(doc[0]) if len(doc) > 0 else ""
        else:
            # String or other format
            text_content = str(doc)
            doc_id = f"doc_{i}"
        
        if text_content:
            tokenized_documents.append(simple_preprocess(text_content))
            processed_documents.append({"doc_id": doc_id, "text": text_content})
    except Exception as e:
        print(f"Error processing document {i}: {e}")
        continue

print(f"Successfully processed {len(tokenized_documents)} documents")
documents = processed_documents  # Use the processed documents for the final pickle

# check if we have valid tokenized documents
if not tokenized_documents:
    print("ERROR: No valid tokenized documents found!")
    exit(1)

# check tokenized document structure
print(f"Sample tokenized documents:")
for i in range(min(3, len(tokenized_documents))):
    print(f"  Doc {i} ({len(tokenized_documents[i])} tokens): {tokenized_documents[i][:10]}...")  # First 10 tokens

# check if documents have enough tokens - they should have at least a couple
total_tokens = sum(len(doc) for doc in tokenized_documents)
print(f"Total tokens across all documents: {total_tokens}")

# check for empty documents
empty_docs = sum(1 for doc in tokenized_documents if len(doc) == 0)
print(f"Empty documents: {empty_docs}/{len(tokenized_documents)}")

# show some sample original text to see what we're working with
print("Sample original text:")
for i in range(min(3, len(processed_documents))):
    text_preview = processed_documents[i]["text"][:200] + "..." if len(processed_documents[i]["text"]) > 200 else processed_documents[i]["text"]
    print(f"  Doc {i}: {text_preview}")

# tag each document with unique ID for processing
tagged_data = [TaggedDocument(words=words, tags=[str(i)]) for i, words in enumerate(tokenized_documents)]

print(f"Created {len(tagged_data)} tagged documents")

# train the Doc2Vec model with 100 dimensions
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
model = Doc2Vec(vector_size=100, window=5, min_count=1, workers=4, epochs=40)  # Changed min_count to 1

print("Building vocabulary...")
model.build_vocab(tagged_data)
print(f"Vocabulary size: {len(model.wv.key_to_index)}")

if len(model.wv.key_to_index) == 0:
    print("ERROR: No vocabulary built! Check document content.")
    exit(1)

print("Training model...")
model.train(tagged_data, total_examples=model.corpus_count, epochs=model.epochs)

# save the model
model.save("models/house_text_doc2vec.model")

import numpy as np 
import pickle 
# embed each document using the trained model 
doc_vectors = np.array([model.dv[str(i)] for i in range(len(tokenized_documents))])
# save vectors and original text
with open("models/doc2vec_embeddings.pkl", "wb") as f:
    pickle.dump({ "embeddings": doc_vectors, "documents": documents }, f)
