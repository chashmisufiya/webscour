from flask import Flask, render_template, request
import json
import os

app = Flask(__name__)

# ---------- Path setup ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, "..")
INDEXER_DIR = os.path.join(PROJECT_ROOT, "indexer")

INVERTED_INDEX_PATH = os.path.join(INDEXER_DIR, "inverted_index.json")
IDF_PATH = os.path.join(INDEXER_DIR, "idf.json")

# ---------- Load index files ----------
with open(INVERTED_INDEX_PATH, "r", encoding="utf-8") as f:
    inverted_index = json.load(f)

with open(IDF_PATH, "r", encoding="utf-8") as f:
    idf = json.load(f)

# ---------- Route ----------
@app.route("/", methods=["GET", "POST"])
def index():
    results = []
    query = ""

    if request.method == "POST":
        query = request.form["query"].lower().strip()

        if query in inverted_index:
            score = round(idf.get(query, 0), 4)

            for rank, doc in enumerate(inverted_index[query], start=1):
                results.append({
                    "rank": rank,
                    "document": doc,
                    "score": score
                })

    return render_template("index.html", query=query, results=results)

# ---------- Run ----------
if __name__ == "__main__":
    app.run(debug=True)
