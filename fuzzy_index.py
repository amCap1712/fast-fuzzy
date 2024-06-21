#!/usr/bin/env python3

import csv
import os
import datetime
import logging
from math import fabs
from time import time, monotonic
import re
import sys

import psycopg2
from psycopg2.extras import DictCursor, execute_values

from sklearn.feature_extraction.text import TfidfVectorizer
from unidecode import unidecode

# For wolf
DB_CONNECT = "dbname=musicbrainz_db user=musicbrainz host=localhost port=5432 password=musicbrainz"
ARTIST_CONFIDENCE_THRESHOLD = .7
CHUNK_SIZE = 100000

try:
    import nmslib
    have_nmslib = True
except ImportError:
    have_nmslib = False

logger = logging.getLogger(__name__)

def ngrams(string, n=3):
    """ Take a lookup string (noise removed, lower case, etc) and turn into a list of trigrams """

    string = ' ' + string + ' '  # pad names for ngrams...
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]


class FuzzyIndex:
    '''
       Create a fuzzy index using a Term Frequency, Inverse Document Frequency (tf-idf)
       algorithm. Currently the libraries that implement this cannot be serialized to disk,
       so this is an in memory operation. Fortunately for our amounts of data, it should
       be quick to rebuild this index.
    '''

    def __init__(self):
        global have_nmslib

        self.have_nmslib = have_nmslib
        self.vectorizer = None
        self.index = None

    @staticmethod
    def encode_string(text):
        if text is None:
            return None
        return unidecode(re.sub(" +", "", re.sub(r'[^\w ]+', '', text)).strip().lower())

    def build(self, search_data):
        """
            Builds a new index and saves it to disk and keeps it in ram as well.
        """

        if not self.have_nmslib:
            return

        lookup_strings = []
        lookup_ids = []
        for value, lookup_id in search_data:
            lookup_strings.append(value)
            lookup_ids.append(lookup_id)

        self.vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
        # This function's performance degrades over time, despite only ever working on small indexes. WTF?
        lookup_matrix = self.vectorizer.fit_transform(lookup_strings)

        self.index = nmslib.init(method='simple_invindx',
                                 space='negdotprod_sparse_fast',
                                 data_type=nmslib.DataType.SPARSE_VECTOR)
        self.index.addDataPointBatch(lookup_matrix, lookup_ids)
        self.index.createIndex()

    def search(self, query_string):
        """
            Return IDs for the matches in a list. Returns a list of dicts with keys of lookup_string, confidence and recording_id.
        """
        if not self.have_nmslib:
            logger.warn("nmslib not installed and trying fuzzy search, but nothing will match. Install nmslib!")
            return []

        query_matrix = self.vectorizer.transform([query_string])
        # TOTUNE: k might need tuning
        results = self.index.knnQueryBatch(query_matrix, k=3, num_threads=5)

        output = []
        for index, conf in zip(results[0][0], results[0][1]):
            output.append({"confidence": fabs(conf), "id": index, "text": query_string })

        return output

class MappingLookup:

    def __init__(self):
        self.artist_data = {}

    def create_indexes(self, conn):

        # TODO: VA and more complex artist credits probably not handled correctly

        self.artist_index = FuzzyIndex()
        self.recording_indexes = {}

        t0 = monotonic()
        last_artist_credit_id = -1
        last_row = None

        # Read from CSV file, since no sort, faster to iterate
        with open('canonical_musicbrainz_data.csv', newline='') as csvfile:
            reader = csv.reader(csvfile)

            t0_chunk = 0.0
            recording_data = []
            for i, csv_row in enumerate(reader):
                if i == 0:
                    continue

                # If we've indexed 2M rows, stop indexing. In theory.
                # but during the indexing phase, there is a mysterious slow-down in
                # indexing, despite not doing any real work. Around 18M rows. Very odd.
                if i > 2000000:
                    break
        
                # Make the data look like it came from PG
                row = { "id": int(csv_row[0]),
                        "artist_credit_id": int(csv_row[1]),
                        "artist_credit_name": csv_row[3],
                        "recording_name": csv_row[7]
                      }

                if last_artist_credit_id >= 0 and row["artist_credit_id"] != last_artist_credit_id:
                    if recording_data:
                        if i < 1000000:
                            # If we're indexing, print dots. We see dots below 2M rows. Good.
                            # WTF is 18M + rows slow when we're not indexing??
                            print(".", end="")
                            recording_index = FuzzyIndex()
                            recording_index.build(recording_data)
                        else:
                            recording_index = None
                    recording_data = []

                    self.artist_data[last_row["artist_credit_id"]] = (FuzzyIndex.encode_string(last_row["artist_credit_name"]),
                                                                                               last_row["artist_credit_id"])
                    self.recording_indexes[last_row["artist_credit_id"]] = recording_index

                encoded = FuzzyIndex.encode_string(row["recording_name"])
                if encoded:
                    recording_data.append((encoded, row["id"]))
                last_row = row
                last_artist_credit_id = row["artist_credit_id"]

                if i and i % CHUNK_SIZE == 0:
                    t1_chunk = monotonic()
                    print("Indexed %d rows" % i, end="")
                    if t0_chunk:
                        print(" in %.1fs %.2f row/s" % (t1_chunk - t0_chunk, CHUNK_SIZE / (t1_chunk - t0_chunk)))
                    else:
                        print()
                    t0_chunk = t1_chunk

        #TODO: save last generated chunk

        self.artist_index.build(self.artist_data.values())
        t1 = monotonic()
        print("built indexes in %.1f seconds." % (t1 - t0))

    def search(self, artist_name, recording_name):

        # First do artist fuzzy search, which takes 1-2ms with a full index.
        artist_name = FuzzyIndex.encode_string(artist_name)
        recording_name = FuzzyIndex.encode_string(recording_name)
        artists = self.artist_index.search(artist_name)
        results = []

        # For each hit, search recordings.
        for artist in artists:
            artist["id"] = int(artist["id"])
            artist["text"] = self.artist_data[artist["id"]][0]
            if artist["confidence"] > ARTIST_CONFIDENCE_THRESHOLD:
                print("search recordings for: ", artist["text"])
                search_index = self.recording_indexes[artist["id"]]

                # check to see if the artist was indexed
                if search_index is None:
                    print("artist not indexed")
                    return []
                rec_results = search_index.search(FuzzyIndex.encode_string(recording_name))
                for result in rec_results:
                    results.append({ "artist_name": artist["text"],
                                     "artist_credit_id": artist["id"],
                                     "artist_confidence": artist["confidence"],
                                     "recording_name": result["text"],
                                     "canonical_id": result["id"],
                                     "recording_confidence": result["confidence"] })

        return results


mi = MappingLookup()

with psycopg2.connect(DB_CONNECT) as conn:
    mi.create_indexes(conn)
    while True:
        query = input("artist,recording>")
        if not query:
            continue
        try:
            artist_name, recording_name = query.split(",")
        except ValueError:
            print("Input must be artist then recording, separated by comma")
            continue
        t0 = monotonic()
        results = mi.search(artist_name, recording_name)
        t1 = monotonic()
        for result in results:
            print("%-40s %.3f %6d %-40s %.3f %6d" % (result["artist_name"],
                                                     result["artist_confidence"],
                                                     result["artist_credit_id"],
                                                     result["recording_name"],
                                                     result["recording_confidence"],
                                                     result["canonical_id"]))
            
        print("%.3fms" % ((t1 - t0) * 1000))
