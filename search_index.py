#!/usr/bin/env python3

from collections import defaultdict
from math import ceil
from pickle import load
from random import randint
from time import monotonic, sleep
from struct import unpack
import struct
import os
import sys

from fuzzy_index import FuzzyIndex
from shard_histogram import shard_histogram
from utils import split_dict_evenly
from database import Mapping
from database import open_db

RELEASE_CONFIDENCE = .5
RECORDING_CONFIDENCE = .5


class MappingLookupSearch:

    def __init__(self, index_dir, num_shards):
        self.index_dir = index_dir
        self.num_shards = num_shards
        self.shard = None
        self.shards = None

        self.artist_index = None
        self.artist_data = {}

        self.db_file = os.path.join(index_dir, "mapping.db")
        
    @staticmethod
    def chunks(l, n):
        d, r = divmod(len(l), n)
        for i in range(n):
            si = (d+1)*(i if i < r else r) + d*(0 if i < r else i - r)
            yield l[si:si+(d+1 if i < r else d)]

    def split_shards(self):
        """ determine how to break up shards """

        # Split the dict based on an even distribution of the value's sums
        split_hist = split_dict_evenly(shard_histogram, self.num_shards)

        self.shards = {}
        for shard, pairs in enumerate(split_hist):
            for ch in pairs:
                self.shards[ch] = shard

    def load_artist(self, artist_credit_id):
        """ Load one artist's release and recordings data from disk. Correct relrec_offsets chunk must be loaded. """

        # Have we loaded this already? If so, bail!
        try:
            return self.artist_data[artist_credit_id]
        except KeyError:
            pass

        recording_data = []
        recording_releases = defaultdict(list)
        release_data = []
        recording_ref = defaultdict(list)
        data = Mapping.select().where(Mapping.artist_credit_id == artist_credit_id)
        for i, row in enumerate(data):
            encoded = FuzzyIndex.encode_string(row.recording_name)
            recording_ref[encoded].append({ "id": row.recording_id,
                                            "release_id": row.release_id,
                                            "score": row.score })
            recording_releases[row.recording_id].append(row.release_id)

            encoded = FuzzyIndex.encode_string(row.release_name)
            if encoded:
                # Another data struct is needed from which to xref search results 
                # The int passed to the index is the index of this list, where a list of release_ids are.
                release_data.append({ "id": row.release_id,
                                      "text": encoded,
                                      "score": row.score })
                
        #TODO: Dedup this
#eb-1  |     release results for 'war'
#eb-1  |         rel id   name                     confidence
#eb-1  |         2013864  war                            1.00
#eb-1  |         2013864  war                            1.00
#eb-1  |         2013864  war                            1.00

        recording_data = []
        for i, text in enumerate(recording_ref):
            data = recording_ref[text]
            recording_data.append({ "text": text,
                                    "id": i, 
                                    "recording_data": data })

        release_ref = defaultdict(list)
        for release in release_data:
            release_ref[release["text"]].append((release["id"], release["score"]))
            
        release_data = []
        for i, text in enumerate(release_ref):
            release_data.append({ "text": text, "id": i, "release_id_scores": release_ref[text] })
            
#        print("create")
#        for e in sorted(recording_data, key=lambda x: x["text"]):
#            print("%.3f %-8d %-8d %-8d %s" % (0.0, e["id"], e["score"], e["release_id"], e["text"]))
#        print()
        
        recording_index = FuzzyIndex()
        if recording_data:
            recording_index.build(recording_data, "text")
        else:
            return None

        release_index = FuzzyIndex()
        if release_data:
            release_index.build(release_data, "text")
        else:
            return None

        entry = {
            "recording_index": recording_index,
            "recording_data": recording_data,
            "release_index": release_index,
            "recording_releases": recording_releases,
            "release_data": release_data
        }
        self.artist_data[artist_credit_id] = entry
        
        return entry

    def search(self, req):

        artist_ids = req["artist_ids"]
        artist_name = FuzzyIndex.encode_string(req["artist_name"])
        release_name = FuzzyIndex.encode_string(req["release_name"])
        recording_name = FuzzyIndex.encode_string(req["recording_name"])

#        print(f"      ids:", artist_ids)
#        print(f"   artist: {artist_name:<30} {req['artist_name']:<30}")
#        print(f"  release: {release_name:<30} {req['release_name']:<30}")
#        print(f"recording: {recording_name:<30} {req['recording_name']:<30}")
#        print()
        
        open_db(self.db_file)

        results = []
        for artist_id in artist_ids:
            artist_data = self.load_artist(artist_id)
            if artist_data is None:
                print(f"artist {artist_id} not found on this shard. {self.shard}")
                continue

            rec_results = artist_data["recording_index"].search(recording_name, min_confidence=RECORDING_CONFIDENCE)
            exp_results = []
            for result in rec_results:
                data = artist_data["recording_data"][result["id"]]
                for d in data["recording_data"]:
                    exp_results.append({ "text": result["text"],
                                         "confidence": result["confidence"],
                                         "id": d["id"],
                                         "score": d["score"],
                                         "release_id": d["release_id"]})
                    
            rec_results = sorted(exp_results, key=lambda r: (-r["confidence"], r["score"]))
                
#            print("    recording results for '%s'" % recording_name)
#            print("        rec id   name                     confidence score")
#            if rec_results:
#                for res in rec_results:
#                    if res["confidence"] > .0:
#                        print("        %-8d %-30s %.2f %d" % (res["id"], res["text"], res["confidence"], res["score"]))
#            else:
#                print("    ** No recording results **")
#            print()
            

            if not release_name:
                if not rec_results:
                    continue
                return [ (r["release_id"], r["id"], r["confidence"], req["id"]) for r in rec_results[:3] ]

            rel_results = artist_data["release_index"].search(release_name, min_confidence=RELEASE_CONFIDENCE)
            exp_results = []
            for result in rel_results:
                data = artist_data["release_data"][result["id"]]
                for release_id, score in data["release_id_scores"]:
                    exp_results.append({ "text": result["text"],
                                         "confidence": result["confidence"],
                                         "id": release_id,
                                         "score": score })

            rel_results = sorted(exp_results, key=lambda r: (-r["confidence"], r["score"]))
#           print("    release results for '%s'" % release_name)
#           if rel_results:
#               print("        rel id   name                     confidence")
#               for res in rel_results:
#                   if res["confidence"] > .0:
#                       print("        %-8d %-30s %.2f" % (res["id"], res["text"], res["confidence"]))
#               print()
#           else:
#               print("    ** No release results **")


            RESULT_THRESHOLD = .7
            hits = []
            for rec_res in rec_results[:3]:
#                if rec_res["confidence"] < RESULT_THRESHOLD:
#                    break
                for rel_res in rel_results[:3]:
#                    if rel_res["confidence"] < RESULT_THRESHOLD:
#                        break
                    if rec_res["id"] in artist_data["recording_releases"]:
                        hits.append({ "recording_id": rec_res["id"],
                                      "recording_text": rec_res["text"],
                                      "recording_conf": rec_res["confidence"],
                                      "score": rec_res["score"],
                                      "release_id": rel_res["id"],
                                      "release_name": rel_res["text"],
                                      "release_conf": rel_res["confidence"],
                                      "confidence": (rec_res["confidence"] + rel_res["confidence"])/2 })
                       
            if not hits:
                continue
                        
#            print("    combined results for '%s'" % release_name)
#            hits = sorted(hits, key=lambda h: h["confidence"], reverse=True)
#            if hits:
#                print("        rec id   name                     confidence score    rel id   name                conf")
#                for hit in hits:
#                    print("        %-8d %-30s %.2f %-8d %-8d %-30s %.2f -> %.2f" % (hit["recording_id"],
#                                                             hit["recording_text"],
#                                                             hit["recording_conf"],
#                                                             hit["score"],
#                                                             hit["release_id"],
#                                                             hit["release_name"],
#                                                             hit["release_conf"],
#                                                             hit["confidence"]
#                                                              ))
#                print()
#            else:
#                print("    ** No hits **")

            return [ (hits[0]["release_id"], hits[0]["recording_id"], hits[0]["confidence"], req["id"]) ]