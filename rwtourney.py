#!/usr/bin/python

import random

from messages import Upload, Request
from peer import Peer

class RwTourney(Peer):
    def post_init(self):
        self.state = dict()
        self.state["frac_random_bw"] = 0.1
        self.state["len_history"] = 2
        self.state["history_discount"] = 0.9
        self.state["request_count_factor"] = 0.9
    
    def requests(self, peers, history):
        """
        peers: available info about the peers (who has what pieces)
        history: what's happened so far as far as this peer can see

        returns: a list of Request() objects

        This will be called after update_pieces() with the most recent state.
        """
        # get pieces needed
        needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
        needed_pieces = filter(needed, range(len(self.pieces)))
        random.shuffle(needed_pieces)
        np_set = set(needed_pieces) 

        # map pieces to rarity
        pieces_available = []
        for p in peers:
            pieces_available += p.available_pieces
        pieces_available_count = [(piece, pieces_available.count(piece)) for piece in set(pieces_available)]

        # make max number of requests to each peer ordered by preference
        requests = []
        num_piece_requests = {piece: 0 for piece in set(pieces_available)}
        
        random.shuffle(peers)
        for peer in peers:
            # use different randomization for each peer
            random.shuffle(pieces_available_count)
            pieces_available_count.sort(key=lambda piece: piece[1] + self.state["request_count_factor"] * num_piece_requests[piece[0]])

            # preference is rarity + need
            piece_preference_order = list(filter(lambda piece: piece[0] in np_set, pieces_available_count))

            num_requests = 0
            for piece, _ in piece_preference_order:
                if piece in peer.available_pieces and num_requests < self.max_requests:
                    num_requests += 1
                    num_piece_requests[piece] += 1

                    start_block = self.pieces[piece]
                    r = Request(self.id, peer.id, piece, start_block)
                    requests.append(r)

        return requests

    def uploads(self, requests, peers, history):
        """
        requests -- a list of the requests for this peer for this round
        peers -- available info about all the peers
        history -- history for all previous rounds

        returns: list of Upload objects.

        In each round, this will be called after requests().
        """
        chosen = []
        bws = []

        if len(requests) != 0:
            round = history.current_round()
            last_downloads = history.downloads[round-self.state["len_history"]:round] if round != 0 else []
            request_ids = list(set([request.requester_id for request in requests]))

            # chosen_bw maps peer to proportion of upload bandwidth to receive
            chosen_bw = dict() 

            # first, the proportional unchoking slots
            if last_downloads:
                # calculate total discounted downloads over last len_history rounds
                last_id_blocks = dict()
                for i, download in enumerate(reversed(last_downloads)):
                    discount = self.state["history_discount"] ** i
                    for d in download:
                        if d.from_id in last_id_blocks:
                            last_id_blocks[d.from_id] += discount * d.blocks
                        else:
                            last_id_blocks[d.from_id] = discount * d.blocks

                # filter to only those who want to download
                last_id_blocks = {peer: blocks for peer, blocks in last_id_blocks.items() if peer in request_ids}

                if last_id_blocks:
                    total_upload_bw = float(sum(last_id_blocks.values()))
                    chosen_bw_list = [(from_id, (1 - self.state["frac_random_bw"]) * (blocks / total_upload_bw)) for from_id, blocks in last_id_blocks.items()]

                    # combine if multiple downloads from same peer
                    for peer, bw in chosen_bw_list:
                        if peer in chosen_bw:
                            chosen_bw[peer] += bw
                        else:
                            chosen_bw[peer] = bw

            if not chosen_bw:
                # there are no previous round downloads to reference, or the previous uploaders 
                # don't want to download, so allocate to a single peer randomly
                chosen_bw[random.choice(request_ids)] = 1 - self.state["frac_random_bw"]

            # next, the random upload
            unchosen_requests = list(filter(lambda p: p not in chosen_bw.keys(), request_ids))
            
            if unchosen_requests:
                # get pieces needed
                needed = lambda i: self.pieces[i] < self.conf.blocks_per_piece
                needed_pieces = filter(needed, range(len(self.pieces)))
                np_set = set(needed_pieces)

                # prefer to choose randomly among the unchosen peers, weighting by how many pieces peer has that I want
                weighted_unchosen_requests = []
                for peer in peers:
                    if peer.id in unchosen_requests:
                        num_wanted_pieces = len(np_set.intersection(set(peer.available_pieces)))
                        weighted_unchosen_requests += ([peer.id] * (num_wanted_pieces + 1 ))
                chosen_bw[random.choice(weighted_unchosen_requests)] = self.state["frac_random_bw"]
            else:
                # otherwise, choose randomly a peer to give more bandwidth 
                chosen_bw[random.choice(request_ids)] += self.state["frac_random_bw"]

            # total share of bandwidth should sum to 1
            tolerance = 0.0001
            assert abs(sum(chosen_bw.values()) - 1) < tolerance, "total proportion of upload bandwidth is not 1"
            
            # split bw as calculated
            chosen = chosen_bw.keys()
            bws = [bw * self.up_bw for bw in chosen_bw.values()]

            # fix floating point imprecision
            if sum(bws) > self.up_bw:
                small_decrement = 0.0000001
                bws = list(map(lambda bw: max(0, bw - small_decrement), bws))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in zip(chosen, bws)]
            
        return uploads
