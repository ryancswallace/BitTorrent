#!/usr/bin/python

import random

from messages import Upload, Request
from util import even_split
from peer import Peer

class RwStd(Peer):
    def post_init(self):
        self.state = dict()
        self.state["optimistic_unchoke"] = None
        self.state["num_slots"] = 4
    
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
        random.shuffle(peers)
        for peer in peers:
            # use different randomization for each peer
            random.shuffle(pieces_available_count)
            pieces_available_count.sort(key=lambda piece: piece[1])

            # preference is rarity + need
            piece_preference_order = list(filter(lambda piece: piece[0] in np_set, pieces_available_count))

            num_requests = 0
            for piece, _ in piece_preference_order:
                if piece in peer.available_pieces and num_requests < self.max_requests:
                    num_requests += 1
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

            # first, the reciprocal unchoking slots 
            last_downloads = history.downloads[round-1] if round != 0 else []
            request_ids = [request.requester_id for request in requests]

            if last_downloads:
                # filter to only those who want to download, then sort by previous upload bandwith
                last_id_blocks = [(download.from_id, download.blocks) for download in last_downloads]
                last_id_blocks = list(filter(lambda p: p[0] in request_ids, last_id_blocks))
                last_id_blocks.sort(key=lambda download: download[1], reverse=True)
                last_id = [id for (id, _) in last_id_blocks]

                # select at most top three peers
                chosen += last_id[:(self.state["num_slots"] - 1)]

            # next, the optimistic unchoking slot
            if (round % 3) == 0 or self.state["optimistic_unchoke"] is None:
                # every third round, unchoke new agent
                unchosen_requests = list(filter(lambda p: p not in chosen, request_ids))
                if unchosen_requests:
                    opt_peer = random.choice(unchosen_requests)
                    chosen += [opt_peer]
                    self.state["optimistic_unchoke"] = opt_peer
                else:
                    unchosen_peers = list(filter(lambda p: p.id not in chosen, peers))
                    if unchosen_peers:
                        opt_peer = random.choice(unchosen_peers).id
                        chosen += [opt_peer]
                        self.state["optimistic_unchoke"] = opt_peer
            else:
                # unchoke agent optimistically unchoked previously
                if self.state["optimistic_unchoke"]:
                    chosen += [self.state["optimistic_unchoke"]]

            # now try to fill in remaining slots randomly if history is insufficient 
            unchosen_requests = [r for r in request_ids if r not in chosen]
            randomly_chosen = random.sample(unchosen_requests, max(min(len(unchosen_requests), self.state["num_slots"] - len(chosen)), 0))
            chosen += randomly_chosen
            
            # evenly split upload bandwidth among the chosen peeres
            bws = even_split(self.up_bw, len(chosen))

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in zip(chosen, bws)]

        return uploads
