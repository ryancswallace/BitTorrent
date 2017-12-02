#!/usr/bin/python

import random

from messages import Upload, Request
from peer import Peer

class RwTyrant(Peer):
    def post_init(self):
        self.state = dict()
        self.state["gamma"] = 0.05
        self.state["r"] = 3
        self.state["alpha"] = 0.2
        self.state["cap"] = self.up_bw

        self.f = dict()
        self.tau = dict()
        self.prev_unchoked = []
    
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
        round = history.current_round()
        
        if round == 0:
            # initialize f and tau
            for peer in peers:
                self.f[peer.id] = 1
                self.tau[peer.id] = self.up_bw / float(4)

        else:
            # if necessary, update f and tau from previous round
            last_download = history.downloads[round-1]
            last_id_blocks = {download.from_id: download.blocks for download in last_download}
            last_unchoked = [download.from_id for download in last_download if download.blocks > 0]

            last_r_downloads = history.downloads[round-self.state["r"]:round]
            if len(last_r_downloads) == self.state["r"]:
                # history goes back r rounds
                unchoked_sets = []
                for download in last_r_downloads:
                    unchoked_sets += [set([d.from_id for d in download if d.blocks > 0])]
                last_r_unchoked = list(set.intersection(*unchoked_sets))
            else:
                # history doesn't go back r rounds
                last_r_unchoked = None

            for peer_id in self.prev_unchoked:
                if not peer_id in last_unchoked:
                    # choked, increase tau
                    self.tau[peer_id] = (1 + self.state["alpha"]) * self.tau[peer_id]
                else:
                    # unchoked, f is observed rate
                    self.f[peer_id] = last_id_blocks[peer_id]
                if not last_r_unchoked is None:
                    for peer in last_r_unchoked:
                        # chronically unchoked, decrease tau
                        self.tau[peer_id] = (1 - self.state["gamma"]) * self.tau[peer_id]

        # now select uploads for this round
        chosen = []
        bws = []
        if len(requests) != 0:
            # calculate return on investment
            ratios = [(peer, self.f[peer] / float(self.tau[peer])) for peer in self.f.keys()]
     
            # sort by return on investment
            random.shuffle(ratios)
            ratios.sort(key=lambda p: p[1], reverse=True)

            # select top peers
            request_ids = [request.requester_id for request in requests]
            sum_tau = 0
            for peer_id, ratio in ratios:
                if peer_id in request_ids:
                    if sum_tau + self.tau[peer_id] <= self.state["cap"]:
                        # room to add peer
                        chosen += [peer_id]
                        bws += [self.tau[peer_id]]
                        sum_tau += self.tau[peer_id]
                    else:
                        # hit cap
                        break

        self.prev_unchoked = chosen

        # create actual uploads out of the list of peer ids and bandwidths
        uploads = [Upload(self.id, peer_id, bw) for (peer_id, bw) in zip(chosen, bws)]

        return uploads
