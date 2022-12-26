

class Global:

    def __init__(self):
        pass

    def set_worker(self, workerObj):
        self.worker = workerObj

    def set_election(self, electionObj):
        self.election = electionObj
    
    # def set_membershiplist(self, membershipListObj):
    #     self.membershipList = membershipListObj
    
    def set_leader(self, leaderObj):
        self.leader = leaderObj