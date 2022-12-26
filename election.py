from globalClass import Global
import asyncio
from config import Config, H2
import logging


class Election:


    def __init__(self, globalObj: Global):
        self.globalObj = globalObj
        self.highestElectionID = None
        self.electionPhase = False
        self.coordinate_ack = 0

    def initiate_election(self):
        """function to initiate the election phase"""
        self.electionPhase = True
        self.globalObj.worker.leaderNode = None
        logging.info(f'ELECTION INITIATED by {self.globalObj.worker.config.node.unique_name}')
        # while self.electionPhase:
        #     asyncio.gather(self.globalObj.worker.send_election_messages())
    
    def check_if_leader(self):
        """Function to check if the current node has the highest ID"""
        node = self.globalObj.worker.config.node
        if node.unique_name != H2.unique_name:
            return False
        
        self.electionPhase = False
        self.coordinate_ack = 0
        return True
            