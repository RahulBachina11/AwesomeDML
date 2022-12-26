from datetime import datetime
from typing import List
from nodes import Node
import time
from nodes import Node
from globalClass import Global
import asyncio

import logging

from config import CLEANUP_TIME, M, GLOBAL_RING_TOPOLOGY, Config


class MemberShipList:
    """Class to maintain Local membership list"""

    def __init__(self, node: Node, ping_nodes: List[Node], globalObj: Global):
        self.globalObj = globalObj
        self.memberShipListDict = {}
        self.itself: Node = node
        self.current_pinging_nodes: List[Node] = ping_nodes
        self._nodes_cleaned = set()
        self.false_positives = 0
        self.indirect_failures = 0

    def _cleanup(self):
        """Check and clean up the elapsed members from membershiplist"""
        keys_for_cleanup = []
        for key in self.memberShipListDict.keys():
            node_curr_time, node_curr_status = self.memberShipListDict[key]

            if not node_curr_status:
                if (time.time() - node_curr_time) >= CLEANUP_TIME:
                    logging.error(
                        f'cleanup timeout elapsed removing from membershiplist: {key}')
                    keys_for_cleanup.append(key)

        for key_for_cleanup in keys_for_cleanup:
            if self.globalObj.worker.leaderNode != None:
                print(key_for_cleanup, self.globalObj.worker.leaderNode.unique_name)
                if key_for_cleanup == self.globalObj.worker.leaderNode.unique_name and not self.globalObj.election.electionPhase:
                    # logging.error('I should start the election')
                    self.globalObj.election.initiate_election()

            del self.memberShipListDict[key_for_cleanup]
            asyncio.create_task(self.globalObj.worker.handle_failures_if_pending_status(key_for_cleanup))
            self._nodes_cleaned.add(key_for_cleanup)

        if len(self._nodes_cleaned) >= M:
            self.topology_change()
            asyncio.create_task(self.globalObj.worker.replicate_files())
            self._nodes_cleaned.clear()

        new_ping_nodes = []
        for ping_node in self.current_pinging_nodes:
            if ping_node.unique_name not in keys_for_cleanup:
                new_ping_nodes.append(ping_node)

        self.current_pinging_nodes = new_ping_nodes

    def _find_replacement_node(self, node, index, online_nodes, new_ping_nodes):
        """Finds the replacement node from local membershiplist"""
        if (node in online_nodes) and (node not in new_ping_nodes):
            return node

        return self._find_replacement_node(GLOBAL_RING_TOPOLOGY[node][index], index, online_nodes, new_ping_nodes)

    def get_online_nodes(self):
        online_nodes = [Config.get_node_from_unique_name(
            key) for key in self.memberShipListDict.keys()]

        return online_nodes

    def topology_change(self):
        """Change topology change"""
        if len(self.memberShipListDict) == 0:
            self.current_pinging_nodes = []
            return

        online_nodes = [Config.get_node_from_unique_name(
            key) for key in self.memberShipListDict.keys()]
        actual_ping_nodes = GLOBAL_RING_TOPOLOGY[self.itself]

        new_ping_nodes = []

        index = 0
        for node in actual_ping_nodes:
            if len(new_ping_nodes) < len(self.memberShipListDict):
                replacement_node = self._find_replacement_node(
                    node, index, online_nodes, new_ping_nodes)
                if (replacement_node not in new_ping_nodes) and not (replacement_node is self.itself):
                    new_ping_nodes.append(replacement_node)
            index += 1

        self.current_pinging_nodes = new_ping_nodes

    def get(self) -> dict:
        """Return current Local membership List"""
        self._cleanup()
        self.memberShipListDict[self.itself.unique_name] = (time.time(), 1)
        return self.memberShipListDict

    def update(self, new_membership_list: dict) -> None:
        """Merge new mebership List to current membership List"""
        isNewNodeAddedToList = False
        for key in new_membership_list.keys():
            new_time, new_status = new_membership_list[key]
            if key in self.memberShipListDict:
                if key == self.itself.unique_name:
                    continue
                curr_time, curr_status = self.memberShipListDict[key]
                if curr_time < new_time:
                    if curr_status == 0 and new_status == 1:
                        logging.info(f'unsuspecting {key}')
                        self.false_positives += 1
                    elif curr_status == 1 and new_status == 0:
                        logging.info(f'indirectly suspecting as failure {key}')
                        self.indirect_failures += 1
                    logging.debug(
                        f'updating {key} to {new_time}, {new_status}')
                    self.memberShipListDict[key] = (new_time, new_status)
            else:
                logging.info(f'new node added: {key}')
                self.memberShipListDict[key] = (new_time, new_status)

                if new_status:
                    isNewNodeAddedToList = True

        if isNewNodeAddedToList:
            self.topology_change()

    def update_node_status(self, node: Node, status: int) -> None:
        """Update node status in membership list"""
        if node.unique_name in self.memberShipListDict:
            _, curr_node_status = self.memberShipListDict[node.unique_name]
            if curr_node_status:
                logging.error(f'suspecting as failure: {node.unique_name}')
                self.memberShipListDict[node.unique_name] = (
                    time.time(), status)

    def print(self) -> None:
        """Prints current membership list"""
        items = list(self.memberShipListDict.items())
        items.sort()

        s = ""
        for key, value in items:
            s += f'{key} : {value}\n'
        logging.info(f"local membership list: {len(items)} \n{s}")

        s = ""
        for node in self.current_pinging_nodes:
            s += node.unique_name + "\n"
        logging.info(f"current ping nodes: \n{s}")
