from nodes import Node
import hashlib
from random import random, seed
from config import Config
import fnmatch 

class Leader:

    def __init__(self, leaderNode : Node, globalObj):
        self.leaderNode = leaderNode
        self.globalObj = globalObj
        self.current_status = {}
        # self.global_file_dict: dict = {
        #     "127.0.0.1:8001": {'Q1.jpg': ['Q1.jpg_version2', 'Q1.jpg_version3', 'Q1.jpg_version4', 'Q1.jpg_version5', 'Q1.jpg_version6']},
        #     "127.0.0.1:8002": {'Q1.jpg': ['Q1.jpg_version2', 'Q1.jpg_version3', 'Q1.jpg_version4', 'Q1.jpg_version5', 'Q1.jpg_version6']},
        #     "127.0.0.1:8003": {'Q1.jpg': ['Q1.jpg_version2', 'Q1.jpg_version3', 'Q1.jpg_version4', 'Q1.jpg_version5', 'Q1.jpg_version6']}
        # }

        self.global_file_dict: dict = {}
        self.status_dict: dict = {}

    def delete_node_from_global_dict(self, node):
        del self.global_file_dict[node]
    
    def merge_files_in_global_dict(self, files_in_node, sender):

        self.global_file_dict[sender] = files_in_node

    def check_if_file_exists(self, sdfsFileName):

        for node in self.global_file_dict.keys():
            for file in self.global_file_dict[node].keys():
                if file == sdfsFileName:
                    return True
        
        return False
    
    def find_nodes_to_delete_file(self, sdfsFileName: str):
        nodes = []
        for node, node_file_dict in self.global_file_dict.items():
            if sdfsFileName in node_file_dict:
                nodes.append(Config.get_node_from_unique_name(node))
        return nodes

    def find_nodes_to_put_file(self, sdfsFileName: str):

        nodes = []
        if self.check_if_file_exists(sdfsFileName):
            for node in self.global_file_dict.keys():
                for file in self.global_file_dict[node].keys():
                    if file == sdfsFileName:
                        nodes.append(Config.get_node_from_unique_name(node))
                        break
            
        else:
            hashObj = hashlib.sha256(sdfsFileName.encode('utf-8'))
            val = int.from_bytes(hashObj.digest(), 'big')

            node_id_set = set()
            while len(node_id_set) < 4:
                val += int(random() * 100)
                id = (val % 10) + 1
                node = Config.get_node_from_id('H'+str(id)).unique_name
                if node in self.globalObj.worker.membership_list.memberShipListDict.keys():
                    node_id_set.add(id)
            
            for id in node_id_set:
                nodes.append(Config.get_node_from_id('H'+str(id)))

        return nodes

    def find_replica_nodes(self, sdfsFileName, num_replicas, current_replicas):
        nodes = []
        hashObj = hashlib.sha256(sdfsFileName.encode('utf-8'))
        val = int.from_bytes(hashObj.digest(), 'big')

        node_set = set()
        while len(node_set) < num_replicas:
            val += int(random() * 100)
            id = (val % 10) + 1
            node  = Config.get_node_from_id('H'+str(id)).unique_name
            if node not in current_replicas and node in self.globalObj.worker.membership_list.memberShipListDict.keys():
                node_set.add(node)
        
        return list(node_set)
    
    def is_file_upload_inprogress(self, sdfsFileName):
        return sdfsFileName in self.status_dict

    def get_machineids_for_file(self, sdfsFileName) -> list:
        machineids = []
        for machineid, machine_file_dict in self.global_file_dict.items():
            if sdfsFileName in machine_file_dict:
                machineids.append(machineid)
        return machineids
    
    def get_machineids_with_filenames(self, sdfsFileName) -> dict:
        machineids_filenames = {}
        for machineid, machine_file_dict in self.global_file_dict.items():
            if sdfsFileName in machine_file_dict:
                machineids_filenames[machineid] = machine_file_dict[sdfsFileName]
        return machineids_filenames
    
    def get_all_matching_files(self, pattern):

        matching_files = set()
        for _, machine_file_dict in self.global_file_dict.items():
            for sdfsFileName, _ in machine_file_dict.items():
                if fnmatch.fnmatch(sdfsFileName, pattern):
                    matching_files.add(sdfsFileName)
        return list(matching_files)

    def create_new_status_for_file(self, filename: str, filepath:str, requestingNode: Node, request_type: str):
        self.status_dict[filename] = {
            'request_type': request_type,
            'file_path': filepath,
            'request_node': requestingNode,
            'replicas': {}
        }

    def check_if_request_completed(self, filename):
        if filename in self.status_dict:
            for key, item in self.status_dict[filename]['replicas'].items():
                if item != 'Success':
                    return False
            return True
        return False
    
    def check_if_request_falied(self, filename):
        if filename in self.status_dict:
            for key, item in self.status_dict[filename]['replicas'].items():
                if item != 'Falied':
                    return False
            return True
        return False
    
    def update_replica_status(self, sdfsFileName:str, replicaNode: Node, status: str):
        if sdfsFileName in self.status_dict and replicaNode.unique_name in self.status_dict[sdfsFileName]['replicas']:
            self.status_dict[sdfsFileName]['replicas'][replicaNode.unique_name] = status
    
    def add_replica_to_file(self, sdfsFileName: str, replicaNode: Node):
        self.status_dict[sdfsFileName]['replicas'][replicaNode.unique_name] = 'Waiting'

    def delete_status_for_file(self, sdfsFileName: str):
        del self.status_dict[sdfsFileName]

    def find_files_for_replication(self):
        file_dict = {}
        
        for node in self.global_file_dict:
            for filename in self.global_file_dict[node]:
                if filename in file_dict.keys():
                    file_dict[filename].append(node)
                else:
                    file_dict[filename] = [node]
        
        replication_dict = {}

        for filename in file_dict:

            if filename not in replication_dict.keys():
                replication_dict[filename] = {}

            if len(file_dict[filename]) < 4:
                new_nodes = self.find_replica_nodes(filename, 4 - len(file_dict[filename]), file_dict[filename])

                for node in new_nodes:
                    print(node, filename)
                    if node not in replication_dict[filename].keys():
                        replication_dict[filename][node] = []

                    for current_alive_node in file_dict[filename]:
                        replication_obj = {
                            'hostname': Config.get_node_from_unique_name(current_alive_node).host,
                            'file_paths': self.global_file_dict[file_dict[filename][0]][filename],
                            'filename': filename,
                        }
                        
                        replication_dict[filename][node].append(replication_obj)
                    
        return replication_dict