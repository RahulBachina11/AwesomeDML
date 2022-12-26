from datetime import datetime
import logging
import sys
import asyncio
from asyncio import Event, exceptions
from time import time
from weakref import WeakSet, WeakKeyDictionary
from typing import final, Final, NoReturn, Optional
from config import GLOBAL_RING_TOPOLOGY, Config, PING_TIMEOOUT, PING_DURATION, USERNAME, PASSWORD, TEST_FILES_PATH, DOWNLOAD_PATH, H1, H2, H3, H4, H5, H6, H7, H8, H9, H10
from nodes import Node
from packets import Packet, PacketType
from protocol import AwesomeProtocol
from membershipList import MemberShipList
from leader import Leader
from globalClass import Global
from election import Election
from file_service import FileService
from os import path
import copy
from typing import List
import random
import os
from ast import literal_eval
from models import perform_inference, NpEncoder, Merge, dump_to_file, perform_inference_without_async, ModelParameters
import json
import fnmatch
import statistics

class Worker:
    """Main worker class to handle all the failure detection and sends PINGs and ACKs to other nodes"""
    def __init__(self, io: AwesomeProtocol) -> None:
        self.io: Final = io
        self._waiting: WeakKeyDictionary[Node, WeakSet[Event]] = WeakKeyDictionary()
        self.config: Config = None
        self.membership_list: Optional[MemberShipList] = None
        self.is_current_node_active = True
        self.waiting_for_introduction = True
        self.total_pings_send = 0
        self.total_ack_missed = 0
        self.missed_acks_count = {}
        self.file_service = FileService()
        self.file_status = {}
        self._waiting_for_leader_event: Optional[Event] = None
        self._waiting_for_second_leader_event: Optional[Event] = None
        self.get_file_sdfsfilename = None
        self.get_file_machineids_with_file_versions = None
        self.job_count = 30
        self.current_job_id = 0
        self.job_reqester_dict = {}
        self.job_task = None

        self.worker_nodes = [H3.unique_name, H4.unique_name, H5.unique_name, H6.unique_name, H7.unique_name, H8.unique_name, H9.unique_name, H10.unique_name]

        self.workers_tasks_dict = {
        }

        self.model_dict = {
            "InceptionV3": {
                'hyperparams' : {
                    'batch_size' : 10, 
                    'time': ModelParameters(download_time=1, model_load_time=5.6, first_image_predict_time=2, each_image_predict_time=0.325, batch_size=10).execution_time_per_vm()
                    },
                'queue': [],
                'inprogress_queue': [],
                'measurements' : {
                    'query_count': 0,
                    'query_rate_list' : [],
                    'query_rate_array': []
                }
            },
            "ResNet50": {
                'hyperparams' : {
                    'batch_size' : 10, 
                    'time': ModelParameters(download_time=1, model_load_time=3.5, first_image_predict_time=1, each_image_predict_time=0.250, batch_size=10).execution_time_per_vm()
                    },
                'queue': [],
                'inprogress_queue': [],
                'measurements' : {
                    'query_count': 0,
                    'query_rate_list' : [],
                    'query_rate_array': []
                }
            }
        }
    
    def load_model_parameters(self, batch_size):
        self.batch_size = batch_size
        self.inceptionV3_model_params = ModelParameters(download_time=1, model_load_time=5.6, first_image_predict_time=2, each_image_predict_time=0.325, batch_size=self.batch_size)
        self.resNet50_model_params = ModelParameters(download_time=1, model_load_time=3.5, first_image_predict_time=1, each_image_predict_time=0.250, batch_size=self.batch_size)

    def initialize(self, config: Config, globalObj: Global) -> None:
        """Function to initialize all the required class for Worker"""
        self.config = config
        globalObj.set_worker(self)
        self.globalObj = globalObj
        self.globalObj.set_election(Election(globalObj))
        # self.waiting_for_introduction = False if self.config.introducerFlag else True
        self.leaderFlag = False
        self.leaderObj: Leader = None
        self.leaderNode: Node= None
        self.fetchingIntroducerFlag = True
        self.temporary_file_dict = {}
        # if self.config.introducerFlag:
        #     self.leaderObj = Leader(self.config.node)
        #     self.leaderFlag = True
        #     self.leaderNode = self.config.node.unique_name

        self.membership_list = MemberShipList(
            self.config.node, self.config.ping_nodes, globalObj)
        
        self.io.testing = config.testing
    
    async def replica_file(self, req_node: Node, replicas: List[dict]):
        """Function to replicate files from other nodes. This function initiates a scp command to transfer the files and is run when leader sends the REPLICATE packet"""
        status = False
        filename = ""
        for replica in replicas:
            host = replica["hostname"]
            username = USERNAME
            password = PASSWORD
            file_locations = replica["file_paths"]
            filename = replica["filename"]
            status = await self.file_service.replicate_file(host=host, username=username, password=password, file_locations=file_locations, filename=filename)
            if status:
                logging.info(f'successfully replicated file {filename} from {host} requested by {req_node.unique_name}')
                response = {"filename": filename, "all_files": self.file_service.current_files}
                await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.REPLICATE_FILE_SUCCESS, response).pack())
                break
            else:
                logging.error(f'failed to replicate file {filename} from {host} requested by {req_node.unique_name}')
        
        if not status:
            response = {"filename": filename, "all_files": self.file_service.current_files}
            await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.REPLICATE_FILE_FAIL, response).pack())

    async def put_file(self, req_node: Node, host, username, password, file_location, filename):
        """Function to download file from the client node. This function initiates a scp command to transfer the files and is run when leader sends the DOWNLOAD packet"""
        status = await self.file_service.download_file(host=host, username=username, password=password, file_location=file_location, filename=filename)
        if status:
            logging.info(f'successfully downloaded file {file_location} from {host} requested by {req_node.unique_name}')
            # download success sending sucess back to requester
            response = {"filename": filename, "all_files": self.file_service.current_files}
            await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.DOWNLOAD_FILE_SUCCESS, response).pack())
        else:
            logging.error(f'failed to download file {file_location} from {host} requested by {req_node.unique_name}')
            # download failed sending failure message back to requester
            response = {"filename": filename, "all_files": self.file_service.current_files}
            await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.DOWNLOAD_FILE_FAIL, response).pack())
    
    async def delete_file(self, req_node, filename):
        """Function to delete file on the node. Itis run when leader sends the DELETE packet"""

        logging.debug(f"request from {req_node.host}:{req_node.port} to delete file {filename}")
        status = self.file_service.delete_file(filename)
        if status:
            logging.info(f"successfully deleted file {filename}")
            response = {"filename": filename, "all_files": self.file_service.current_files}
            await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE_ACK, response).pack())
        else:
            logging.error(f"failed to delete file {filename}")
            response = {"filename": filename, "all_files": self.file_service.current_files}
            await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE_NAK, response).pack())

    async def get_file(self, req_node, filename):
        """Function to get files from other nodes. This function initiates a scp command to transfer the files and is run on the client once the leader sends it info of the replicas"""

        logging.debug(f"request from {req_node.host}:{req_node.port} to get file {filename}")
        response = self.file_service.get_file_details(filename)
        if "latest_file" in response:
            logging.error(f"Failed to find the file {filename}")
            await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.GET_FILE_NAK, response).pack())
        else:
            logging.info(f"Found file {filename} locally")
            await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.GET_FILE_ACK, response).pack())
    
    async def handle_job_request(self, req_node, model, number_of_images, job_id):
        if self.leaderFlag: 
            sdfs_images = await self.ls_all_cli("*.jpeg")
        else:
            sdfs_images = self.ls_all_temp_dict("*.jpeg")
        
        sdfs_images.sort()

        self.preprocess_job_request(req_node, model, number_of_images, job_id, sdfs_images)
        if self.leaderFlag:
            await self.schedule_job()

    def preprocess_job_request(self, req_node, model, number_of_images, job_id, sdfs_images):

        logging.info(f"JOB#{job_id} request from {req_node.host}:{req_node.port} for {model} to run inference on {number_of_images} files")

        if len(sdfs_images) == 0:
            return

        # images = random.sample(sdfs_images, number_of_images)
        images = []

        index = 0
        while len(images) < number_of_images:

            if index >= len(sdfs_images):
                index = 0
            
            images.append(sdfs_images[index])

            index += 1
        
        # batch images
        batch_size = self.model_dict[model]["hyperparams"]["batch_size"]

        all_batches = []

        batch = []
        # batch_id = 1
        for image in images:
            if len(batch) < batch_size:
                batch.append(image)
            else:
                batch.append(image)
                batch_dict = {
                    "job_id": job_id,
                    "batch_id": len(all_batches) + 1,
                    "images": batch
                }
                all_batches.append(batch_dict)
                # batch_id += 1
                batch = []
        
        if len(batch):
            # batch_id += 1
            batch_dict = {
                "job_id": job_id,
                "batch_id": len(all_batches) + 1,
                "images": batch
            }
            all_batches.append(batch_dict)
            batch = []
        
        for all_batch in all_batches:
            self.model_dict[model]["queue"].append(all_batch)
        
        self.job_reqester_dict[job_id] = {
            "request_node": req_node,
            "num_of_batches_pending": len(all_batches)
        }
    
    def get_running_nodes(self, model):
        result = []
        for worker in self.workers_tasks_dict:
            if(self.workers_tasks_dict[worker]['model'] == model):
                result.append(worker)
        return result


    async def schedule_job(self):

        if (len(self.model_dict["InceptionV3"]["queue"]) != 0 and len(self.model_dict["ResNet50"]["queue"]) == 0) or (len(self.model_dict["InceptionV3"]["queue"]) == 0 and len(self.model_dict["ResNet50"]["queue"]) != 0):

            model = "InceptionV3"
            if len(self.model_dict["InceptionV3"]["queue"]) == 0 and len(self.model_dict["ResNet50"]["queue"]) != 0:
                model = "ResNet50"
            
            free_workers = list(set(list(self.membership_list.memberShipListDict.keys())).intersection(set(self.worker_nodes)) - set(list(self.workers_tasks_dict.keys())))

            if len(free_workers) == 0:
                logging.info(f"All the workers are busy will schedule if any worker becomes available")
                return
            
            count = 0
            for worker in free_workers:

                if len(self.model_dict[model]["queue"]) == 0:
                    break
                
                single_batch_dict = self.model_dict[model]["queue"][0]

                single_batch_jobid = single_batch_dict["job_id"]
                single_batch_id = single_batch_dict["batch_id"]
                images = single_batch_dict["images"]

                self.workers_tasks_dict[worker] = {
                    'model': model,
                    'job_id': single_batch_jobid,
                    'batch_id': single_batch_id
                }

                self.model_dict[model]["inprogress_queue"].append(single_batch_dict)
                self.model_dict[model]["queue"].pop(0)

                result_dict = {}
                for image in images:
                    machineids_with_filenames = self.leaderObj.get_machineids_with_filenames(image)
                    result_dict[image] = machineids_with_filenames

                # forward the request to VMs
                workernode = Config.get_node_from_unique_name(worker)
                await self.io.send(workernode.host, workernode.port, Packet(self.config.node.unique_name, PacketType.WORKER_TASK_REQUEST, {"jobid": single_batch_jobid, "batchid": single_batch_id, "model": model, "images": result_dict}).pack())
                count += 1
            
            logging.info(f"scheduling {count} tasks for {model}")

        
        elif len(self.model_dict["InceptionV3"]["queue"]) != 0 and len(self.model_dict["ResNet50"]["queue"]) != 0:

            online_worker_node_count = len(set(list(self.membership_list.memberShipListDict.keys())) - {H1.unique_name, H2.unique_name})
            temp = 1
            split_job_array = []
            while(online_worker_node_count > 1):
                split_job_array.append([online_worker_node_count - 1, temp])
                online_worker_node_count-=1
                temp+=1
            
            differences = []
            for split in split_job_array:
                inception_vmcount, resnet_vmcount = split
                inception_query_rate = (inception_vmcount * self.model_dict['InceptionV3']['hyperparams']['batch_size']) / self.model_dict['InceptionV3']['hyperparams']['time']
                resnet50_query_rate = (resnet_vmcount * self.model_dict['ResNet50']['hyperparams']['batch_size']) / self.model_dict['ResNet50']['hyperparams']['time']

                difference = (abs(inception_query_rate - resnet50_query_rate) / max(inception_query_rate, resnet50_query_rate)) * 100
                differences.append(difference)

            min_index = differences.index(min(differences))
            inception_vmcount_predicted, resnet_vmcount_predicted = split_job_array[min_index]
            logging.info(f"predicted query_differences: {differences}, final split: {split_job_array[min_index]}")

            inceptionV3_running_jobs = self.get_running_nodes('InceptionV3')
            resnet50_running_jobs = self.get_running_nodes('ResNet50')
            free_workers = list(set(list(self.membership_list.memberShipListDict.keys())).intersection(set(self.worker_nodes)) - set(list(self.workers_tasks_dict.keys())))

            new_nodes_for_inceptionV3 = []
            if len(inceptionV3_running_jobs) < inception_vmcount_predicted:
                
                while len(free_workers) > 0:
                    free_worker = free_workers[-1]
                    if len(new_nodes_for_inceptionV3) + len(inceptionV3_running_jobs) < inception_vmcount_predicted:
                        new_nodes_for_inceptionV3.append(free_worker)
                    else:
                        break
                    free_workers.pop()
                
                if len(new_nodes_for_inceptionV3) + len(inceptionV3_running_jobs) < inception_vmcount_predicted:
                    # assign that from resNet VMs
                    while len(resnet50_running_jobs) > 0:
                        free_worker = resnet50_running_jobs[-1]
                        if len(new_nodes_for_inceptionV3) + len(inceptionV3_running_jobs) < inception_vmcount_predicted:
                            new_nodes_for_inceptionV3.append(free_worker)
                        else:
                            break
                        resnet50_running_jobs.pop()
            
            new_nodes_for_resnet50 = []
            if len(resnet50_running_jobs) < resnet_vmcount_predicted:
                
                while len(free_workers) > 0:
                    free_worker = free_workers[-1]
                    if len(new_nodes_for_resnet50) + len(resnet50_running_jobs) < resnet_vmcount_predicted:
                        new_nodes_for_resnet50.append(free_worker)
                    else:
                        break
                    free_workers.pop()
                
                if len(new_nodes_for_resnet50) + len(resnet50_running_jobs) < resnet_vmcount_predicted:
                    # assign that from inceptionV3 VMs
                    while len(inceptionV3_running_jobs) > 0:
                        free_worker = inceptionV3_running_jobs[-1]
                        if len(new_nodes_for_resnet50) + len(resnet50_running_jobs) < resnet_vmcount_predicted:
                            new_nodes_for_resnet50.append(free_worker)
                        else:
                            break
                        inceptionV3_running_jobs.pop()
            
            logging.info(f"New nodes for resNet50={new_nodes_for_resnet50 + resnet50_running_jobs}, New nodes for inceptionV3={new_nodes_for_inceptionV3 + inceptionV3_running_jobs}")

            nodes_for_resnet50 = new_nodes_for_resnet50
            nodes_for_inceptionv3 = new_nodes_for_inceptionV3

            model = 'ResNet50'
            for worker in nodes_for_resnet50:

                if len(self.model_dict[model]["queue"]) == 0:
                    break
                
                single_batch_dict = self.model_dict[model]["queue"][0]

                single_batch_jobid = single_batch_dict["job_id"]
                single_batch_id = single_batch_dict["batch_id"]
                images = single_batch_dict["images"]

                if worker in self.workers_tasks_dict:

                    # add the batch_dict to infront of the queue
                    batch_dict = self.workers_tasks_dict[worker]
                    batch_jobid = batch_dict['job_id']
                    batch_id = batch_dict['batch_id']
                    batch_dict_model = batch_dict["model"]

                    i = 0
                    for item in self.model_dict[batch_dict_model]['inprogress_queue']:
                        item_jobid = item["job_id"]
                        item_batchid = item["batch_id"]
                        if item_jobid == batch_jobid and item_batchid == batch_id:
                            break
                        
                        i+= 1

                    logging.info(f'preempting job {batch_jobid} of {batch_dict_model} from {worker}')
                    preempted_batch = self.model_dict[batch_dict_model]['inprogress_queue'].pop(i)
                    self.model_dict[batch_dict_model]["queue"].insert(0, preempted_batch)

                self.workers_tasks_dict[worker] = {
                    'model': model,
                    'job_id': single_batch_jobid,
                    'batch_id': single_batch_id
                }

                self.model_dict[model]["inprogress_queue"].append(single_batch_dict)
                self.model_dict[model]["queue"].pop(0)

                result_dict = {}
                for image in images:
                    machineids_with_filenames = self.leaderObj.get_machineids_with_filenames(image)
                    result_dict[image] = machineids_with_filenames

                # forward the request to VMs
                workernode = Config.get_node_from_unique_name(worker)
                logging.info(f'Assigning a new job {model} to {workernode.unique_name}')

                await self.io.send(workernode.host, workernode.port, Packet(self.config.node.unique_name, PacketType.WORKER_TASK_REQUEST, {"jobid": single_batch_jobid, "batchid": single_batch_id, "model": model, "images": result_dict}).pack())

            model = 'InceptionV3'
            for worker in nodes_for_inceptionv3:

                if len(self.model_dict[model]["queue"]) == 0:
                    break
                
                single_batch_dict = self.model_dict[model]["queue"][0]

                single_batch_jobid = single_batch_dict["job_id"]
                single_batch_id = single_batch_dict["batch_id"]
                images = single_batch_dict["images"]

                if worker in self.workers_tasks_dict:

                    # add the batch_dict to infront of the queue
                    batch_dict = self.workers_tasks_dict[worker]
                    batch_jobid = batch_dict['job_id']
                    batch_id = batch_dict['batch_id']
                    batch_dict_model = batch_dict["model"]

                    i = 0
                    for item in self.model_dict[batch_dict_model]['inprogress_queue']:
                        item_jobid = item["job_id"]
                        item_batchid = item["batch_id"]
                        if item_jobid == batch_jobid and item_batchid == batch_id:
                            break
                        
                        i+= 1

                    logging.info(f'preempting job {batch_jobid} of {batch_dict_model} from {worker}')
                    preempted_batch = self.model_dict[batch_dict_model]['inprogress_queue'].pop(i)
                    self.model_dict[batch_dict_model]["queue"].insert(0, preempted_batch)

                self.workers_tasks_dict[worker] = {
                    'model': model,
                    'job_id': single_batch_jobid,
                    'batch_id': single_batch_id
                }

                self.model_dict[model]["inprogress_queue"].append(single_batch_dict)
                self.model_dict[model]["queue"].pop(0)

                result_dict = {}
                for image in images:
                    machineids_with_filenames = self.leaderObj.get_machineids_with_filenames(image)
                    result_dict[image] = machineids_with_filenames

                # forward the request to VMs
                workernode = Config.get_node_from_unique_name(worker)
                logging.info(f'Assigning a new job {model} to {workernode.unique_name}')
                await self.io.send(workernode.host, workernode.port, Packet(self.config.node.unique_name, PacketType.WORKER_TASK_REQUEST, {"jobid": single_batch_jobid, "batchid": single_batch_id, "model": model, "images": result_dict}).pack())

        else:
            return
        
        model = 'ResNet50'
        model_running_list = self.get_running_nodes(model)
        if len(model_running_list):
            query_rate = len(model_running_list) * self.model_dict[model]["hyperparams"]["batch_size"]
            self.model_dict[model]["measurements"]["query_rate_array"].append((time(), query_rate))

        model = 'InceptionV3'
        model_running_list = self.get_running_nodes(model)
        if len(model_running_list):
            query_rate = len(model_running_list) * self.model_dict[model]["hyperparams"]["batch_size"]
            self.model_dict[model]["measurements"]["query_rate_array"].append((time(), query_rate))

    def display_machineids_for_file(self, sdfsfilename, machineids):
        """Function to pretty print replica info for the LS command"""
        output = f"File {sdfsfilename} found in {len(machineids)} machines:\n"
        for recv_machineid in machineids:
            output += f"{recv_machineid}\n"
        print(output)
    
    def _add_waiting(self, node: Node, event: Event) -> None:
        """Function to keep track of all the unresponsive PINGs"""
        waiting = self._waiting.get(node)
        if waiting is None:
            self._waiting[node] = waiting = WeakSet()
        waiting.add(event)

    def _notify_waiting(self, node) -> None:
        """Notify the PINGs which are waiting for ACKs"""
        waiting = self._waiting.get(node)
        if waiting is not None:
            for event in waiting:
                event.set()
    
    async def handle_worker_task_request(self, curr_node, model, req_images, jobid, batchid, start_time):
        try:
            logging.info(f"received a Task from cordinator: JobId={jobid}, model={model}, images_count={len(req_images)}")
            # perform prediction
            filename = await self.predict_locally_cli(model, req_images, jobid, batchid)
            # upload it to SDFS
            logging.info(f"JOB#{jobid}: uploading result file:{filename} to SDFS")
            await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.PUT_REQUEST, {'file_path': DOWNLOAD_PATH + filename, 'filename': filename}).pack())
            
            await asyncio.sleep(0)

            # send response to cordinator
            logging.info(f"ACK for JOB#{jobid} to {curr_node.unique_name}")
            await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.WORKER_TASK_REQUEST_ACK, {'jobid': jobid, "batchid": batchid, "model": model, 'image_count': len(req_images), 'start_time': start_time}).pack())
            # del self.job_task_dict[jobid]
            self.job_task = None
        except asyncio.CancelledError as e:
            logging.info(f"Stopping the JOB#{jobid}")
        finally:
            logging.info(f"Task JOB#{jobid} cancelled")

    async def _run_handler(self) -> NoReturn:
        """RUN the main loop which handles all the communication to external nodes"""
        
        while True:
            packedPacket, host, port = await self.io.recv()

            packet: Packet = Packet.unpack(packedPacket)
            if (not packet) or (not self.is_current_node_active):
                continue

            logging.debug(f'got data: {packet.data} from {host}:{port}')

            if packet.type == PacketType.ACK or packet.type == PacketType.INTRODUCE_ACK:
                """Instructions to execute when the node receives failure detector ACKs or ACKs from the introducer"""

                # print('I GOT AN ACK FROM ', packet.sender)
                curr_node: Node = Config.get_node_from_unique_name(
                    packet.sender)
                logging.debug(f'got ack from {curr_node.unique_name}')
                if curr_node:

                    if packet.type == PacketType.ACK:
                        self.membership_list.update(packet.data)
                    else:
                        self.membership_list.update(packet.data['membership_list'])
                        leader = packet.data['leader']
                        self.leaderNode = Config.get_node_from_unique_name(leader)

                    self.waiting_for_introduction = False
                    # self.membership_list.update(packet.data)
                    self.missed_acks_count[curr_node] = 0
                    self._notify_waiting(curr_node)

            elif packet.type == PacketType.FETCH_INTRODUCER_ACK:
                """Instructions to execute once the node receives the introducer information from the Introducer DNS process"""
                logging.debug(f'got fetch introducer ack from {self.config.introducerDNSNode.unique_name}')
                introducer = packet.data['introducer']
                print(introducer)
                if introducer == self.config.node.unique_name:
                    self.leaderObj = Leader(self.config.node, self.globalObj)
                    self.globalObj.set_leader(self.leaderObj)
                    self.leaderFlag = True
                    self.leaderNode = self.config.node
                    self.waiting_for_introduction = False
                    self.leaderObj.global_file_dict = copy.deepcopy(self.temporary_file_dict)
                    self.leaderObj.global_file_dict[self.config.node.unique_name] = self.file_service.current_files
                    self.temporary_file_dict = {}
                    logging.info(f"I BECAME THE LEADER {self.leaderNode.unique_name}")
                    if H2.unique_name == self.config.node.unique_name:
                        await self.schedule_job()
                else:
                    self.leaderNode = Config.get_node_from_unique_name(introducer)
                    logging.info(f"MY NEW LEADER IS {self.leaderNode.unique_name}")
                    response = {'all_files': self.file_service.current_files}
                    await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.ALL_LOCAL_FILES, response).pack())

                self.fetchingIntroducerFlag = False
                self._notify_waiting(self.config.introducerDNSNode)
            
            elif packet.type == PacketType.ALL_LOCAL_FILES:
                files_in_node = packet.data['all_files']
               
                if isinstance(self.leaderObj, Leader):
                    self.leaderObj.merge_files_in_global_dict(files_in_node, packet.sender)
                
                if H1.unique_name == self.leaderNode.unique_name:
                    await self.io.send(H2.host, H2.port, Packet(self.config.node.unique_name, PacketType.ALL_LOCAL_FILES_RELAY, {"all_files": files_in_node, 'node': packet.sender, 'leader_files': self.file_service.current_files}).pack())


            elif packet.type == PacketType.ALL_LOCAL_FILES_RELAY:
                files_in_node = packet.data['all_files']
                sender_node = packet.data['node']
                leader_files = packet.data['leader_files']

                self.temporary_file_dict[sender_node] = files_in_node
                self.temporary_file_dict[H1.unique_name] = leader_files

            elif packet.type == PacketType.PING or packet.type == PacketType.INTRODUCE:
                # print(f'{datetime.now()}: received ping from {host}:{port}')
                self.membership_list.update(packet.data)
                await self.io.send(host, port, Packet(self.config.node.unique_name, PacketType.ACK, self.membership_list.get()).pack())

            elif packet.type == PacketType.ELECTION:
                """Instructions to execute when node receives the election packet. If it has not started its own election then its starts it. If election is already going on, it checks if it itself is the leader using the full membership list. If new leader, then send the coordinate message"""

                logging.info(f'{self.config.node.unique_name}  GOT AN ELECTION PACKET')
                if not self.globalObj.election.electionPhase:
                    self.globalObj.election.initiate_election()
                else:
                    if self.globalObj.election.check_if_leader():
                        await self.send_coordinator_message()
            
            elif packet.type == PacketType.COORDINATE:
                """Instructions to execute when a node receives the COORDINATE message from the new leader"""
                self.globalObj.election.electionPhase = False
                self.leaderNode = Config.get_node_from_unique_name(packet.sender)
                logging.info(f'{self.config.node.unique_name} NEW LEADER IS f{packet.sender}')
                response = {'all_files': self.file_service.current_files}
                await self.io.send(host, port, Packet(self.config.node.unique_name, PacketType.COORDINATE_ACK, response).pack())
            
            elif packet.type == PacketType.COORDINATE_ACK:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                files_in_node = packet.data['all_files']
                self.globalObj.election.coordinate_ack += 1
                self.temporary_file_dict[packet.sender] = files_in_node
                # self.leaderObj.merge_files_in_global_dict(files_in_node, host, port)

                print(f"Got COORDINATE_ACK from {curr_node.unique_name} and my members: {list(self.membership_list.memberShipListDict.keys())}")
                # if self.globalObj.election.coordinate_ack == len(self.membership_list.memberShipListDict.keys()) - 1:
                logging.info(f'{self.config.node.unique_name} IS THE NEW LEADER NOW')
                await self.update_introducer()
            
            elif packet.type == PacketType.REPLICATE_FILE:
                """Handle REPLICATE request from the leader"""
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    replicas = data["replicas"]
                    logging.debug(f"request from {curr_node.host}:{curr_node.port} to replicate files")
                    asyncio.create_task(self.replica_file(req_node=curr_node, replicas=replicas))
            
            elif packet.type == PacketType.REPLICATE_FILE_SUCCESS:
                """Handle the Replicate success messages from the replicas. This request is only handled by the leader"""
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    sdfsFileName = data['filename']
                    all_files = data['all_files']
                    # update status dict
                    self.leaderObj.merge_files_in_global_dict(all_files, packet.sender)
                    self.leaderObj.update_replica_status(sdfsFileName, curr_node, 'Success')
                    print(f'{packet.sender} REPLICATED {sdfsFileName}')
                    if self.leaderObj.check_if_request_completed(sdfsFileName):
                        self.leaderObj.delete_status_for_file(sdfsFileName)
                        print(f'REPLICATED {sdfsFileName} at all nodes: in {time() - self.replicate_start_time} seconds')

            elif packet.type == PacketType.REPLICATE_FILE_FAIL:

                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    sdfsFileName = data['filename']
                    all_files = data['all_files']
                    # update status dict
                    self.leaderObj.merge_files_in_global_dict(all_files, packet.sender)
                    self.leaderObj.update_replica_status(sdfsFileName, curr_node, 'Failed')
                    print(f'{packet.sender} NOT REPLICATED {sdfsFileName}')
                    # self.leaderObj.delete_status_for_file(sdfsFileName)

            elif packet.type == PacketType.DOWNLOAD_FILE:
                # parse packet and get all the required fields to download a file
                """Handle Download request from the leader"""
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    machine_hostname = data["hostname"]
                    machine_username = USERNAME
                    machine_password = PASSWORD
                    machine_file_location = data["file_path"]
                    machine_filename = data["filename"]
                    logging.debug(f"request from {curr_node.host}:{curr_node.port} to download file from {machine_username}@{machine_hostname}:{machine_file_location}")
                    asyncio.create_task(self.put_file(curr_node, machine_hostname, machine_username, machine_password, machine_file_location, machine_filename))

            elif packet.type == PacketType.DOWNLOAD_FILE_SUCCESS:
                """INstructions for the leader to handle the download file success messages. The leader updates the status of the file in and notifies the client that the file has been uploaded successfully if Success is received from all 4 replicas"""

                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    sdfsFileName = data['filename']
                    all_files = data['all_files']
                    # update status dict
                    self.leaderObj.merge_files_in_global_dict(all_files, packet.sender)
                    self.leaderObj.update_replica_status(sdfsFileName, curr_node, 'Success')
                    if self.leaderObj.check_if_request_completed(sdfsFileName):
                        original_requesting_node = self.leaderObj.status_dict[sdfsFileName]['request_node']
                        await self.io.send(original_requesting_node.host, original_requesting_node.port, Packet(self.config.node.unique_name, PacketType.PUT_REQUEST_SUCCESS, {'filename': sdfsFileName}).pack())
                        self.leaderObj.delete_status_for_file(sdfsFileName)
            
            elif packet.type == PacketType.DOWNLOAD_FILE_FAIL:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    sdfsFileName = data['filename']
                    all_files = data['all_files']
                    # update status dict
                    self.leaderObj.merge_files_in_global_dict(all_files, packet.sender)
                    self.leaderObj.update_replica_status(sdfsFileName, curr_node, 'Failed')
                    if self.leaderObj.check_if_request_falied(sdfsFileName):
                        original_requesting_node = self.leaderObj.status_dict[sdfsFileName]['request_node']
                        await self.io.send(original_requesting_node.host, original_requesting_node.port, Packet(self.config.node.unique_name, PacketType.PUT_REQUEST_FAIL, {'filename': sdfsFileName}).pack())
                        del self.leaderObj.status_dict[sdfsFileName]
            
            elif packet.type == PacketType.DELETE_FILE:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    machine_filename = data["filename"]
                    await self.delete_file(curr_node, machine_filename)

            elif packet.type == PacketType.DELETE_FILE_ACK or packet.type == PacketType.DELETE_FILE_NAK:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    sdfsFileName = data['filename']
                    all_files = data['all_files']
                    # update status dict
                    self.leaderObj.merge_files_in_global_dict(all_files, packet.sender)
                    self.leaderObj.update_replica_status(sdfsFileName, curr_node, 'Success')
                    if self.leaderObj.check_if_request_completed(sdfsFileName):
                        original_requesting_node = self.leaderObj.status_dict[sdfsFileName]['request_node']
                        await self.io.send(original_requesting_node.host, original_requesting_node.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE_REQUEST_SUCCESS, {'filename': sdfsFileName}).pack())
                        self.leaderObj.delete_status_for_file(sdfsFileName)

            elif packet.type == PacketType.GET_FILE:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    data: dict = packet.data
                    machine_filename = data["filename"]
                    self.get_file(curr_node, machine_filename)

            elif packet.type == PacketType.PUT_REQUEST:
                """Instructions to handle the PUT request from the client. This request is handled by the leader. It finds 4 random replicas for the file, sends download command to them and tracks their status"""
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    sdfsFileName = packet.data['filename']
                    if self.leaderObj.is_file_upload_inprogress(sdfsFileName):
                        await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.PUT_REQUEST_FAIL, {'filename': sdfsFileName, 'error': 'File upload already inprogress...'}).pack())
                    else:
                        download_nodes = self.leaderObj.find_nodes_to_put_file(sdfsFileName)
                        self.leaderObj.create_new_status_for_file(sdfsFileName, packet.data['file_path'], curr_node, 'PUT')
                        for node in download_nodes:
                            await self.io.send(node.host, node.port, Packet(self.config.node.unique_name, PacketType.DOWNLOAD_FILE, {'hostname': host, 'file_path': packet.data['file_path'], 'filename': sdfsFileName}).pack())
                            self.leaderObj.add_replica_to_file(sdfsFileName, node)
                        await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.PUT_REQUEST_ACK, {'filename': sdfsFileName}).pack())

            elif packet.type == PacketType.DELETE_FILE_REQUEST:
                """Instructions for the leader to handle the delete request from the client. It finds the replicas for that file and sends the delete command to them It also tracks their status"""
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    sdfsFileName = packet.data['filename']
                    if self.leaderObj.is_file_upload_inprogress(sdfsFileName):
                        await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE_REQUEST_FAIL, {'error': "File upload inprogress"}).pack())
                    else:
                        file_nodes = self.leaderObj.find_nodes_to_delete_file(sdfsFileName)
                        if len(file_nodes) == 0:
                            await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE_REQUEST_SUCCESS, {'filename': sdfsFileName}).pack())
                        else:
                            self.leaderObj.create_new_status_for_file(sdfsFileName, '', curr_node, 'DELETE')
                            for node in file_nodes:
                                await self.io.send(node.host, node.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE, {'filename': sdfsFileName}).pack())
                                self.leaderObj.add_replica_to_file(sdfsFileName, node)
                            await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE_REQUEST_ACK, {'filename': sdfsFileName}).pack())

            elif packet.type == PacketType.DELETE_FILE_REQUEST_ACK:
                filename = packet.data['filename']
                print(f'Leader successfully received DELETE request for file {filename}. waiting for nodes to delete the file...')
                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()

            elif packet.type == PacketType.DELETE_FILE_REQUEST_FAIL:
                filename = packet.data['filename']
                error = packet.data['error']
                print(f'Failed to delete file {filename}: {error}')
                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()
                
                if self._waiting_for_second_leader_event is not None:
                    self._waiting_for_second_leader_event.set()

            elif packet.type == PacketType.DELETE_FILE_REQUEST_SUCCESS:
                filename = packet.data['filename']
                print(f'FILE {filename} SUCCESSFULLY DELETED')

                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()

                if self._waiting_for_second_leader_event is not None:
                    self._waiting_for_second_leader_event.set()

            elif packet.type == PacketType.PUT_REQUEST_ACK:
                filename = packet.data['filename']
                print(f'Leader successfully received PUT request for file {filename}. waiting for nodes to download the file...')
                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()

            elif packet.type == PacketType.PUT_REQUEST_SUCCESS:
                filename = packet.data['filename']
                print(f'FILE {filename} SUCCESSFULLY STORED')

                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()

                if self._waiting_for_second_leader_event is not None:
                    self._waiting_for_second_leader_event.set()

            elif packet.type == PacketType.PUT_REQUEST_FAIL:
                filename = packet.data['filename']
                error = packet.data['error']
                print(f'Failed to PUT file {filename}: {error}')
                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()
                
                if self._waiting_for_second_leader_event is not None:
                    self._waiting_for_second_leader_event.set()

            elif packet.type == PacketType.LIST_FILE_REQUEST:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    sdfsFileName = packet.data['filename']
                    machines = self.leaderObj.get_machineids_for_file(sdfsFileName)
                    await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.LIST_FILE_REQUEST_ACK, {'filename': sdfsFileName, 'machines': machines}).pack())

            elif packet.type == PacketType.LIST_FILE_REQUEST_ACK:
                recv_sdfsfilename = packet.data['filename']
                recv_machineids: list[str] = packet.data["machines"]
                self.display_machineids_for_file(recv_sdfsfilename, recv_machineids)
                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()

            elif packet.type == PacketType.GET_FILE_REQUEST:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    sdfsFileName = packet.data['filename']
                    machineids_with_filenames = self.leaderObj.get_machineids_with_filenames(sdfsFileName)
                    await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.GET_FILE_REQUEST_ACK, {'filename': sdfsFileName, 'machineids_with_file_versions': machineids_with_filenames}).pack())
            
            elif packet.type == PacketType.GET_FILE_REQUEST_ACK:
                self.get_file_sdfsfilename = packet.data['filename']
                self.get_file_machineids_with_file_versions = packet.data["machineids_with_file_versions"]
                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()
            
            elif packet.type == PacketType.GET_FILE_NAMES_REQUEST:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    file_pattern = packet.data['filepattern']
                    all_filenames = self.leaderObj.get_all_matching_files(file_pattern)
                    await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.GET_FILE_NAMES_REQUEST_ACK, {'filepattern': file_pattern, 'files': all_filenames}).pack())

            elif packet.type == PacketType.GET_FILE_NAMES_REQUEST_ACK:
                self.get_file_sdfsfilename = packet.data['filepattern']
                self.get_file_machineids_with_file_versions = packet.data["files"]
                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()
            


            elif packet.type == PacketType.SUBMIT_JOB_RELAY:

                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    model = packet.data['model']
                    images_count = packet.data['images_count']
                    request_node = packet.data['request_node']

                    self.job_count += 1
                    # await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.SUBMIT_JOB_REQUEST_ACK, {'jobid': self.job_count}).pack())
                    await self.handle_job_request(Config.get_node_from_unique_name(request_node), model=model, number_of_images=images_count, job_id=self.job_count)







            





            elif packet.type == PacketType.SUBMIT_JOB_REQUEST:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    model = packet.data['model']
                    images_count = packet.data['images_count']
                    self.job_count += 1
                    await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.SUBMIT_JOB_REQUEST_ACK, {'jobid': self.job_count}).pack())
                    if H1.unique_name == self.leaderNode.unique_name:
                        await self.io.send(H2.host, H2.port, Packet(self.config.node.unique_name, PacketType.SUBMIT_JOB_RELAY, {'model': model, 'images_count': images_count, 'request_node': curr_node.unique_name}).pack())
                    await self.handle_job_request(curr_node, model=model, number_of_images=images_count, job_id=self.job_count)
            
            elif packet.type == PacketType.SUBMIT_JOB_REQUEST_ACK:
                self.current_job_id = packet.data['jobid']
                print(f"Leader node ACK for Job#{self.current_job_id}")
                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()
            
            elif packet.type == PacketType.SUBMIT_JOB_REQUEST_SUCCESS:
                jobid = packet.data['jobid']
                print(f'Job#{jobid} SUCCESSFULLY Completed')

                # await self.get_output_cli(jobid)

                if self._waiting_for_leader_event is not None:
                    self._waiting_for_leader_event.set()

                if self._waiting_for_second_leader_event is not None:
                    self._waiting_for_second_leader_event.set()
            
            elif packet.type == PacketType.WORKER_TASK_REQUEST:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:

                    if self.job_task is not None:
                        # cancel the task
                        self.job_task.cancel()
                        # Wait for the cancellation of task to be complete
                        try:
                            await self.job_task
                        except asyncio.CancelledError:
                            print("cancelled now")
                        
                        self.job_task = None
                    
                    jobid = packet.data['jobid']
                    batchid = packet.data['batchid']
                    model = packet.data['model']
                    req_images = packet.data['images']
                    start_time = time()

                    task = asyncio.create_task(self.handle_worker_task_request(curr_node, model, req_images, jobid, batchid, start_time))
                    self.job_task = task


            elif packet.type == PacketType.WORKER_TASK_ACK_RELAY:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    jobid = packet.data['jobid']
                    batchid = packet.data['batchid']
                    model = packet.data['model']
                    image_count = packet.data['image_counts']

                    if jobid in self.job_reqester_dict:
                        index = -1
                        i = 0
                        for batch_dict in self.model_dict[model]["queue"]:
                            if batch_dict["job_id"] == jobid and batch_dict["batch_id"] == batchid:
                                index = i
                                break
                            i += 1

                        if index != -1:
                            self.model_dict[model]["queue"].pop(index)
                        self.job_reqester_dict[jobid]["num_of_batches_pending"] -= 1
                        self.model_dict[model]['measurements']['query_count'] += image_count


            
            elif packet.type == PacketType.WORKER_TASK_REQUEST_ACK:

                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    jobid = packet.data['jobid']
                    batchid = packet.data['batchid']
                    model = packet.data['model']
                    image_count= packet.data['image_count']
                    start_time = packet.data['start_time']
                    if jobid in self.job_reqester_dict:
                        
                        self.model_dict[model]['measurements']['query_count'] += image_count
                        self.model_dict[model]['measurements']['query_rate_list'].append((time(), time() - start_time, image_count))

                        index = -1
                        i = 0
                        for batch_dict in self.model_dict[model]["inprogress_queue"]:
                            if batch_dict["job_id"] == jobid and batch_dict["batch_id"] == batchid:
                                index = i
                                break
                            i += 1

                        if index != -1:
                            self.model_dict[model]["inprogress_queue"].pop(index)

                        del self.workers_tasks_dict[curr_node.unique_name]

                        req_node = self.job_reqester_dict[jobid]["request_node"]
                        self.job_reqester_dict[jobid]["num_of_batches_pending"] -= 1
                        if self.job_reqester_dict[jobid]["num_of_batches_pending"] == 0:
                            await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.SUBMIT_JOB_REQUEST_SUCCESS, {'jobid': jobid}).pack())

                        if H1.unique_name == self.leaderNode.unique_name:
                            await self.io.send(H2.host, H2.port, Packet(self.config.node.unique_name, PacketType.WORKER_TASK_ACK_RELAY, {'jobid': jobid, 'batchid': batchid, 'model': model, 'image_counts': image_count}).pack())

                    # asyncio.create_task(self.schedule_job())
                    if self.leaderFlag:
                        await self.schedule_job()

            elif packet.type == PacketType.SET_BATCH_SIZE:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    model = packet.data['model']
                    batch_size = int(packet.data['batch_size'])
                    
                    self.model_dict[model]['hyperparams']['batch_size'] = batch_size
                    self.model_dict[model]['hyperparams']['time'] = ModelParameters(download_time=1, model_load_time=5.6, first_image_predict_time=2, each_image_predict_time=0.325, batch_size=batch_size).execution_time_per_vm()

                    print('set hyperparameters')

            elif packet.type == PacketType.GET_C2_COMMAND:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    inceptionv3_avg, inceptionv3_std, inceptionv3_quantiles, resnet50_avg, resnet50_std, resnet50_quantiles = self.calculate_c2_command_params()

                    await self.io.send(curr_node.host, curr_node.port, Packet(self.config.node.unique_name, PacketType.GET_C2_COMMAND_ACK, {'inceptionv3_avg': inceptionv3_avg, 'inceptionv3_std': inceptionv3_std, 'inceptionv3_quantiles': inceptionv3_quantiles, 'resnet50_avg': resnet50_avg, 'resnet50_std': resnet50_std, 'resnet50_quantiles': resnet50_quantiles}).pack())

            elif packet.type == PacketType.GET_C2_COMMAND_ACK:
                curr_node: Node = Config.get_node_from_unique_name(packet.sender)
                if curr_node:
                    inceptionv3_avg = packet.data['inceptionv3_avg']
                    inceptionv3_std = packet.data['inceptionv3_std']
                    inceptionv3_quantiles = packet.data['inceptionv3_quantiles']
                    resnet50_avg = packet.data['resnet50_avg']
                    resnet50_std = packet.data['resnet50_std']
                    resnet50_quantiles = packet.data['resnet50_quantiles']
                    
                    self.print_c2_command(inceptionv3_avg, inceptionv3_std, inceptionv3_quantiles, resnet50_avg, resnet50_std, resnet50_quantiles)
                    
                    if self._waiting_for_leader_event is not None:
                        self._waiting_for_leader_event.set()


            # elif packet.type == PacketType.WORKER_KILL_TASK_REQUEST:
            #     curr_node: Node = Config.get_node_from_unique_name(packet.sender)
            #     if curr_node:
            #         jobid = packet.data['jobid']
            #         if jobid in self.job_task_dict:
            #             task = self.job_task_dict[jobid]
            #             task.cancel()
            #             # Wait for the cancellation of task to be complete
            #             try:
            #                 await task
            #             except asyncio.CancelledError:
            #                 print("cancelled now")
            #             del self.job_task_dict[jobid]
            #         await self.io.send(req_node.host, req_node.port, Packet(self.config.node.unique_name, PacketType.WORKER_KILL_TASK_REQUEST_ACK, {'jobid': jobid}).pack())
            
            # elif packet.type == PacketType.WORKER_KILL_TASK_REQUEST_ACK:
            #     curr_node: Node = Config.get_node_from_unique_name(packet.sender)
            #     if curr_node:
            #         jobid = packet.data['jobid']
            #         logging.info(f"{curr_node.unique_name} killed its JOB#{jobid}")

    async def _wait(self, node: Node, timeout: float) -> bool:
        """Function to wait for ACKs after PINGs"""
        event = Event()
        self._add_waiting(node, event)

        try:
            await asyncio.wait_for(event.wait(), timeout)
        except exceptions.TimeoutError:
            # print(f'{datetime.now()}: failed to recieve ACK from {node.unique_name}')
            self.total_ack_missed += 1
            if not self.waiting_for_introduction and not self.fetchingIntroducerFlag:
                if node in self.missed_acks_count:
                    self.missed_acks_count[node] += 1
                else:
                    self.missed_acks_count[node] = 1
                
                logging.error(f'failed to recieve ACK from {node.unique_name} for {self.missed_acks_count[node]} times')
                if self.missed_acks_count[node] > 3:
                    self.membership_list.update_node_status(node=node, status=0)
            else:
                logging.error(f'failed to recieve ACK from {node.unique_name}')
                self.fetchingIntroducerFlag = True
        except Exception as e:
            self.total_ack_missed += 1
            # print(f'{datetime.now()}: Exception when waiting for ACK from {node.unique_name}: {e}')
            if not self.waiting_for_introduction and not self.fetchingIntroducerFlag:
                if node in self.missed_acks_count:
                    self.missed_acks_count[node] += 1
                else:
                    self.missed_acks_count[node] = 1

                logging.error(f'Exception when waiting for ACK from {node.unique_name} for {self.missed_acks_count[node]} times: {e}')
                if self.missed_acks_count[node] > 3:
                    self.membership_list.update_node_status(node=node, status=0)
            else:
                logging.error(
                    f'Exception when waiting for ACK from introducer: {e}')

        return event.is_set()

    async def _wait_for_leader(self, timeout: float) -> bool:
        """Function to wait for Leader to respond back for request"""
        event = Event()
        self._waiting_for_leader_event = event

        try:
            await asyncio.wait_for(event.wait(), timeout)
        except exceptions.TimeoutError:
            print(f'{datetime.now()}: failed to recieve Response from Leader')
        except Exception as e:
            print(f'{datetime.now()}: Exception when waiting for Response from Leader: {e}')

        return event.is_set()

    async def introduce(self) -> None:
        """FUnction to ask introducer to introduce"""
        logging.debug(
            f'sending pings to introducer: {self.leaderNode.unique_name}')
        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.INTRODUCE, self.membership_list.get()).pack())
        await self._wait(self.leaderNode, PING_TIMEOOUT)

    async def fetch_introducer(self) -> None:
        logging.debug(f'sending pings to introducer DNS: {self.config.introducerDNSNode.unique_name}')
        print('sending ping to fetch introducer')
        await self.io.send(self.config.introducerDNSNode.host, self.config.introducerDNSNode.port, Packet(self.config.node.unique_name, PacketType.FETCH_INTRODUCER, {}).pack())
        await self._wait(self.config.introducerDNSNode, PING_TIMEOOUT)

    async def update_introducer(self):
        print('updating introducer on DNS')
        await self.io.send(self.config.introducerDNSNode.host, self.config.introducerDNSNode.port, Packet(self.config.node.unique_name, PacketType.UPDATE_INTRODUCER, {}).pack())
        await self._wait(self.config.introducerDNSNode, PING_TIMEOOUT)

    async def check(self, node: Node) -> None:
        """Fucntion to send PING to a node"""
        logging.debug(f'pinging: {node.unique_name}')
        await self.io.send(node.host, node.port, Packet(self.config.node.unique_name, PacketType.PING, self.membership_list.get()).pack())
        await self._wait(node, PING_TIMEOOUT)

    async def send_election_messages(self):
        """function to keep sending election message while the election phase is in progress"""
        while True:
            if self.globalObj.election.electionPhase:
                for node in self.membership_list.current_pinging_nodes:
                    print(f'sending election message to {node.unique_name}')
                    await self.io.send(node.host, node.port, Packet(self.config.node.unique_name, PacketType.ELECTION, {}).pack())

            await asyncio.sleep(PING_DURATION)
    
    async def send_coordinator_message(self):
        """Multicast coordinate messages if the current node has the highest id in the election phase"""
        self.temporary_file_dict = {}
        online_nodes = self.membership_list.get_online_nodes()

        for node in online_nodes:
            if node.unique_name != self.config.node.unique_name :
                await self.io.send(node.host, node.port, Packet(self.config.node.unique_name, PacketType.COORDINATE, {}).pack())
            # await self._wait(node, PING_TIMEOOUT)

    async def run_failure_detection(self) -> NoReturn:
        """Function to sends pings to subset of nodes in the RING"""
        while True:
            if self.is_current_node_active:
                if not self.waiting_for_introduction:
                    for node in self.membership_list.current_pinging_nodes:
                        self.total_pings_send += 1
                        asyncio.create_task(self.check(node))
                else:
                    self.total_pings_send += 1
                    # print(self.fetchingIntroducerFlag)
                    if self.fetchingIntroducerFlag:
                        asyncio.create_task(self.fetch_introducer())
                    # print(self.waiting_for_introduction, self.fetchingIntroducerFlag)
                    if self.waiting_for_introduction and not self.fetchingIntroducerFlag:
                        # print('i entered here now')
                        asyncio.create_task(self.introduce())

            await asyncio.sleep(PING_DURATION)

    async def send_put_request_to_leader(self, localFileName, sdfsFileName):
        """function to send the PUT request to the leader from the client"""
        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.PUT_REQUEST, {'file_path': localFileName, 'filename': sdfsFileName}).pack())
        await self._wait_for_leader(20)

    async def send_del_request_to_leader(self, sdfsFileName):
        """function to send the DELETE request to the leader from the client"""
        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE_REQUEST, {'filename': sdfsFileName}).pack())
        await self._wait_for_leader(20)

    async def send_ls_request_to_leader(self, sdfsfilename):
        """function to send the LS request to the leader from the client"""

        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.LIST_FILE_REQUEST, {'filename': sdfsfilename}).pack())
        await self._wait_for_leader(20)
    
    async def send_get_file_request_to_leader(self, sdfsfilename):
        """function to send the GET request to the leader from the client"""

        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.GET_FILE_REQUEST, {'filename': sdfsfilename}).pack())
        await self._wait_for_leader(20)
    
    async def send_get_filenames_request_to_leader(self, file_pattern):
        """function to send the GET_FILENAMES request to the leader from the client"""
        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.GET_FILE_NAMES_REQUEST, {'filepattern': file_pattern}).pack())
        await self._wait_for_leader(20)
    
    async def send_submit_job_request_to_leader(self, model, num_of_images):
        """function to send the PUT request to the leader from the client"""
        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.SUBMIT_JOB_REQUEST, {'model': model, 'images_count': num_of_images}).pack())
        await self._wait_for_leader(20)

    async def send_c2_command_to_leader(self):
        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.GET_C2_COMMAND, {}).pack())
        await self._wait_for_leader(20)

    async def send_batch_size_command_to_leader(self, model, batch_size):

        await self.io.send(self.leaderNode.host, self.leaderNode.port, Packet(self.config.node.unique_name, PacketType.SET_BATCH_SIZE, {'model': model, 'batch_size': batch_size}).pack())

    def isCurrentNodeLeader(self):
        """Function to check if this node is the leader"""
        if self.leaderObj is not None and self.config.node.unique_name == self.leaderNode.unique_name:
            return True
        return False
    
    async def replace_files_downloading_by_node(self, node):

        for file, file_dict in self.leaderObj.status_dict.items():

            if file_dict['request_type'] == 'PUT':

                if node == file_dict['request_node']:
                    
                    new_replicas = []
                    for replica_node, download_status in file_dict['replicas'].items():
                        if download_status == 'Success':
                            new_replicas.append(replica_node)
                else:
                    if node in file_dict['replicas'] and file_dict['replicas'][node] != 'Success':
                        running_nodes = set(list(self.globalObj.worker.membership_list.memberShipListDict.keys()))
                        replica_nodes = set(list(file_dict['replicas'].keys()))
                        possible_replica_nodes = list(running_nodes.difference(replica_nodes))
                        if len(possible_replica_nodes) == 0:
                            possible_replica_node = random.choice(possible_replica_nodes)
                            await self.io.send(possible_replica_node.host, possible_replica_node.port, Packet(self.config.node.unique_name, PacketType.DOWNLOAD_FILE, {'hostname': file_dict['request_node'], 'file_path': file_dict['file_path'], 'filename': file}).pack())
                            del self.leaderObj.status_dict[file]['replicas'][node]
                            self.leaderObj.status_dict[file]['replicas'][possible_replica_node] = 'Waiting'
            
            elif file_dict['request_type'] == 'DELETE':
                if node in file_dict['replicas']:
                    # updating delete status as success as node rejoins it will clear all stored files
                    self.leaderObj.update_replica_status(file, node, 'Success')
                    if self.leaderObj.check_if_request_completed(file):
                        original_requesting_node = self.leaderObj.status_dict[file]['request_node']
                        await self.io.send(original_requesting_node.host, original_requesting_node.port, Packet(self.config.node.unique_name, PacketType.DELETE_FILE_REQUEST_SUCCESS, {'filename': file}).pack())
                        self.leaderObj.delete_status_for_file(file)

    async def handle_failures_if_pending_status(self, node: str):
        if self.leaderFlag:
            self.leaderObj.delete_node_from_global_dict(node)
            await self.replace_files_downloading_by_node(node)

            if node in self.workers_tasks_dict:

                # add the batch_dict to infront of the queue
                batch_dict = self.workers_tasks_dict[node]
                batch_jobid = batch_dict['job_id']
                batch_id = batch_dict['batch_id']
                batch_dict_model = batch_dict["model"]

                i = 0
                for item in self.model_dict[batch_dict_model]['inprogress_queue']:
                    item_jobid = item["job_id"]
                    item_batchid = item["batch_id"]
                    if item_jobid == batch_jobid and item_batchid == batch_id:
                        break
                    
                    i+= 1

                logging.info(f'JOB#{batch_jobid} BATCH#{batch_id} DIED!!!')
                preempted_batch = self.model_dict[batch_dict_model]['inprogress_queue'].pop(i)
                self.model_dict[batch_dict_model]["queue"].insert(0, preempted_batch)

                del self.workers_tasks_dict[node]
                await self.schedule_job()

    async def replicate_files(self):
        """Function to replicate the files once 3 failures are detected"""
        if self.leaderFlag:
            replication_dict = self.leaderObj.find_files_for_replication()
            print(replication_dict)
            self.replicate_start_time = time()
            for filename in replication_dict:
                for node in replication_dict[filename]:
                    downloading_node = Config.get_node_from_unique_name(node)
                    await self.io.send(downloading_node.host, downloading_node.port, Packet(self.config.node.unique_name, PacketType.REPLICATE_FILE, {'replicas': replication_dict[filename][node]}).pack())
                    self.leaderObj.create_new_status_for_file(filename, '', self.config.node, 'REPLICATE')
            #     for replication_obj in replication_dict[filename]:
            #         downloading_node = Config.get_node_from_unique_name(replication_obj['downloading_node'])
            # pass

    async def get_file_locally(self, machineids_with_filenames, sdfsfilename, localfilename, file_count=1):
        # download latest file locally
        # if self.config.node.unique_name in machineids_with_filenames:
        #     if file_count == 1:
        #         filepath = self.file_service.copyfile(machineids_with_filenames[self.config.node.unique_name][-1], localfilename)
        #         print(f"GET file {sdfsfilename} success: copied to {filepath}")
        #     else:
        #         files = machineids_with_filenames[self.config.node.unique_name]
        #         filepaths = []
        #         if file_count > len(files):
        #             file_count = len(files)
        #         for i in range(0, file_count):
        #             filepath = self.file_service.copyfile(machineids_with_filenames[self.config.node.unique_name][len(files) - 1 - i], f'{localfilename}_version{i}')
        #             filepaths.append(filepath)
        #         print(f"GET files {sdfsfilename} success: copied to {filepaths}")
        # else:
        # file not in local system, download files from machines
        downloaded = False
        for machineid, files in machineids_with_filenames.items():
            download_node = self.config.get_node_from_unique_name(machineid)
            if file_count == 1:
                downloaded = await self.file_service.download_file_to_dest(host=download_node.host, username=USERNAME, password=PASSWORD, file_location=files[-1], destination_file=localfilename)
            else:
                if file_count > len(files):
                    file_count = len(files)
                for i in range(0, file_count):
                    downloaded = await self.file_service.download_file_to_dest(host=download_node.host, username=USERNAME, password=PASSWORD, file_location=files[len(files) - 1 - i], destination_file=f'{localfilename}_version{i}')
            if downloaded:
                print(f"GET file {sdfsfilename} success: copied to {localfilename}")
                break
        if not downloaded:
            print(f"GET file {sdfsfilename} failed")
    
    async def run_inference_on_testfiles(self, model, images):
        start_time = time()
        await perform_inference(model, images)
        print(f"{model} Inference on {len(images)} images took {time() - start_time}")
    
    async def run_inference_cli(self, model, images):
        
        start_time = time()

        for image, locations in images.items():
            await self.get_cli_nowait(image, DOWNLOAD_PATH + image, locations)
        
        failed_images = []
        images_full_path = []
        for image, _ in images.items():
            if os.path.exists(DOWNLOAD_PATH + image):
                images_full_path.append(DOWNLOAD_PATH + image)
            else:
                failed_images.append(image)

        logging.info(f"{model} Download of {len(images_full_path)} images took {time() - start_time} sec")

        start_time1 = time()
        results = await perform_inference(model, images_full_path)

        for failed_image in failed_images:
            results[failed_image] = "Failed to download file from SDFS"

        logging.info(f"{model} Inference on {len(images_full_path)} downloaded images took {time() - start_time1} sec: Total runtime of task: {time() - start_time} sec")

        return results

    

    def print_c2_command(self, inceptionv3_avg, inceptionv3_std, inceptionv3_quantiles, resnet50_avg, resnet50_std, resnet50_quantiles):
        print(f"Query Processing Time per model: \nInceptionV3:\nAverage={inceptionv3_avg}\nStandard Deviation={inceptionv3_std}\nPercentiles={inceptionv3_quantiles}")
        print(f"ResNet50:\nAverage={resnet50_avg}\nStandard Deviation={resnet50_std}\nPercentiles={resnet50_quantiles}")

    def calculate_c2_command_params(self):
        inceptionv3_query_rate = []
        inceptionv3_query_rate_list = self.model_dict['InceptionV3']['measurements']['query_rate_list']
        for i in range(len(inceptionv3_query_rate_list)):
            timestamp, execution_time, image_count = inceptionv3_query_rate_list[i]
            inceptionv3_query_rate.append(execution_time/image_count)
        
        try:
            inceptionv3_avg = statistics.mean(inceptionv3_query_rate)
            inceptionv3_std = statistics.stdev(inceptionv3_query_rate)
            inceptionv3_quantiles = statistics.quantiles(inceptionv3_query_rate, n=4)
        except statistics.StatisticsError as e:
            print(e)
            inceptionv3_avg = 0
            inceptionv3_std = 0
            inceptionv3_quantiles = 0


        resnet50_query_rate = []
        resnet50_query_rate_list = self.model_dict['ResNet50']['measurements']['query_rate_list']
        for i in range(len(resnet50_query_rate_list)):
            timestamp, execution_time, image_count = resnet50_query_rate_list[i]
            resnet50_query_rate.append(execution_time/image_count)
        
        try:
            resnet50_avg = statistics.mean(resnet50_query_rate)
            resnet50_std = statistics.stdev(resnet50_query_rate)
            resnet50_quantiles = statistics.quantiles(resnet50_query_rate, n=4)
        except statistics.StatisticsError as e:
            print(e)
            resnet50_avg = 0
            resnet50_std = 0
            resnet50_quantiles = 0

        return (inceptionv3_avg, inceptionv3_std, inceptionv3_quantiles, resnet50_avg, resnet50_std, resnet50_quantiles)
        
    def run_inference_without_async(self, model, images):

        start_time = time()

        # download all the images locally
        failed_images = []
        images_full_path = []
        for image in images:
            if os.path.exists(image):
                images_full_path.append(image)
            elif os.path.exists(DOWNLOAD_PATH + image):
                images_full_path.append(DOWNLOAD_PATH + image)
            # else:
            #     await self.get_cli(image, DOWNLOAD_PATH + image)
            #     if os.path.exists(DOWNLOAD_PATH + image):
            #         images_full_path.append(DOWNLOAD_PATH + image)
            #     else:
            #         failed_images.append(image)
        
        print(f"{model} Download of {len(images_full_path)} images took {time() - start_time} sec")
        # results = await perform_inference(model, images_full_path)

        results = perform_inference_without_async(model, images_full_path)

        for failed_image in failed_images:
            results[failed_image] = "Failed to download file from SDFS"

        print(f"{model} Inference on {len(images_full_path)} images took {time() - start_time} sec")

        return results

    async def get_cli(self, sdfsfilename, localfilename):
        if self.isCurrentNodeLeader():
            logging.info(f"fetching machine details locally about {sdfsfilename}.")
            machineids_with_filenames = self.leaderObj.get_machineids_with_filenames(sdfsfilename)
            await self.get_file_locally(machineids_with_filenames=machineids_with_filenames, sdfsfilename=sdfsfilename, localfilename=localfilename)
        else:
            logging.info(f"fetching machine details where the {sdfsfilename} is stored from Leader.")
            await self.send_get_file_request_to_leader(sdfsfilename)
            if self.get_file_machineids_with_file_versions is not None and self.get_file_sdfsfilename is not None:
                await self.get_file_locally(machineids_with_filenames=self.get_file_machineids_with_file_versions, sdfsfilename=self.get_file_sdfsfilename, localfilename=localfilename)
                self.get_file_machineids_with_file_versions = None
                self.get_file_sdfsfilename = None

            del self._waiting_for_leader_event
            self._waiting_for_leader_event = None
        
    async def get_cli_nowait(self, sdfsfilename, localfilename, machineids_with_filenames):
        logging.info(f"fetching machine details where the {sdfsfilename} is stored from Leader.")
        await self.get_file_locally(machineids_with_filenames=machineids_with_filenames, sdfsfilename=self.get_file_sdfsfilename, localfilename=localfilename)
    
    async def get_all_files(self, pattern):
        
        await self.send_get_filenames_request_to_leader(pattern)
        
        all_files = []
        if self.get_file_machineids_with_file_versions is not None and self.get_file_sdfsfilename is not None:
            all_files = self.get_file_machineids_with_file_versions
            self.get_file_machineids_with_file_versions = None
            self.get_file_sdfsfilename = None
        
        del self._waiting_for_leader_event
        self._waiting_for_leader_event = None

        return all_files
    
    async def download_all_files(self, all_files, output_dir):
        for f in all_files:
            await self.get_cli(f, output_dir + f)
        
        downloaded_files = []
        failed_files = []
        for f in all_files:
            if not os.path.exists(output_dir + f):
                failed_files.append(f)
            else:
                downloaded_files.append(f)
        
        if len(failed_files):
            print(f"Failed to download {len(failed_files)} files: {failed_files}")

        return downloaded_files
        
    def merge_all_json_files(self, all_files, local_dir, output_file):
        
        available_files = []
        for f in all_files:
            if os.path.exists(local_dir + f):
                available_files.append(f)
        
        # merge all the files into a single json file
        if len(available_files) == 0:
            print("requested files are not available in provided directory")
            return 
        
        final_output:dict = {}
        for f in available_files:
            with open(local_dir + f, encoding='utf-8') as f1:
                new_dict = json.loads(f1.read())
                final_output.update(new_dict)
        
        # create new file with the result               
        dump_to_file(final_output, local_dir + output_file)

        print(f"written final data into {local_dir + output_file}")

    async def put_cli(self, localfilename, sdfsfilename):
        
        if not path.exists(localfilename):
            print('invalid localfilename for put command.')
            return

        await self.send_put_request_to_leader(localfilename, sdfsfilename)

        event = Event()
        self._waiting_for_second_leader_event = event
        await asyncio.wait([self._waiting_for_second_leader_event.wait()])
        del self._waiting_for_second_leader_event
        self._waiting_for_second_leader_event = None
    
    async def ls_all_cli(self, file_pattern):
        matched_files = []
        if self.isCurrentNodeLeader():
            matched_files = self.leaderObj.get_all_matching_files(file_pattern)
        else:
            matched_files = await self.get_all_files(file_pattern)
        return matched_files

    def ls_all_temp_dict(self, file_pattern):
        matched_files = []
        matched_files = self.get_all_matching_files_from_temp_dict(file_pattern)
        return matched_files

    def get_all_matching_files_from_temp_dict(self, pattern):

        matching_files = set()
        for _, machine_file_dict in self.temporary_file_dict.items():
            for sdfsFileName, _ in machine_file_dict.items():
                if fnmatch.fnmatch(sdfsFileName, pattern):
                    matching_files.add(sdfsFileName)
        return list(matching_files)

    
    async def predict_locally_cli(self, model, p_images, job_id, batch_id=0):
        
        # perform prediction on all the images
        # await self.run_inference_on_testfiles(model, images)
        results = await self.run_inference_cli(model, p_images)

        # create new file with the result
        filename = f"output_{job_id}_{batch_id}_{self.config.node.host.split('.')[0]}.json"                    
        dump_to_file(results, DOWNLOAD_PATH + filename)
        
        print(f"written output to file {filename}")
        
        return filename

    def predict_locally_cli_without_async(self, model, p_images, job_id):

        images = []
        try:
            images_option = p_images
            if isinstance(images_option, int):
                dir_list = os.listdir(TEST_FILES_PATH)
                if images_option > len(dir_list) or images_option <= 0:
                    images = dir_list
                else:
                    images = random.sample(dir_list, images_option)
            elif isinstance(images_option, list):
                images = images_option
            else:
                print('invalid images provided')
                return
        except:
            images.append(images_option)
        
        # perform prediction on all the images
        results = self.run_inference_without_async(model, images)

        # create new file with the result
        filename = f"output_{job_id}_{self.config.node.host.split('.')[0]}.json"                    
        dump_to_file(results, DOWNLOAD_PATH + filename)
        
        print(f"written output to file {filename}")

        return filename

    async def get_output_cli(self, jobid):
        filepattern = f"output_{jobid}_*.json"
        matched_files = await self.ls_all_cli(filepattern)

        downloaded_files = await self.download_all_files(matched_files, DOWNLOAD_PATH)

        print(f"{len(downloaded_files)} files downloaded!!!\n{downloaded_files}")

        output_file = f"final_{jobid}.json"

        self.merge_all_json_files(downloaded_files, DOWNLOAD_PATH, output_file)

    async def check_user_input(self):
        """Function to ask for user input and handles"""
        loop = asyncio.get_event_loop()
        queue = asyncio.Queue()

        def response():
            loop.create_task(queue.put(sys.stdin.readline()))

        loop.add_reader(sys.stdin.fileno(), response)

        while True:

            print(f'choose one of the following options or type commands:')
            print('MP4 commands:')
            print(' C1: Query Rate (10sec) & Query Count [Per model]')
            print(' C2: Query Processing Time: [Average, Percentiles, Standard Deviation]')
            print(' C3 <InceptionV3|ResNet50> <Batch Size>')
            print(' C4: submit-job <InceptionV3|ResNet50> <num:of images>')
            print(' C4: get-output <jobid>')
            print(' C5: Display current assigned jobs')
            print('')
            print('options:')
            print(' 1. list the membership list.')
            print(' 2. list self id.')
            print(' 3. join the group.')
            print(' 4. leave the group.')
            print(' 5. load testfiles into sdfs.')
            print(' 6. print files stored per node.')
            print(' 7. print all files in the SDFS.')
            print(' 8. print number of files in the SDFS.')
            if self.config.testing:
                print('9. print current bps.')
                print('10. current false positive rate.')
            print('')
            print('commands:')
            print(' * put <localfilename> <sdfsfilename>')
            print(' * get <sdfsfilename> <localfilename>')
            print(' * get-all <sdfsfilepattern> <local_dir>')
            print(' * delete <sdfsfilename>')
            print(' * ls <sdfsfilename>')
            print(' * ls-all <sdfsfilepattern>')
            print(' * store')
            print(' * get-versions <sdfsfilename> <numversions> <localfilename>')
            print('')

            option: Optional[str] = None
            while True:
                option = await queue.get()
                if option != '\n':
                    break
            
            if option.strip() == '1':
                self.membership_list.print()
            elif option.strip() == '2':
                print(self.config.node.unique_name)
            elif option.strip() == '3':
                self.is_current_node_active = True
                logging.info('started sending ACKs and PINGs')
            elif option.strip() == '4':
                self.is_current_node_active = False
                self.membership_list.memberShipListDict = dict()
                self.config.ping_nodes = GLOBAL_RING_TOPOLOGY[self.config.node]
                self.waiting_for_introduction = True
                self.io.time_of_first_byte = 0
                self.io.number_of_bytes_sent = 0
                # self.initialize(self.config)
                logging.info('stopped sending ACKs and PINGs')
            elif option.strip() == '5':
                logging.info('loading testfiles into sdfs ...')
                dir_list = os.listdir(TEST_FILES_PATH)
                start_time = time()
                for file in dir_list:
                    await self.send_put_request_to_leader(TEST_FILES_PATH + file, file)

                    event = Event()
                    self._waiting_for_second_leader_event = event
                    await asyncio.wait([self._waiting_for_second_leader_event.wait()])
                    del self._waiting_for_second_leader_event
                    self._waiting_for_second_leader_event = None
                print(f"PUT all testfiles runtime: {time() - start_time} seconds")
            elif option.strip() == '6':
                if self.leaderFlag:
                    print(self.leaderObj.global_file_dict)
            elif option.strip() == '7':
                sdfs_files = set()
                for k, v in self.leaderObj.global_file_dict.items():
                    for k1, v1 in v.items():
                        sdfs_files.add(k1)
                print(f"SDFS files: {list(sdfs_files)}")
            elif option.strip() == '8':
                sdfs_files = set()
                for k, v in self.leaderObj.global_file_dict.items():
                    for k1, v1 in v.items():
                        sdfs_files.add(k1)
                print(f"SDFS file count: {len(sdfs_files)}")
            elif self.config.testing and option.strip() == '9':
                if self.io.time_of_first_byte != 0:
                    logging.info(
                        f'BPS: {(self.io.number_of_bytes_sent)/(time() - self.io.time_of_first_byte)}')
                else:
                    logging.info(f'BPS: 0')
            elif self.config.testing and option.strip() == '10':
                if self.membership_list.false_positives > self.membership_list.indirect_failures:
                    logging.info(
                        f'False positive rate: {(self.membership_list.false_positives - self.membership_list.indirect_failures)/self.total_pings_send}, pings sent: {self.total_pings_send}, indirect failures: {self.membership_list.indirect_failures}, false positives: {self.membership_list.false_positives}')
                else:
                    logging.info(
                        f'False positive rate: {(self.membership_list.indirect_failures - self.membership_list.false_positives)/self.total_pings_send}, pings sent: {self.total_pings_send}, indirect failures: {self.membership_list.indirect_failures}, false positives: {self.membership_list.false_positives}')
            else: # checking if user typed cmd
                option = option.strip()
                options = option.split(' ')
                if len(options) == 0:
                    print('invalid option.')
                cmd = options[0]

                if cmd == "C1":
                    print(f"Qeury Count:\n  InceptionV3:{self.model_dict['InceptionV3']['measurements']['query_count']}\n   ResNet50:{self.model_dict['ResNet50']['measurements']['query_count']}")

                    inceptionv3_query_rate = []
                    inceptionv3_query_rate_list = self.model_dict['InceptionV3']['measurements']['query_rate_array']
                    curr_time = time()
                    if len(inceptionv3_query_rate_list):
                        curr_time = inceptionv3_query_rate_list[-1][0]
                    for i in range(len(inceptionv3_query_rate_list) - 1, -1, -1):
                        timestamp, query_rate = inceptionv3_query_rate_list[i]
                        if curr_time - timestamp <= 10:
                            inceptionv3_query_rate.append(query_rate)
                        else:
                            break
                    
                    resnet50_query_rate = []
                    resnet50_query_rate_list = self.model_dict['ResNet50']['measurements']['query_rate_array']
                    curr_time = time()
                    if len(resnet50_query_rate_list):
                        curr_time = resnet50_query_rate_list[-1][0]
                    for i in range(len(resnet50_query_rate_list) - 1, -1, -1):
                        timestamp, query_rate = resnet50_query_rate_list[i]
                        if curr_time - timestamp <= 10:
                            resnet50_query_rate.append(query_rate)
                        else:
                            break
                    
                    inceptionv3_avg, inceptionv3_std, inceptionv3_quantiles, resnet50_avg, resnet50_std, resnet50_quantiles = self.calculate_c2_command_params()
                    
                    inceptionv3_avg_query_rate = 0
                    if len(self.workers_tasks_dict) != 0 and len(inceptionv3_query_rate):
                        if inceptionv3_avg != 0:
                            inceptionv3_avg_query_rate = inceptionv3_query_rate[-1]/inceptionv3_avg
                        else:
                            inceptionv3_avg_query_rate = inceptionv3_query_rate[-1]/self.model_dict['InceptionV3']['hyperparams']['time']
                    
                    resnet50_avg_query_rate = 0
                    if len(self.workers_tasks_dict) != 0 and len(resnet50_query_rate):
                        if resnet50_avg != 0:
                            resnet50_avg_query_rate = resnet50_query_rate[-1]/resnet50_avg
                        else:
                            resnet50_avg_query_rate = resnet50_query_rate[-1]/self.model_dict['ResNet50']['hyperparams']['time']
                    
                    print(f"Qeury Rate [10 sec]:\n  InceptionV3:{inceptionv3_avg_query_rate}\n   ResNet50:{resnet50_avg_query_rate}")
                
                elif cmd == "C2":
                    
                    if self.isCurrentNodeLeader():

                        inceptionv3_avg, inceptionv3_std, inceptionv3_quantiles, resnet50_avg, resnet50_std, resnet50_quantiles = self.calculate_c2_command_params()
                        self.print_c2_command(inceptionv3_avg, inceptionv3_std, inceptionv3_quantiles, resnet50_avg, resnet50_std, resnet50_quantiles)

                    else:
                        await self.send_c2_command_to_leader()

                elif cmd == "C3":
                    # set batch size

                    model = options[1]
                    batch_size = options[2]

                    await self.send_batch_size_command_to_leader(model, batch_size)
                
                elif cmd == "C5":
                    print(json.dumps(self.workers_tasks_dict, indent=4))

                elif cmd == "put": # PUT file
                    if len(options) != 3:
                        print('invalid options for put command.')
                        continue
                    start_time = time()
                    localfilename = options[1]
                    sdfsfilename = options[2]
                    await self.put_cli(localfilename, sdfsfilename)
                    print(f"PUT runtime: {time() - start_time} seconds")

                elif cmd == "get": # GET file
                    if len(options) != 3:
                        print('invalid options for get command.')
                        continue
                    
                    start_time = time()
                    sdfsfilename = options[1]
                    localfilename = options[2]

                    await self.get_cli(sdfsfilename, localfilename)
                    
                    print(f"GET runtime: {time() - start_time} seconds")

                elif cmd == "delete": # DEL file
                    if len(options) != 2:
                        print('invalid options for delete command.')
                        continue
                    start_time = time()
                    sdfsfilename = options[1]
                    await self.send_del_request_to_leader(sdfsfilename)

                    event = Event()
                    self._waiting_for_second_leader_event = event
                    await asyncio.wait([self._waiting_for_second_leader_event.wait()])
                    del self._waiting_for_second_leader_event
                    self._waiting_for_second_leader_event = None
                    print(f"DELETE runtime: {time() - start_time} seconds")
                 
                elif cmd == "ls": # list all the
                    if len(options) != 2:
                        print('invalid options for ls command.')
                        continue
                    start_time = time()
                    sdfsfilename = options[1]
                    if self.isCurrentNodeLeader():
                        machineids = self.leaderObj.get_machineids_for_file(sdfsfilename)
                        self.display_machineids_for_file(sdfsfilename, machineids)
                    else:
                        await self.send_ls_request_to_leader(sdfsfilename)
                        del self._waiting_for_leader_event
                        self._waiting_for_leader_event = None
                    print(f"LS runtime: {time() - start_time} seconds")
        
                elif cmd == "store": # store
                    self.file_service.list_all_files()

                elif cmd == "get-versions": # get-versions
                    if len(options) != 4:
                        print('invalid options for get-versions command.')
                        continue
                    start_time = time()
                    sdfsfilename = options[1]
                    numversions = int(options[2])
                    localfilename = options[3]

                    if self.isCurrentNodeLeader():
                        logging.info(f"fetching machine details locally about {sdfsfilename}.")
                        machineids_with_filenames = self.leaderObj.get_machineids_with_filenames(sdfsfilename)
                        await self.get_file_locally(machineids_with_filenames=machineids_with_filenames, sdfsfilename=sdfsfilename, localfilename=localfilename, file_count=numversions)
                    else:
                        logging.info(f"fetching machine details where the {sdfsfilename} is stored from Leader.")
                        await self.send_get_file_request_to_leader(sdfsfilename)
                        if self.get_file_machineids_with_file_versions is not None and self.get_file_sdfsfilename is not None:
                            await self.get_file_locally(machineids_with_filenames=self.get_file_machineids_with_file_versions, sdfsfilename=self.get_file_sdfsfilename, localfilename=localfilename, file_count=numversions)
                            self.get_file_machineids_with_file_versions = None
                            self.get_file_sdfsfilename = None

                        del self._waiting_for_leader_event
                        self._waiting_for_leader_event = None
                    print(f"GET-VERSIONS runtime: {time() - start_time} seconds")

                elif cmd == "predict-locally": # predict_locally
                    
                    if len(options) != 4:
                        print('invalid options for predict-locally command.')
                        continue
                    
                    model = options[1]
                    if model not in ["InceptionV3", "ResNet50"]:
                        print('invalid model expected: InceptionV3 or ResNet50.')
                        continue

                    job_id = random.randrange(1, 100000)
                    try:
                        job_id = literal_eval(options[2])
                    except:
                        pass

                    images = []
                    try:
                        images_option = literal_eval(options[3])
                        if isinstance(images_option, int):
                            dir_list = os.listdir(TEST_FILES_PATH)
                            if images_option > len(dir_list) or images_option <= 0:
                                images = dir_list
                            else:
                                images = random.sample(dir_list, images_option)
                        elif isinstance(images_option, list):
                            images = images_option
                        else:
                            print('invalid images provided')
                            continue
                    except:
                        images.append(images_option)
                    
                    await self.predict_locally_cli(model, images, job_id)

                elif cmd == "ls-all":
                    
                    if len(options) != 2:
                        print('invalid options for ls-all command.')
                        continue
                    
                    start_time = time()
                    file_pattern = options[1]
                    matched_files = await self.ls_all_cli(file_pattern)
                    print(f"{len(matched_files)} files in SDFS matching {file_pattern}: \n{matched_files}")
                    print(f"LS-ALL runtime: {time() - start_time} seconds")

                elif cmd == "get-all":
                    
                    if len(options) != 3:
                        print('invalid options for get-all command.')
                        continue
                    
                    start_time = time()
                    sdfsfilepattern = options[1]
                    local_dir = options[2]
                    if not os.path.isdir(local_dir):
                        print("Invalid local directory provided")
                        continue

                    matched_files = await self.ls_all_cli(sdfsfilepattern)

                    downloaded_files = await self.download_all_files(matched_files, DOWNLOAD_PATH)

                    print(f"{len(downloaded_files)} files downloaded!!!\n{downloaded_files}")
                    
                    print(f"GET-ALL runtime: {time() - start_time} seconds")

                elif cmd == "get-output":

                    if len(options) != 2:
                        print('invalid options for get-output command.')
                        continue
                        
                    start_time = time()
                    jobid = options[1]

                    await self.get_output_cli(jobid)
                    
                    print(f"GET-OUTPUT runtime: {time() - start_time} seconds")

                elif cmd == "submit-job":

                    if len(options) != 3:
                        print('invalid options for submit-job command.')
                        continue
                        
                    model = options[1]
                    if model not in ["InceptionV3", "ResNet50"]:
                        print('invalid model expected: InceptionV3 or ResNet50.')
                        continue
                    
                    images_option = 0
                    try:
                        images_option = literal_eval(options[2])
                        if not isinstance(images_option, int):
                            print('invalid images count provided')
                            continue
                    except:
                        print('invalid images count provided')
                        continue
                    
                    # send this command to coordinator
                    await self.send_submit_job_request_to_leader(model, images_option)

                    print(f"coordinator recevied JOB request and assigned jobid: {self.current_job_id}")

                    # event = Event()
                    # self._waiting_for_second_leader_event = event
                    # await asyncio.wait([self._waiting_for_second_leader_event.wait()])
                    # del self._waiting_for_second_leader_event
                    # self._waiting_for_second_leader_event = None
                
                # elif cmd == "kill-job":

                #     if len(options) != 2:
                #         print('invalid options for kill-job command.')
                #         continue
                    
                #     jobid = 0
                #     try:
                #         jobid = literal_eval(options[1])
                #         if not isinstance(jobid, int):
                #             print('invalid jobid provided')
                #             continue
                #     except:
                #         print('invalid jobid provided')
                #         continue

                #     # if jobid in self.job_task_dict:

                #     #     task = self.job_task_dict[jobid]

                #     #     task.cancel()

                #     #     # Wait for the cancellation of task to be complete
                #     #     try:
                #     #         await task
                #     #     except asyncio.CancelledError:
                #     #         print("main(): cancel_me is cancelled now")

                else:
                    print('invalid option.')

    @final
    async def run(self) -> NoReturn:
        await asyncio.gather(
            self._run_handler(),
            self.run_failure_detection(),
            self.check_user_input(),
            self.send_election_messages()
            )
        raise RuntimeError()
