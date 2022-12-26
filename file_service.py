import asyncssh
import logging
import os, shutil
from typing import List

# SDFS_LOCATION = "/mnt/d/sdfs/" #"./sdfs/"
# SDFS_LOCATION = './sdfs/'
SDFS_LOCATION = '/home/bachina3/MP3/awesomesdfs/sdfs/'
MAX_FILE_VERSIONS = 5

CLEANUP_ON_STARTUP = False

class FileService:
    """Service to handle all file ops and scp commands"""

    def __init__(self) -> None:
        self.current_files = {}
        if CLEANUP_ON_STARTUP:
            self.cleanup_all_files()
        else:
            self.load_files_from_directory()

    def load_files_from_directory(self):
        files = os.listdir(SDFS_LOCATION)
        files.sort(reverse=True)
        for filename in files:
            if not filename.startswith('output_'):
                pos = filename.rfind("_")
                fullname = filename[:pos]
                if fullname in self.current_files:
                    self.current_files[fullname].insert(0, filename)
                else:
                    self.current_files[fullname] = [filename]

    def list_all_files(self):
        files = "filename: [versions]\n"
        for key, value in self.current_files.items():
            files += f"{key}: {value}({len(value)})\n"
        logging.info(f"files stored locally: \n{files}")

    def cleanup_all_files(self):
        for filename in os.listdir(SDFS_LOCATION):
            file_path = os.path.join(SDFS_LOCATION, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                logging.error(f'Failed to delete {file_path}. Reason: {e}')
    
    async def replicate_file(self, host:str, username: str, password: str, file_locations: List[str], filename: str):
        try:
            async with asyncssh.connect(host, username=username, password=password, known_hosts=None) as conn:
                await asyncssh.scp((conn, SDFS_LOCATION + filename + "*"), SDFS_LOCATION)

            self.current_files[filename] = file_locations
            return True
        except (OSError, asyncssh.Error) as exc:
            logging.error(f'Failed to replicate file {filename} from {host}: {str(exc)}')
            return False

    async def download_file(self, host: str, username: str, password: str, file_location: str, filename: str) -> None:  
        destination_file = ""
        if filename in self.current_files:
            # file is already present
            file_list = self.current_files[filename]
            current_latest_filename: str = file_list[-1]
            pos = current_latest_filename.rfind("_")
            version = int(current_latest_filename[pos + len("_version"):])
            destination_file = f"{filename}_version{version + 1}"
        else:
            destination_file = f"{filename}_version1"

        try:
            async with asyncssh.connect(host, username=username, password=password, known_hosts=None) as conn:
                await asyncssh.scp((conn, file_location), SDFS_LOCATION + destination_file)
            
            # saved file successfully add it to the dict
            if filename in self.current_files:
                self.current_files[filename].append(destination_file)
                if len(self.current_files[filename]) > MAX_FILE_VERSIONS:
                    os.remove(SDFS_LOCATION + self.current_files[filename][0])
                    del self.current_files[filename][0]
            else:
                self.current_files[filename] = [destination_file]

            return True
        except (OSError, asyncssh.Error) as exc:
            logging.error(f'Failed to download file {file_location} from {host}: {str(exc)}')
            return False
    
    def get_file_details(self, sdfsfilename):
        response = {"local_store": self.current_files}
        if sdfsfilename in self.current_files:
            response["latest_file"] = self.current_files[sdfsfilename][-1]
            response["all_versions"] = self.current_files[sdfsfilename]
        return response

    def delete_file(self, sdfsfilename):
        deleted = False
        if sdfsfilename in self.current_files:
            files = self.current_files[sdfsfilename]
            for file in files:
                try:
                    os.remove(SDFS_LOCATION + file)
                except FileNotFoundError:
                    print(f"FileNotFoundError: {file}")
            del self.current_files[sdfsfilename]
            deleted = True
        return deleted
    
    def copyfile(self, sdfsfilename, dest):
        return shutil.copy2(SDFS_LOCATION + sdfsfilename, dest)

    async def download_file_to_dest(self, host: str, username: str, password: str, file_location: str, destination_file: str) -> None:  
        # file_location = "/Users/rahul/Q1.jpg"
        try:
            async with asyncssh.connect(host, username=username, password=password, known_hosts=None) as conn:
                await asyncssh.scp((conn, SDFS_LOCATION + file_location), destination_file)
            return True
        except (OSError, asyncssh.Error) as exc:
            logging.error(f'Failed to download file {file_location} from {host}: {str(exc)}')
            return False
