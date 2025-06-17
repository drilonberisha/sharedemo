import requests
import json
import time
import os
from base64 import b64encode
from typing import Optional, Dict, Any, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logging.basicConfig(
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class AnaplanAPI:
    def __init__(self, email: str, password: str, base_url: str = "https://api.anaplan.com/2/0"):
        """
        Initialize Anaplan API wrapper with credentials.
        
        Args:
            email (str): User email for authentication
            password (str): User password for authentication
            base_url (str): Anaplan API base URL
        """
        self.email = email
        self.password = password
        self.base_url = base_url
        self.token = None
        self.headers = None
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry configuration and backoff logging.
        
        Returns:
            requests.Session: Configured session with retry logic
        """
        session = requests.Session()
        retries = Retry(
            total=60,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET", "PUT"],
            raise_on_status=False
        )

        class CustomRetry(Retry):
            def increment(self, *args, **kwargs):
                logger.info("Retrying request due to failure...")
                return super().increment(*args, **kwargs)

        custom_retries = CustomRetry(
            total=retries.total,
            backoff_factor=retries.backoff_factor,
            status_forcelist=retries.status_forcelist,
            allowed_methods=retries.allowed_methods,
            raise_on_status=retries.raise_on_status
        )
        adapter = HTTPAdapter(max_retries=custom_retries)
        session.mount("https://", adapter)
        return session

    def authenticate(self) -> None:
        """
        Authenticate with Anaplan API using base64-encoded email and password.
        
        Raises:
            requests.RequestException: If authentication fails
        """
        auth_str = b64encode(f"{self.email}:{self.password}".encode()).decode()
        headers = {"Authorization": f"Basic {auth_str}"}
        url = "https://auth.anaplan.com/token/authenticate"
        
        try:
            response = self.session.post(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") != "SUCCESS":
                raise requests.RequestException("Invalid credentials")
            self.token = data["tokenInfo"]["tokenValue"]
            self.headers = {
                "Authorization": f"AnaplanAuthToken {self.token}",
                "Content-Type": "application/json"
            }
            logger.info("Authentication successful")
            
        except requests.RequestException as e:
            logger.error(f"Authentication failed: {e}")
            raise

    def get_workspaces(self) -> Dict[str, Any]:
        """
        Retrieve available workspaces.
        
        Returns:
            Dict[str, Any]: Workspace data
            
        Raises:
            requests.RequestException: If request fails
        """
        if not self.headers:
            self.authenticate()
                
        url = f"{self.base_url}/workspaces"
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            logger.info("Workspaces retrieved successfully")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to get workspaces: {e}")
            raise

    def list_files(self, workspace_id: str, model_id: str) -> List[Dict[str, Any]]:
        """
        List files in a specific workspace and model.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            
        Returns:
            List[Dict[str, Any]]: List of file metadata
            
        Raises:
            requests.RequestException: If request fails
        """
        if not self.headers:
            self.authenticate()
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/files"
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            logger.info(f"Files listed for workspace {workspace_id}, model {model_id}")
            return response.json().get("files", [])
        except requests.RequestException as e:
            logger.error(f"Failed to list files: {e}")
            raise

    def list_processes(self, workspace_id: str, model_id: str) -> List[Dict[str, Any]]:
        """
        List processes in a specific workspace and model.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            
        Returns:
            List[Dict[str, Any]]: List of process metadata
            
        Raises:
            requests.RequestException: If request fails
        """
        if not self.headers:
            self.authenticate()
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/processes"
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            logger.info(f"Processes listed for workspace {workspace_id}, model {model_id}")
            return response.json().get("processes", [])
        except requests.RequestException as e:
            logger.error(f"Failed to list processes: {e}")
            raise

    def get_file_id(self, workspace_id: str, model_id: str, file_name: str) -> str:
        """
        Get file ID by file name.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            file_name (str): Name of the file to find
            
        Returns:
            str: File ID
            
        Raises:
            ValueError: If file not found
            requests.RequestException: If request fails
        """
        files = self.list_files(workspace_id, model_id)
        for file in files:
            if file.get("name") == file_name:
                logger.info(f"Found file ID {file.get('id')} for file {file_name}")
                return file.get("id")
        logger.error(f"File {file_name} not found in workspace {workspace_id}, model {model_id}")
        raise ValueError(f"File {file_name} not found")

    def get_process_id(self, workspace_id: str, model_id: str, process_name: str) -> str:
        """
        Get process ID by process name.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            process_name (str): Name of the process to find
            
        Returns:
            str: Process ID
            
        Raises:
            ValueError: If process not found
            requests.RequestException: If request fails
        """
        processes = self.list_processes(workspace_id, model_id)
        for process in processes:
            if process.get("name") == process_name:
                logger.info(f"Found process ID {process.get('id')} for process {process_name}")
                return process.get("id")
        logger.error(f"Process {process_name} not found in workspace {workspace_id}, model {model_id}")
        raise ValueError(f"Process {process_name} not found")

    def upload_file_chunk(self, workspace_id: str, model_id: str, file_id: str, chunk_number: int, 
                         chunk_data: bytes) -> None:
        """
        Upload a single chunk of a file.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            file_id (str): File ID
            chunk_number (int): Chunk number (0-based index)
            chunk_data (bytes): Chunk data to upload
            
        Raises:
            requests.RequestException: If chunk upload fails
        """
        if not self.headers:
            self.authenticate()
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks/{chunk_number}"
        chunk_headers = self.headers.copy()
        chunk_headers["Content-Type"] = "application/octet-stream"
        
        try:
            response = self.session.put(url, headers=chunk_headers, data=chunk_data, timeout=10)
            response.raise_for_status()
            logger.info(f"Uploaded chunk {chunk_number} for file ID {file_id}")
        except requests.RequestException as e:
            logger.error(f"Failed to upload chunk {chunk_number}: {e}")
            raise

    def upload_file(self, workspace_id: str, model_id: str, file_path: str, file_name: str, 
                    chunk_size: int = 10485760) -> str:
        """
        Upload a file to Anaplan in chunks.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            file_path (str): Local path to the file to upload
            file_name (str): Name of the file in Anaplan
            chunk_size (int): Size of each chunk in bytes (default: 10MB)
            
        Returns:
            str: File ID of the uploaded file
            
        Raises:
            requests.RequestException: If upload fails
            FileNotFoundError: If file_path is invalid
        """
        if not self.headers:
            self.authenticate()
                
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
            raise FileNotFoundError(f"File {file_path} does not exist")
            
        file_size = os.path.getsize(file_path)
        chunk_count = (file_size + chunk_size - 1) // chunk_size
        
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/files"
        payload = {"name": file_name, "chunkCount": chunk_count}
        try:
            response = self.session.post(url, headers=self.headers, json=payload, timeout=10)
            response.raise_for_status()
            file_id = response.json().get("id")
            if not file_id:
                logger.error("Failed to initiate file upload: No file ID returned")
                raise requests.RequestException("No file ID returned")
            logger.info(f"Initiated file upload for {file_name}, file ID: {file_id}")
        except requests.RequestException as e:
            logger.error(f"Failed to initiate file upload: {e}")
            raise
            
        with open(file_path, "rb") as f:
            for chunk_number in range(chunk_count):
                chunk_data = f.read(chunk_size)
                self.upload_file_chunk(workspace_id, model_id, file_id, chunk_number, chunk_data)
                
        complete_url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/complete"
        complete_payload = {"chunkCount": chunk_count}
        try:
            response = self.session.post(complete_url, headers=self.headers, json=complete_payload, timeout=10)
            response.raise_for_status()
            logger.info(f"Completed file upload for {file_name}, file ID: {file_id}")
            return file_id
        except requests.RequestException as e:
            logger.error(f"Failed to complete file upload: {e}")
            raise

    def trigger_process(self, workspace_id: str, model_id: str, process_id: str, locale_name: str = "en_US") -> str:
        """
        Trigger an Anaplan process and return the task ID.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            process_id (str): Process ID to trigger
            locale_name (str): Locale for the process (default: en_US)
            
        Returns:
            str: Task ID
            
        Raises:
            requests.RequestException: If process trigger fails
        """
        if not self.headers:
            self.authenticate()
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks"
        payload = {"localeName": locale_name}
        try:
            response = self.session.post(url, headers=self.headers, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("status", {}).get("message") != "Success":
                logger.error(f"Failed to trigger process: {data.get('status', {}).get('message', 'Unknown error')}")
                raise requests.RequestException(data.get("status", {}).get("message", "Unknown error"))
            task_id = data["task"]["taskId"]
            logger.info(f"Triggered process {process_id}, task ID: {task_id}")
            return task_id
        except requests.RequestException as e:
            logger.error(f"Failed to trigger process: {e}")
            raise

    def monitor_task(self, workspace_id: str, model_id: str, process_id: str, task_id: str, 
                    poll_interval: int = 5, max_attempts: int = 240) -> Dict[str, Any]:
        """
        Monitor task status until completion or failure.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            process_id (str): Process ID
            task_id (str): Task ID to monitor
            poll_interval (int): Seconds between status checks
            max_attempts (int): Maximum number of status checks
            
        Returns:
            Dict[str, Any]: Task status information
            
        Raises:
            requests.RequestException: If task monitoring fails
            TimeoutError: If task monitoring times out
        """
        if not self.headers:
            self.authenticate()
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks/{task_id}"
        
        for attempt in range(max_attempts):
            try:
                response = self.session.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                task_state = data["task"]["taskState"]
                logger.info(f"Task {task_id} status: {task_state}")
                if task_state in ["COMPLETE", "FAILED", "CANCELLED"]:
                    return {
                        "status": task_state,
                        "details": data["task"].get("result", {}),
                        "message": data["task"].get("currentStep", "Task Completed Successfully")
                    }
                time.sleep(poll_interval)
            except requests.RequestException as e:
                logger.error(f"Failed to check task status: {e}")
                raise
                
        logger.error("Task monitoring timed out")
        raise TimeoutError("Task monitoring timed out")

    def execute_sequence(self, workspace_id: str, model_id: str, process_wake_up: str, 
                        file_path: str, file_name: str, process_name: str, chunk_size: int = 10485760,
                        locale_name: str = "en_US") -> None:
        """
        Execute a sequence of operations: wake-up process, file upload, and main process.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            process_wake_up (str): Name of the wake-up process to run first
            file_path (str): Local path to the file to upload
            file_name (str): Name of the file in Anaplan
            process_name (str): Name of the main process to trigger
            chunk_size (int): Size of each chunk in bytes (default: 10MB)
            locale_name (str): Locale for processes (default: en_US)
            
        Raises:
            ValueError: If process or file not found
            requests.RequestException: If any API call fails
            FileNotFoundError: If file_path is invalid
            TimeoutError: If task monitoring times out
        """
        try:
            wake_up_id = self.get_process_id(workspace_id, model_id, process_wake_up)
            wake_up_task_id = self.trigger_process(workspace_id, model_id, wake_up_id, locale_name)
            wake_up_result = self.monitor_task(workspace_id, model_id, wake_up_id, wake_up_task_id)
            if wake_up_result["status"] != "COMPLETE":
                logger.error(f"Wake-up process failed: {wake_up_result['message']}")
                raise requests.RequestException(f"Wake-up process failed: {wake_up_result['message']}")

            file_id = self.upload_file(workspace_id, model_id, file_path, file_name, chunk_size)

            process_id = self.get_process_id(workspace_id, model_id, process_name)
            task_id = self.trigger_process(workspace_id, model_id, process_id, locale_name)
            main_process_result = self.monitor_task(workspace_id, model_id, process_id, task_id)
            if main_process_result["status"] != "COMPLETE":
                logger.error(f"Main process failed: {main_process_result['message']}")
                raise requests.RequestException(f"Main process failed: {main_process_result['message']}")

            logger.info("Sequence executed successfully")
            
        except (ValueError, requests.RequestException, FileNotFoundError, TimeoutError) as e:
            logger.error(f"Sequence execution failed: {e}")
            raise

def main():
    try:
        email = "your.email@example.com"
        password = "your_password"
        workspace_id = "YOUR_WORKSPACE_ID"
        model_id = "YOUR_MODEL_ID"
        process_wake_up = "YOUR_WAKE_UP_PROCESS_NAME"
        file_path = "path/to/your/file.csv"
        file_name = "file.csv"
        process_name = "YOUR_MAIN_PROCESS_NAME"
        
        anaplan = AnaplanAPI(email, password)
        anaplan.execute_sequence(
            workspace_id,
            model_id,
            process_wake_up,
            file_path,
            file_name,
            process_name,
            locale_name="en_US"
        )
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
