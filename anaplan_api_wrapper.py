import requests
import json
import time
import os
from typing import Optional, Dict, Any, List

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

    def authenticate(self) -> bool:
        """
        Authenticate with Anaplan API using email and password.
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        url = "https://auth.anaplan.com/token/authenticate"
        payload = {
            "username": self.email,
            "password": self.password
        }
        
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "SUCCESS":
                self.token = data["tokenInfo"]["tokenValue"]
                self.headers = {
                    "Authorization": f"AnaplanAuthToken {self.token}",
                    "Content-Type": "application/json"
                }
                return True
            return False
            
        except requests.RequestException as e:
            print(f"Authentication failed: {e}")
            return False

    def get_workspaces(self) -> Optional[Dict[str, Any]]:
        """
        Retrieve available workspaces.
        
        Returns:
            Optional[Dict[str, Any]]: Workspace data or None if request fails
        """
        if not self.headers:
            if not self.authenticate():
                return None
                
        url = f"{self.base_url}/workspaces"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Failed to get workspaces: {e}")
            return None

    def list_files(self, workspace_id: str, model_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        List files in a specific workspace and model.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            
        Returns:
            Optional[List[Dict[str, Any]]]: List of file metadata or None if request fails
        """
        if not self.headers:
            if not self.authenticate():
                return None
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/files"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            return data.get("files", [])
        except requests.RequestException as e:
            print(f"Failed to list files: {e}")
            return None

    def list_processes(self, workspace_id: str, model_id: str) -> Optional[List[Dict[str, Any]]]:
        """
        List processes in a specific workspace and model.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            
        Returns:
            Optional[List[Dict[str, Any]]]: List of process metadata or None if request fails
        """
        if not self.headers:
            if not self.authenticate():
                return None
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/processes"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            return data.get("processes", [])
        except requests.RequestException as e:
            print(f"Failed to list processes: {e}")
            return None

    def get_file_id(self, workspace_id: str, model_id: str, file_name: str) -> Optional[str]:
        """
        Get file ID by file name.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            file_name (str): Name of the file to find
            
        Returns:
            Optional[str]: File ID if found, None otherwise
        """
        files = self.list_files(workspace_id, model_id)
        if files:
            for file in files:
                if file.get("name") == file_name:
                    return file.get("id")
        return None

    def get_process_id(self, workspace_id: str, model_id: str, process_name: str) -> Optional[str]:
        """
        Get process ID by process name.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            process_name (str): Name of the process to find
            
        Returns:
            Optional[str]: Process ID if found, None otherwise
        """
        processes = self.list_processes(workspace_id, model_id)
        if processes:
            for process in processes:
                if process.get("name") == process_name:
                    return process.get("id")
        return None

    def upload_file_chunk(self, workspace_id: str, model_id: str, file_id: str, chunk_number: int, 
                         chunk_data: bytes) -> bool:
        """
        Upload a single chunk of a file.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            file_id (str): File ID
            chunk_number (int): Chunk number (0-based index)
            chunk_data (bytes): Chunk data to upload
            
        Returns:
            bool: True if chunk upload successful, False otherwise
        """
        if not self.headers:
            if not self.authenticate():
                return False
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks/{chunk_number}"
        chunk_headers = self.headers.copy()
        chunk_headers["Content-Type"] = "application/octet-stream"
        
        try:
            response = requests.put(url, headers=chunk_headers, data=chunk_data)
            response.raise_for_status()
            return True
        except requests.RequestException as e:
            print(f"Failed to upload chunk {chunk_number}: {e}")
            return False

    def upload_file(self, workspace_id: str, model_id: str, file_path: str, 
                    chunk_size: int = 10485760) -> Optional[Dict[str, Any]]:
        """
        Upload a file to Anaplan in chunks.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            file_path (str): Local path to the file to upload
            chunk_size (int): Size of each chunk in bytes (default: 10MB)
            
        Returns:
            Optional[Dict[str, Any]]: File upload result or None if upload fails
        """
        if not self.headers:
            if not self.authenticate():
                return None
                
        # Get file name and size
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        # Initiate file upload
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/files"
        payload = {"name": file_name}
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            file_id = response.json().get("id")
            if not file_id:
                print("Failed to initiate file upload: No file ID returned")
                return None
        except requests.RequestException as e:
            print(f"Failed to initiate file upload: {e}")
            return None
            
        # Read and upload file in chunks
        chunk_count = (file_size + chunk_size - 1) // chunk_size
        with open(file_path, "rb") as f:
            for chunk_number in range(chunk_count):
                chunk_data = f.read(chunk_size)
                if not self.upload_file_chunk(workspace_id, model_id, file_id, chunk_number, chunk_data):
                    print(f"Upload aborted at chunk {chunk_number}")
                    return None
                
        # Complete file upload
        complete_url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/complete"
        try:
            response = requests.post(complete_url, headers=self.headers, json={})
            response.raise_for_status()
            return {
                "file_id": file_id,
                "file_name": file_name,
                "chunk_count": chunk_count,
                "status": "COMPLETED"
            }
        except requests.RequestException as e:
            print(f"Failed to complete file upload: {e}")
            return None

    def trigger_process(self, workspace_id: str, model_id: str, process_id: str) -> Optional[str]:
        """
        Trigger an Anaplan process and return the task ID.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            process_id (str): Process ID to trigger
            
        Returns:
            Optional[str]: Task ID if successful, None otherwise
        """
        if not self.headers:
            if not self.authenticate():
                return None
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks"
        try:
            response = requests.post(url, headers=self.headers, json={})
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "SUCCESS":
                return data["task"]["taskId"]
            return None
            
        except requests.RequestException as e:
            print(f"Failed to trigger process: {e}")
            return None

    def monitor_task(self, workspace_id: str, model_id: str, process_id: str, task_id: str, 
                    poll_interval: int = 5, max_attempts: int = 60) -> Dict[str, Any]:
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
        """
        if not self.headers:
            if not self.authenticate():
                return {"status": "FAILED", "message": "Authentication failed"}
                
        url = f"{self.base_url}/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks/{task_id}"
        
        for attempt in range(max_attempts):
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                
                task_state = data["task"]["taskState"]
                if task_state in ["COMPLETE", "FAILED", "CANCELLED"]:
                    return {
                        "status": task_state,
                        "details": data["task"].get("result", {}),
                        "message": data["task"].get("statusMessage", "")
                    }
                    
                time.sleep(poll_interval)
                
            except requests.RequestException as e:
                print(f"Failed to check task status: {e}")
                return {"status": "ERROR", "message": str(e)}
                
        return {"status": "TIMEOUT", "message": "Task monitoring timed out"}

    def execute_sequence(self, workspace_id: str, model_id: str, process_wake_up: str, 
                        file_path: str, process_name: str, chunk_size: int = 10485760) -> Dict[str, Any]:
        """
        Execute a sequence of operations: wake-up process, file upload, and main process.
        
        Args:
            workspace_id (str): Workspace ID
            model_id (str): Model ID
            process_wake_up (str): Name of the wake-up process to run first
            file_path (str): Local path to the file to upload
            process_name (str): Name of the main process to trigger
            chunk_size (int): Size of each chunk in bytes for file upload (default: 10MB)
            
        Returns:
            Dict[str, Any]: Result of the sequence execution
        """
        result = {"steps": {}, "success": True, "message": ""}
        
        # Step 1: Run wake-up process
        wake_up_id = self.get_process_id(workspace_id, model_id, process_wake_up)
        if not wake_up_id:
            result["steps"]["wake_up"] = {"status": "FAILED", "message": f"Wake-up process '{process_wake_up}' not found"}
            result["success"] = False
            return result
            
        wake_up_task_id = self.trigger_process(workspace_id, model_id, wake_up_id)
        if not wake_up_task_id:
            result["steps"]["wake_up"] = {"status": "FAILED", "message": "Failed to trigger wake-up process"}
            result["success"] = False
            return result
            
        wake_up_result = self.monitor_task(workspace_id, model_id, wake_up_id, wake_up_task_id)
        result["steps"]["wake_up"] = wake_up_result
        if wake_up_result["status"] != "COMPLETE":
            result["success"] = False
            result["message"] = f"Wake-up process failed: {wake_up_result['message']}"
            return result
            
        # Step 2: Upload file
        file_name = os.path.basename(file_path)
        upload_result = self.upload_file(workspace_id, model_id, file_path, chunk_size)
        if not upload_result:
            result["steps"]["file_upload"] = {"status": "FAILED", "message": f"Failed to upload file '{file_name}'"}
            result["success"] = False
            return result
        result["steps"]["file_upload"] = upload_result
        
        # Step 3: Trigger main process
        process_id = self.get_process_id(workspace_id, model_id, process_name)
        if not process_id:
            result["steps"]["main_process"] = {"status": "FAILED", "message": f"Main process '{process_name}' not found"}
            result["success"] = False
            return result
            
        task_id = self.trigger_process(workspace_id, model_id, process_id)
        if not task_id:
            result["steps"]["main_process"] = {"status": "FAILED", "message": "Failed to trigger main process"}
            result["success"] = False
            return result
            
        main_process_result = self.monitor_task(workspace_id, model_id, process_id, task_id)
        result["steps"]["main_process"] = main_process_result
        if main_process_result["status"] != "COMPLETE":
            result["success"] = False
            result["message"] = f"Main process failed: {main_process_result['message']}"
        
        return result

def main():
    # Example usage
    email = "your.email@example.com"
    password = "your_password"
    
    # Initialize API wrapper
    anaplan = AnaplanAPI(email, password)
    
    # Example parameters (replace with actual values)
    workspace_id = "YOUR_WORKSPACE_ID"
    model_id = "YOUR_MODEL_ID"
    process_wake_up = "YOUR_WAKE_UP_PROCESS_NAME"
    file_path = "path/to/your/file.csv"
    process_name = "YOUR_MAIN_PROCESS_NAME"
    
    # Execute sequence
    result = anaplan.execute_sequence(workspace_id, model_id, process_wake_up, file_path, process_name)
    
    # Print results
    print("\nSequence Execution Results:")
    print(f"Overall Success: {result['success']}")
    if result["message"]:
        print(f"Message: {result['message']}")
        
    print("\nStep Details:")
    for step, details in result["steps"].items():
        print(f"{step}:")
        print(f"  Status: {details['status']}")
        if "message" in details:
            print(f"  Message: {details['message']}")
        if "file_id" in details:
            print(f"  File ID: {details['file_id']}")
        if "chunk_count" in details:
            print(f"  Chunks Uploaded: {details['chunk_count']}")

if __name__ == "__main__":
    main()
