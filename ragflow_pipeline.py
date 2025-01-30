import os
import argparse
import requests
import time
from pathlib import Path
from typing import Set, List, Optional
from tqdm import tqdm

# Configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
SUPPORTED_EXTENSIONS = {'.pdf', '.docx', '.txt', '.md', '.csv'}

class RAGFlowPipeline:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.strip('/')
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
    def get_existing_datasets(self) -> Set[str]:
        """Get list of existing dataset names and their documents"""
        response = requests.get(
            f"{self.base_url}/api/v1/datasets",
            headers=self.headers
        )
        response.raise_for_status()
        return {ds['name']: ds['id'] for ds in response.json()['data']}

    def get_dataset_documents(self, dataset_id: str) -> Set[str]:
        """Get list of document names in a dataset"""
        response = requests.get(
            f"{self.base_url}/api/v1/datasets/{dataset_id}/documents",
            headers=self.headers
        )
        response.raise_for_status()
        return {doc['name'] for doc in response.json()['data']}

    def create_dataset(self, name: str) -> str:
        """Create new dataset if it doesn't exist"""
        existing = self.get_existing_datasets()
        if name in existing:
            print(f"Dataset '{name}' already exists")
            return None
            
        payload = {"name": name}
        response = requests.post(
            f"{self.base_url}/api/v1/datasets",
            headers=self.headers,
            json=payload
        )
        return response.json()['data']['id']

    def upload_file(self, dataset_id: str, file_path: Path):
        """Upload file to RAGFlow dataset"""
        with open(file_path, 'rb') as f:
            response = requests.post(
                f"{self.base_url}/api/v1/datasets/{dataset_id}/documents",
                headers={'Authorization': f'Bearer {self.api_key}'},
                files=[('file', (file_path.name, f, 'multipart/form-data'))]
            )
        return response.json()

    def process_directory(
        self,
        dataset_name: str,
        directory: str,
        allowed_extensions: List[str],
        skip_extensions: List[str]
    ):
        """Main pipeline processing function"""
        # Get or create dataset
        existing_datasets = self.get_existing_datasets()
        dataset_id = existing_datasets.get(dataset_name) or self.create_dataset(dataset_name)
        if not dataset_id:
            return

        # Get existing documents in dataset
        existing_docs = self.get_dataset_documents(dataset_id)
        
        # Prepare file list with progress tracking
        file_paths = []
        for root, _, files in os.walk(directory):
            for filename in files:
                file_path = Path(root) / filename
                ext = file_path.suffix.lower()
                
                # Extension filtering
                if allowed_extensions and ext not in allowed_extensions:
                    continue
                if skip_extensions and ext in skip_extensions:
                    continue
                if ext not in SUPPORTED_EXTENSIONS:
                    continue
                
                # Skip already uploaded files
                if filename in existing_docs:
                    continue
                    
                file_paths.append(file_path)

        # Process files with progress bar
        with tqdm(total=len(file_paths), desc="Uploading files") as pbar:
            for file_path in file_paths:
                try:
                    for attempt in range(MAX_RETRIES):
                        try:
                            result = self.upload_file(dataset_id, file_path)
                            if result['code'] == 0:
                                pbar.update(1)
                                pbar.set_postfix(file=file_path.name, status="OK")
                            else:
                                pbar.set_postfix(file=file_path.name, status=result['message'])
                            break
                        except requests.exceptions.RequestException as e:
                            if attempt < MAX_RETRIES - 1:
                                time.sleep(RETRY_DELAY)
                                continue
                            raise
                except Exception as e:
                    pbar.set_postfix(file=file_path.name, status=f"Failed: {str(e)}")
                    
                time.sleep(0.1)  # Brief pause between uploads

        print(f"\nProcessing complete! Uploaded {len(file_paths)} files to {dataset_name}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='RAGFlow Directory Pipeline')
    parser.add_argument('--api-key',
                        help='RAGFlow API key (can also set RAGFLOW_API_KEY env var)',
                        default=os.environ.get('RAGFLOW_API_KEY'))
    parser.add_argument('--base-url', required=True, help='RAGFlow API base URL')
    parser.add_argument('--dataset', required=True, help='Dataset name')
    parser.add_argument('--directory', required=True, help='Directory to process')
    parser.add_argument('--extensions', nargs='*', help='Allowed file extensions')
    parser.add_argument('--skip-ext', nargs='*', help='Extensions to skip')
    
    args = parser.parse_args()
    
    pipeline = RAGFlowPipeline(
        api_key=args.api_key,
        base_url=args.base_url
    )
    
    pipeline.process_directory(
        dataset_name=args.dataset,
        directory=args.directory,
        allowed_extensions=args.extensions,
        skip_extensions=args.skip_ext
    )