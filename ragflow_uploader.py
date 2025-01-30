import os
import hashlib
import requests
from typing import List, Set
from pathlib import Path

class RAGFlowUploader:
    def __init__(self, api_key: str, base_url: str = "http://localhost"):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
    def create_dataset(self, name: str) -> str:
        """Create or get existing dataset"""
        url = f"{self.base_url}/api/v1/datasets"
        payload = {"name": name}
        
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            response.raise_for_status()
            return response.json()['data']['id']
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:  # Handle duplicate dataset
                datasets = requests.get(url, headers=self.headers).json()['data']
                return next(d['id'] for d in datasets if d['name'] == name)
            raise

    def get_existing_documents(self, dataset_id: str) -> Set[str]:
        """Get set of existing document names to prevent duplicates"""
        url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents"
        response = requests.get(url, headers=self.headers)
        return {doc['name'] for doc in response.json()['data']['docs']}

    def upload_file(self, dataset_id: str, file_path: Path):
        """Upload file with duplicate check"""
        existing = self.get_existing_documents(dataset_id)
        if file_path.name in existing:
            print(f"Skipping existing file: {file_path.name}")
            return

        url = f"{self.base_url}/api/v1/datasets/{dataset_id}/documents"
        with open(file_path, 'rb') as f:
            response = requests.post(
                url,
                files={'file': (file_path.name, f)},
                headers={'Authorization': f'Bearer {self.headers["Authorization"]}'}
            )
        response.raise_for_status()
        print(f"Uploaded: {file_path.name}")
        return response.json()['data'][0]['id']

    def process_directory(
        self,
        dataset_name: str,
        root_dir: str,
        extensions: List[str] = None,
        exclude_dirs: List[str] = None
    ):
        """Main processing pipeline"""
        dataset_id = self.create_dataset(dataset_name)
        doc_ids = []

        for root, dirs, files in os.walk(root_dir):
            # Filter directories
            if exclude_dirs:
                dirs[:] = [d for d in dirs if d not in exclude_dirs]

            for file in files:
                path = Path(root) / file
                if extensions and path.suffix.lower() not in extensions:
                    continue

                try:
                    if doc_id := self.upload_file(dataset_id, path):
                        doc_ids.append(doc_id)
                except Exception as e:
                    print(f"Error processing {file}: {str(e)}")

        # Trigger processing
        if doc_ids:
            url = f"{self.base_url}/api/v1/datasets/{dataset_id}/chunks"
            requests.post(url, json={"document_ids": doc_ids}, headers=self.headers)
            print(f"Started processing {len(doc_ids)} documents")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='RAGFlow Directory Uploader')
    parser.add_argument('directory', help='Directory to upload')
    parser.add_argument('--dataset', default='auto_upload', help='Dataset name')
    parser.add_argument('--extensions', nargs='*', help='Allowed file extensions')
    parser.add_argument('--exclude-dirs', nargs='*', help='Directories to exclude')
    args = parser.parse_args()

    # Get API key from environment
    api_key = os.getenv('RAGFLOW_API_KEY')
    if not api_key:
        raise ValueError("Missing RAGFLOW_API_KEY environment variable")

    uploader = RAGFlowUploader(api_key)
    uploader.process_directory(
        dataset_name=args.dataset,
        root_dir=args.directory,
        extensions=args.extensions,
        exclude_dirs=args.exclude_dirs
    )