
from .filesystem import FileSystemStore

def resolve_store(uri: str):
    if uri.startswith("s3://"):
        # Future: return S3Store()
        raise NotImplementedError("S3 output not implemented yet. Please use a local path.")
    return FileSystemStore()
