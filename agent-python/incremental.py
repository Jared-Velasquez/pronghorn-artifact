from typing import List
import os
from pathlib import Path

from orchestration.checkpoint import Checkpoint

from minio import Minio

# https://criu.org/index.php?title=Memory_changes_tracking
# How to track memory changes (pages of 4KB)?
# Step 1: ask the kernel to keep track of memory changes (by writing 4 into /proc/{pid}/clear_refs file for each 
# pid we're interested in)

# after a while...

# Step 2: get the list of modified pages of a process by reading its /proc/{pid}/pagemap file and look at the
# soft-dirty bit in the pagemap entries

class IncrementalChain:
    """Manages a local chain of CRIU checkpoint directories"""

    def __init__(self, base_dir="./chain"):
        self.base_dir = base_dir
        self.entries = []
        self.restore_dir = None
        self.restored_depth = 0

    def setup_for_restore(self, client: Minio, checkpoint: Checkpoint, pool: List[Checkpoint]):
        """Download full ancestor chain from MinIO for this checkpoint

        Walks checkpoint.parent_path up the chain, downloads each entry,
        creates parent symlinks, and sets self.restore_dir to the
        target dentry

        Args:
            client: MinIO client
            checkpoint: target Checkpoint to restore from
            pool: full pool list (to resolve parent_path -> Checkpoint objects)
        Returns:
            str: local directory path for criu restore -D
        
        """

        pass

    def clear_soft_dirty(self, pid):
        """Write 4 to /proc/{pid}/clear_refs to reset page tracking.
        Called immediately after criu restore succeeds"""

        clear_refs_path = Path(f"/proc/{pid}/clear_refs")
        try:
            with open(clear_refs_path, 'w') as f:
                f.write('4')
        except Exception as e:
            print(f"Error clearing soft-dirty bits for pid {pid}: {e}")

    def upload_entry(self, client: Minio, entry_dir: str, minio_path: str):
        """Upload a single chain's entry files to MinIO"""

        # CRIU creates a collection of multiple image files to save the state;
        # assume we store in entry_dir

        if not os.path.isdir(entry_dir):
            raise ValueError(f"Invalid entry_dir {entry_dir}")
        
        for root, dirs, files in os.walk(entry_dir):
            for file in files:
                if file.startswith("pages-") and file.endswith(".img"):
                    local_path = os.path.join(root, file)

                    try:
                        client.fput_object(
                            bucket_name="checkpoints",
                            object_name=os.path.join(minio_path, file),
                            file_path=local_path
                        )
                    except Exception as e:
                        print(f"Error uploading {local_path} to MinIO: {e}")

    def get_entry_size(self, entry_dir):
        """Return total bytes of pages-*.img in entry_dir"""

        if not os.path.isdir(entry_dir):
            raise ValueError(f"Invalid entry_dir {entry_dir}")

        total_size = 0

        for root, dirs, files in os.walk(entry_dir):
            for file in files:
                if file.startswith("pages-") and file.endswith(".img"):
                    # should we use pathlib for this instead?
                    local_path = os.path.join(root, file)
                    total_size += os.path.getsize(local_path) # gets size in bytes

        return total_size

    def get_chain_depth(self, checkpoint: Checkpoint, pool: List[Checkpoint]):
        """Walk parent_path chain to compute depth of a checkpoint"""

        pass

    def is_full_dump(self):
        """Whether the next dump will be full (no parent) or incremental"""

        pass