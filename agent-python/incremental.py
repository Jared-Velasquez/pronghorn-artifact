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

        # Walk from leaf to root, collecting the chain in reverse order
        path_index = {c.path: c for c in pool}
        chain = []
        current = checkpoint
        while current is not None:
            chain.append(current)
            current = path_index.get(current.parent_path) if current.parent_path else None
        chain.reverse()  # now ordered root -> ... -> leaf

        # Download each entry and create parent symlinks
        prev_local_dir = None
        for entry in chain:
            local_dir = os.path.join(self.base_dir, entry.path)
            os.makedirs(local_dir, exist_ok=True)

            objects = client.list_objects("checkpoints", prefix=entry.path, recursive=True)
            for obj in objects:
                filename = obj.object_name.split("/", maxsplit=1)[1]
                client.fget_object("checkpoints", obj.object_name, os.path.join(local_dir, filename))

            if prev_local_dir is not None:
                parent_name = os.path.basename(prev_local_dir)
                os.symlink(f"../{parent_name}", os.path.join(local_dir, "parent"))

            self.entries.append(local_dir)
            prev_local_dir = local_dir

        self.restore_dir = prev_local_dir
        return self.restore_dir

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

    def get_chain_depth(self, checkpoint: Checkpoint, pool: List[Checkpoint]) -> int:
        """Walk parent_path chain to compute depth of a checkpoint"""

        depth = 0
        current = checkpoint
        path_index = {c.path: c for c in pool}

        while current.parent_path is not None:
            current = path_index[current.parent_path]
            depth += 1

        return depth

    def is_full_dump(self) -> bool:
        """Whether the next dump will be full (no parent) or incremental"""

        # TODO: how can we support performing full dumps in the middle of a chain?
        # Good for performance optimization (e.g. if chain length is 200, might want to do a full dump at 100 instead of incremental dumps for the next 100 checkpoints)
        return len(self.entries) == 0