from minio import Minio

class IncrementalChain:
    """Manages a local chain of CRIU checkpoint directories"""

    def __init__(self, base_dir="./chain"):
        self.base_dir = base_dir
        self.entries = []
        self.restore_dir = None
        self.restored_depth = 0

    def setup_for_restore(self, client, checkpoint, pool):
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

        pass

    def upload_entry(self, client: Minio, entry_dir, minio_path):
        """Upload a single chain's entry files to MinIO"""

        pass

    def get_entry_size(self, entry_dir):
        """Return total bytes of pages-*.img in entry_dir"""

        pass

    def get_chain_depth(self, checkpoint, pool):
        """Walk parent_path chain to compute depth of a checkpoint"""

        pass

    def is_full_dump(self):
        """Whether the next dump will be full (no parent) or incremental"""

        pass