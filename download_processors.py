import asyncio
import hashlib
import os
import secrets
import shutil
import urllib.parse
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Tuple, AsyncGenerator, Dict, Any

import aiofiles
import httpx
import libtorrent
from pathvalidate import sanitize_filename
# noinspection PyProtectedMember
from pathvalidate._filename import _DEFAULT_MAX_FILENAME_LEN

from progress_speed import ProgressStream


class BaseSourceProcessor(ABC):
    """Abstract base class for all file sources (HTTP, Torrent, Local)."""
    def __init__(self, processor_type: str, speed_manager, temp_dir: str = "./downloads"):
        self.processor_type = processor_type
        self.speed_manager = speed_manager
        self.temp_dir = temp_dir
        self.session_id = secrets.token_hex(6)
        os.makedirs(self.temp_dir, exist_ok=True)

    @abstractmethod
    async def prepare(self) -> Tuple[str, Optional[int]]:
        """Inspects the source and returns (filename, total_size)."""
        pass

    @abstractmethod
    async def yield_chunks(self, chunk_size_limit: int) -> AsyncGenerator[Tuple[str, int, bool], None]:
        """
        Processes the source and yields ready-to-upload chunk files.
        Yields: (filepath, part_index, is_last_part)
        """
        pass

    @abstractmethod
    def get_processor_metadata(self) -> Dict[str, Any]:
        """
        Returns a dictionary of metadata fields specific to this processor type.
        This dictionary can be appended to files, written to database logs,
        or dynamically injected into Telegram upload captions.
        """
        pass

    @abstractmethod
    async def close(self):
        """Clean up resources."""
        pass


# --- FUTURE STUBS FOR EXPANSION ---

class TorrentProcessor(BaseSourceProcessor):
    """
    Torrents
    """
    def __init__(self, magnet_url: str, speed_manager, temp_dir: str = "./downloads"):
        super().__init__("TorrentProcessor", speed_manager, temp_dir)
        self.magnet_url = magnet_url
        self.torrent_session = libtorrent.session()

        # Configure session settings for DHT and Alerts ---
        settings = self.torrent_session.get_settings()
        settings['enable_dht'] = True  # Required for finding trackerless magnet peers
        settings['alert_mask'] = libtorrent.alert_category.all  # Required for read_piece_alert
        self.torrent_session.apply_settings(settings)

        self.torrent_params = libtorrent.parse_magnet_uri(self.magnet_url)
        self.torrent_info_hash = str(self.torrent_params.info_hashes.get_best())
        self.save_path = os.path.join(temp_dir, self.torrent_info_hash)
        self.torrent_params.save_path = self.save_path
        self.torrent_handle = self.torrent_session.add_torrent(self.torrent_params)
        self.torrent_info: Any = None
        self.torrent_files: Any = []
        self.torrent_total_size = -1

    async def prepare(self):
        print("[TorrentProcessor] Retrieving torrent metadata from peers (this can take a moment)...")
        while not self.torrent_handle.status().has_metadata:
            await asyncio.sleep(1)
        self.torrent_info = self.torrent_handle.torrent_file()
        self.torrent_files = self.torrent_info.files()
        print("[TorrentProcessor] Received torrent metadata from peers.")
        self.torrent_total_size = self.torrent_files.total_size()
        return self.torrent_info.name(), self.torrent_total_size

    async def yield_chunks(self, chunk_size_limit: int):
        total_bytes = 0
        part_index = 1

        num_pieces = self.torrent_info.num_pieces()
        piece_length = self.torrent_info.piece_length()

        while total_bytes < self.torrent_total_size:
            _filename_last_part = f".{self.session_id}.kpart{part_index}"
            part_filepath = os.path.join(self.temp_dir, str(sanitize_filename(f"{self.torrent_info.name()}", max_len=_DEFAULT_MAX_FILENAME_LEN-len(_filename_last_part)))+_filename_last_part)
            start_byte = total_bytes
            end_byte = min(start_byte + chunk_size_limit - 1, self.torrent_total_size - 1)
            chunk_len = end_byte - start_byte + 1
            is_last_part = (end_byte == self.torrent_total_size - 1)

            start_piece = start_byte // self.torrent_info.piece_length()
            end_piece = end_byte // self.torrent_info.piece_length()

            self.speed_manager.download = ProgressStream(total_size=(end_byte - start_byte) + 1)

            print(f"\n[TorrentProcessor] Downloading Part {part_index}; Pieces: {start_piece}-{end_piece} (Range: {start_byte}-{end_byte})...")

            def pre_allocate_kpart():
                os.makedirs(os.path.dirname(part_filepath), exist_ok=True)
                with open(part_filepath, "wb") as f:
                    f.truncate(chunk_len)

            await asyncio.to_thread(pre_allocate_kpart)

            priorities = [0] * num_pieces
            for p in range(start_piece, end_piece + 1):
                priorities[p] = 4  # Normal download priority
            self.torrent_handle.prioritize_pieces(priorities)

            # Stream and assemble piece-by-piece directly to disk as they complete
            target_pieces = list(range(start_piece, end_piece + 1))
            requested_pieces = set()
            written_pieces = set()

            status_init = self.torrent_handle.status()
            initial_payload = status_init.total_payload_download

            while len(written_pieces) < len(target_pieces):
                # Request memory buffers for newly finished pieces
                for p in target_pieces:
                    if p not in requested_pieces and self.torrent_handle.have_piece(p):
                        self.torrent_handle.read_piece(p)
                        if self.speed_manager.download and self.speed_manager.download.total_size == self.speed_manager.download.processed:
                            print(f'\n[TorrentProcessor] requested piece {p}({len(requested_pieces)})')
                        requested_pieces.add(p)

                # Process piece buffer arrivals
                alerts = self.torrent_session.pop_alerts()
                for alert in alerts:
                    if isinstance(alert, libtorrent.read_piece_alert):
                        p = alert.piece

                        # Verify this piece belongs to our active target window
                        if p in target_pieces and p not in written_pieces:
                            piece_data = bytes(alert.buffer)
                            piece_size = alert.size

                            # Calculate the global byte boundaries of this piece
                            piece_start_global = p * piece_length
                            piece_end_global = piece_start_global + piece_size - 1

                            # Find the intersection between the piece and our current chunk boundaries
                            inter_start = max(piece_start_global, start_byte)
                            inter_end = min(piece_end_global, end_byte)

                            if inter_start <= inter_end:
                                # Map the intersection to local coordinates inside the piece buffer
                                slice_local_start = inter_start - piece_start_global
                                slice_local_end = inter_end - piece_start_global
                                slice_bytes = piece_data[slice_local_start: slice_local_end + 1]

                                # Determine the offset within our temporary .kpart file
                                kpart_offset = inter_start - start_byte

                                # Write the slice to the .kpart file on a background thread
                                def write_slice_to_kpart(path, offset, data):
                                    with open(path, "r+b") as f:
                                        f.seek(offset)
                                        f.write(data)

                                await asyncio.to_thread(write_slice_to_kpart, part_filepath, kpart_offset, slice_bytes)
                                if self.speed_manager.download and self.speed_manager.download.total_size == self.speed_manager.download.processed:
                                    print(f'\n[TorrentProcessor] wrote piece {p}({len(written_pieces)})')

                            written_pieces.add(p)
                            # Garbage collect the piece buffer from memory immediately
                            del piece_data

                    # --- AUTO-RECOVERY FOR SEEDING READ ERRORS ---
                    elif isinstance(alert, libtorrent.file_error_alert):
                        msg = alert.message()

                        # When libtorrent fails to read a deleted piece for a peer,
                        # the error message will explicitly contain 'reading' or 'read'
                        if "reading" in msg or "read" in msg or 'file_open' in msg:
                            # print(f"\n[TorrentProcessor] Peer requested a deleted piece. Auto-resuming download...")
                            self.torrent_handle.clear_error()
                            self.torrent_handle.resume()
                        else:
                            # If it's a write error (e.g., Disk Full on current chunk), print the true error
                            print(f"\n[TorrentProcessor] Disk Write/Access Error: {msg}")

                # update the speed counter
                status = self.torrent_handle.status()
                downloaded_bytes_realtime = min(status.total_payload_download - initial_payload, chunk_len)
                self.speed_manager.download.update(downloaded_bytes_realtime, increment=False)
                self.speed_manager.display()

                await asyncio.sleep(0.1)

            print(f"\n[TorrentProcessor] Successfully assembled Part {part_index} at: {part_filepath}")
            self.speed_manager.download = None

            # Releasing the storage of libtorrent
            priorities = self.torrent_handle.get_piece_priorities()
            for p in target_pieces:
                priorities[p] = 0
            self.torrent_handle.prioritize_pieces(priorities)

            # Tell libtorrent to close its OS file handles for this torrent
            self.torrent_handle.flush_cache()

            await asyncio.sleep(1)

            def wipe_raw_files():
                if os.path.exists(self.save_path):
                    for item in os.listdir(self.save_path):
                        item_path = os.path.join(self.save_path, item)
                        try:
                            if os.path.isdir(item_path):
                                shutil.rmtree(item_path)
                                print(f"\n[TorrentProcessor] Deleted scratch folder: {item}")
                            else:
                                os.remove(item_path)
                                print(f"\n[TorrentProcessor] Deleted raw scratch file: {item}")
                        except Exception as e:
                            # Fallback: if OS locks still prevent deletion, truncate file to 0 bytes
                            try:
                                if os.path.isfile(item_path):
                                    with open(item_path, "wb") as f:
                                        f.truncate(0)
                                    print(f"\n[TorrentProcessor] Truncated locked raw file: {item}")
                                else:
                                    print(f"\n[TorrentProcessor] Exception while deleting: {item} > {e}")
                            except Exception as ex:
                                print(f"\n[TorrentProcessor] Could not wipe {item} yet: {ex}")

            # Offload directory scanning and deletion to a background thread
            await asyncio.to_thread(wipe_raw_files)

            yield part_filepath, part_index, is_last_part

            # Increment our global byte cursor and part index
            total_bytes += chunk_len
            part_index += 1

    def get_processor_metadata(self) -> Dict[str, Any]:
        file_index = []
        files_storage = self.torrent_info.files()
        is_v2 = files_storage.v2()  # Check if this is a v2 or hybrid torrent

        for idx in range(files_storage.num_files()):
            file_hash = None

            if is_v2:
                # Retrieve the SHA-256 Merkle root hash
                root_hash = files_storage.root(idx)
                # Convert to string and verify it's not a dummy zero-hash
                if root_hash and str(root_hash) != "0000000000000000000000000000000000000000000000000000000000000000":
                    file_hash = str(root_hash)
            else:
                # Retrieve the optional SHA-1 file hash
                v1_hash = files_storage.hash(idx)
                if v1_hash and str(v1_hash) != "0000000000000000000000000000000000000000":
                    file_hash = str(v1_hash)

            file_index.append({
                "path": files_storage.file_path(idx),
                "size": files_storage.file_size(idx),
                "is_pad": bool(files_storage.file_flags(idx) & files_storage.flag_pad_file),
                "hash": file_hash  # Hexadecimal string or None
            })

        return {
            "magnet_url": self.magnet_url,
            "name": self.torrent_info.name(),
            "file_index": file_index,
        }

    async def close(self):
        pass


class LocalFileProcessor(BaseSourceProcessor):
    """
    Torrents
    """
    def __init__(self, path: str, speed_manager, temp_dir: str = "./downloads"):
        super().__init__("LocalFileProcessor", speed_manager, temp_dir)
        self.name = ""
        self.path = path
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"\n[LocalFileProcessor] Path does not exist: {self.path}")

        # Determine if the target is a file or a directory
        self.is_file = os.path.isfile(self.path)
        # Use the parent directory if path is a file, otherwise use the path itself
        self.base_dir = os.path.dirname(self.path) if self.is_file else self.path

        self.files = self._get_path_info()

    def _get_path_info(self):
        if self.is_file:
            return [self._get_file_info(self.path)]

        file_list = []
        # Walk through the directory
        for root, dirs, files in os.walk(self.path):
            for file in files:
                full_path = os.path.join(root, file)
                file_list.append(self._get_file_info(full_path))
        return file_list

    def _get_file_info(self, full_path):
        # Calculate relative path with respect to the base directory
        relative_path = os.path.relpath(full_path, self.base_dir)

        # Get file size
        size = os.path.getsize(full_path)

        # Calculate SHA256 hash
        sha256_hash = hashlib.sha256()
        try:
            with open(full_path, "rb") as f:
                # Read in chunks to handle large files efficiently
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            file_hash = sha256_hash.hexdigest()
        except (PermissionError, OSError):
            file_hash = "ERROR_READING_FILE"
        return {
            "path": relative_path,
            "size": size,
            "hash": file_hash
        }

    async def prepare(self):
        total_size = 0
        for file in self.files:
            total_size += file['size']
        self.name = os.path.basename(self.path.strip('/'))
        return self.name, total_size

    async def yield_chunks(self, chunk_size_limit: int):
        total_size = sum(file['size'] for file in self.files)
        os.makedirs(self.temp_dir, exist_ok=True)

        # Edge Case: The source contains no files or only 0-byte files
        if total_size == 0:
            part_filepath = os.path.join(self.temp_dir, f"{self.name}.{self.session_id}.kpart1")
            # Create an empty file
            await asyncio.to_thread(lambda: open(part_filepath, "wb").close())
            yield part_filepath, 1, True
            return

        total_written_bytes = 0
        part_index = 1
        bytes_written_in_current_part = 0
        part_filepath = os.path.join(self.temp_dir, f"{self.name}.{self.session_id}.kpart{part_index}")

        # Open the first part file asynchronously
        part_f = await asyncio.to_thread(open, part_filepath, "wb")
        buffer_size = 64 * 1024  # Read/write in efficient 64KB increments

        try:
            for file in self.files:
                # Reconstruct full path using base_dir instead of path
                file_path = os.path.join(self.base_dir, file['path'])
                file_size = file['size']

                # Skip empty/pad files since they don't contribute bytes to the virtual stream
                if file_size == 0:
                    continue

                # Open the source file safely
                in_f = await asyncio.to_thread(open, file_path, "rb")
                try:
                    bytes_read_from_file = 0
                    while bytes_read_from_file < file_size:
                        # Determine boundaries
                        space_left_in_part = chunk_size_limit - bytes_written_in_current_part
                        left_in_file = file_size - bytes_read_from_file

                        to_read = min(buffer_size, space_left_in_part, left_in_file)

                        # Read and write on worker threads to avoid blocking the event loop
                        chunk = await asyncio.to_thread(in_f.read, to_read)
                        if not chunk:
                            break  # Unexpected early EOF

                        await asyncio.to_thread(part_f.write, chunk)

                        bytes_read_from_file += len(chunk)
                        bytes_written_in_current_part += len(chunk)
                        total_written_bytes += len(chunk)

                        # Check if our current .kpart file is full
                        if bytes_written_in_current_part >= chunk_size_limit:
                            await asyncio.to_thread(part_f.close)

                            is_last_part = (total_written_bytes >= total_size)
                            yield part_filepath, part_index, is_last_part

                            if is_last_part:
                                break

                            # Set up the next kpart file
                            part_index += 1
                            bytes_written_in_current_part = 0
                            part_filepath = os.path.join(
                                self.temp_dir,
                                f"{self.name}.{self.session_id}.kpart{part_index}"
                            )
                            part_f = await asyncio.to_thread(open, part_filepath, "wb")

                    # Break the outer loop if we've successfully written everything
                    if total_written_bytes >= total_size:
                        break

                finally:
                    await asyncio.to_thread(in_f.close)

            # Close the last part file if it contains residual bytes and is still open
            if part_f and not getattr(part_f, 'closed', True):
                await asyncio.to_thread(part_f.close)
                yield part_filepath, part_index, True

        except Exception as e:
            # Ensure file descriptors are cleaned up in case of a pipeline crash
            if part_f and not getattr(part_f, 'closed', True):
                await asyncio.to_thread(part_f.close)
            raise e

    def get_processor_metadata(self) -> Dict[str, Any]:
        return {
            "root_path": self.path,
            "name": self.name,
            "file_index": self.files,
        }

    async def close(self):
        pass


import os
import urllib.parse
import asyncio
from typing import Dict, Any, Tuple, Optional, AsyncGenerator
import httpx
import aiofiles


class HttpProcessor(BaseSourceProcessor):
    """
    HttpProcessor
    """

    def __init__(self, url: str, speed_manager, temp_dir: str = "./downloads"):
        super().__init__("HttpProcessor", speed_manager, temp_dir)

        # 1. Hardcode the Google Takeout download URL
        self.url = (
            "https://takeout-download.usercontent.google.com/download/"
            "takeout-20260629T165836Z-3-001.zip?j=e21e1824-e16c-4823-a762-34334f644912"
            "&i=0&user=841068456632&authuser=0"
        )

        # 2. Hardcode your active Session ID to ensure files have identical names across runs
        self.session_id = "e2c4dfff5961"

        # 3. Hardcode the HTTP headers and cookies from the curl command
        self.cookie_string = (
            "__Secure-BUCKET=CJsC; SEARCH_SAMESITE=CgQIlaEB; AEC=AdJVEauvL4dIl7vbhitMxHt9j1ajSa_Ju22zKxYALnXHG48-iFCqYe4gfA; "
            "S=billing-ui-v3=pHtfDFISI3iyYJ1DEBejZaDtcZYUCrV5jVH8ufJZq0s:billing-ui-v3-efe=pHtfDFISI3iyYJ1DEBejZaDtcZYUCrV5jVH8ufJZq0s; "
            "__Secure-1PSIDTS=sidts-CjEByojQU7iQRI1ir9fcVpCoI4thhWF3xbirEYpCf-i2uI88kJ-gGy8RGJUgK8PiSqEEEAA; "
            "__Secure-3PSIDTS=sidts-CjEByojQU7iQRI1ir9fcVpCoI4thhWF3xbirEYpCf-i2uI88kJ-gGy8RGJUgK8PiSqEEEAA; "
            "NID=532=R9-Jyn5uDRhiFU_S8JWBH2zdQHQbTBh3cj2Kgr7WFe1KFq8IR2d95RVyMGEUk3BiLHdxhd0UlY8O7YAeCV-T-2-vICRLtB5G54wQMeH0ppOq"
            "0m7uTAcpMNBPdgpUA2gAuJ5LoQovz2bL7n9pT9BR8-GWcItWufm070Fa9OfuRZ_iELvkiGJO1sjeoYjP6AjDwp8ZvGavM5ECO-pIoHtnjEpCLSdxH0Pg"
            "E1c3jRN02d_esnY2NX6Dm6MYbNu6cNb2w5IrzBjLndslX1pwN8s4OGB8syhkkqssfR0CMLEVaDPFamcmn_3Ncnh9LinmaJa63WrAn0OBQqqs2Xq_tGAP"
            "VemiZkuGA5fiPBPUwMkI3YAnPy8ieKJ_tMjSkae_WegohGJ1HMy3h5DbZnZB1pGFmkSsd1KQlX7be_UtsrlnSIbFbEa9osV9-HSeN1Za888jw6CTVU5a"
            "rSe9Pj_yWL2bwwG1gll-WCoYlVEAQuu_5OXc8sSlVU4nMroeohZO9hRnYd4kttuVpruPST3Xf1balvQ5fE-Wiw1zHyxiN44MspS5t6k67Nb6uCNL23gV"
            "AIeIfU5_meWWorkLcVqWQ8e7ai_W-bYcQi0GT4Ngj-x1dEh6SWSGwgE5JUcUqkYk1hG1yoJfkvuPhlUpTuUk1znWc-MJMHzEd5KfXiOL_tAbBC-X6eAc"
            "y2DL94dw6U6EJrSWwy1yAJ7vlpNO9nrc03poN6tZci2aJi-g2nwuLdXMWN7uNC4Ir_b2WICO216qaZ6O-AShjNWucdDRhYiG4m65fsde1NctZnPnYrxW"
            "J6z4Bpz8FJKPXx86jqRwwgmBy1iz4lHos0brv7hJJVYNHSuCYZPQwobfaV2sP16sx-RxHBr-hIS9BOoISM_ukFp689sV1tTMb5oPD9yMeS-Zm6ZP11cr"
            "KwKCjqNpX3gG6s_h5JvwmAp0Kbbs3e2bA8urK3AdAQS5QKVarjkgSYgMXFZAvSiRROzdbEMSzvHo3tGBOYUNZqZzFlkaRa-B4m7v1C-mr9T3Iq8-uuzr"
            "I_feXiySm-Ch5xKv0-3I1C-U1NXtdarCA63f19683E1ydox5OhNtG0hUT97ujT4NHMh2iptF2ACVYQq750bDmbANq8Le5-OmAm_LFr16IWUjUtjDfE2M"
            "taropAXhhfbgFSY2Vtzgmtnrx7CeP3DgPtCLQ7Y-EvgAFaw-4sVljUSZPCy95kn86nNx178vovifA_2T4ljQip9vEdWKl2UzcEhwuKmF4OXoEpI8gjzh"
            "E_Oh7Cx7xHsyfHP46VEpR6Abt-aT360KNb2Hv0Cdzi-8waZhdpdoju7EWjn8qrn58-jSjSGJE8WpazZTJkXsj5kFvm1ndBolhFMYc2o_Soe_ZzYJfNVy"
            "ZDJl6RyOr66PvGP6mt30U-U4kFsgiYg7Ka694Xf2GNoZ90MCFtCv2f2uwGKmG5UvMynTraaWg3ggdw2x1okJY9M3Wq6v4XKZC5oc2LqgcBp5u-EZn4G5"
            "BfzAlwdJact-mQELIvlWId13VteWQVvAbEZSMJvTeAOsprAs7OZ2KQdi2D51spPb4QGfmch2G6npqsyUgg-XG-6sGJXQAsYAzXjRBNlwoHl_fcWlwoM8"
            "L5fQDVAHABo09I-QNibssR7ACd-EIHr65mzyR6vzT4VFDPsUbpsM9wJct6RkawpVGZvkGDSeCvgwta9hnd1cCwwAtBzyLuHAZP459cDBfOWdjO1jDuXa"
            "3dQE4vvWCYJzvsFYLPCxmxka4QEFiGgwpFePYOEprhaMLM4SxFnkgg9Re6zoEOz_bPw6wXtPa39DLd4K2AsgFsyoAsGaiO-oq2FCKsbx5kx6KrUjxP7u"
            "qLxJh1Ooz3ZwqyKwBRI-SkMrFMFN9DaxqxuJBbxQNcKt5ofW_Htgh_P--gzYIUFuUke3EDLu5-od_cZ-QfznY6Br3qw46_ZuKpn7Tmc_3Ani2z4KvzLW"
            "TqobGBV4iFP2eDhzHshZMYQS5acskluF1PXSNmILlr1NrIDHfdXVIZs4AzFRKZmCJG9IzlYCt24WCUZVee-JdOFB6e6clHxFFCL63DUpp9GI0h0RJcDx"
            "UtvEb4f-fxokB4mSZmattp9A1yPvd8RalVmfvtNdP8fzkMfDEtkVNSiMrWgMAkuVaXvnmMKY-yD2z6zf9KgVj5dmX4KZXlK5WtP1obDOTPxy3y8ADeY"
            "SySRty-GbDdMx0nV5kpCk9QJuYGW-pR3_HfewBzneC9wWneMdL7e6kfJvLZyVPk95OEyJaojRM9X508pFvhBWKcbgZvTtr2gncfBjmwdssESZtiM-5Fr"
            "I6mmbNgeRBbDlnoU5qy4voliM0isiY7-pgESAxJmyPgMJ1lOYawG-DHGWFI4PfIDe5nAzk04ZrA7lEW6Pu1sxuFUcOPmuNAYSeYgZdg; SID=g.a000_"
            "gi4Ki0I2RVHTLnKPM6bg7Mf6mr7csdf9shYSuG-BpKlxdK0b0j_J6EoOCma6SuIYU9e2gACgYKAYQSARcSFQHGX2MiULhSnf5wWlUmKM6hFLw1ARoVAUF"
            "8yKofz2udoq0SG4dXn6RzlWPV0076; __Secure-1PSID=g.a000_gi4Ki0I2RVHTLnKPM6bg7Mf6mr7csdf9shYSuG-BpKlxdK0jgv3NZhulB9iDIBxPB"
            "pzcAACgYKAfESARcSFQHGX2MicFtvhIaQBwZP-XnKYQbDqxoVAUF8yKp5LuQP0uYRlsELpx2VUvaG0076; __Secure-3PSID=g.a000_gi4Ki0I2RVHTL"
            "nKPM6bg7Mf6mr7csdf9shYSuG-BpKlxdK04Nu9bGQ5_zfxmqU5QFnx2AACgYKAQ4SARcSFQHGX2MiXaTbceHheXRD5bWw04TQ5hoVAUF8yKr6GRFQG0bIK"
            "rnP92Fc0nlp0076; HSID=A6eoZJPkK_qJsW1-n; SSID=AABlx6JcLEt4_0FeK; APISID=QGurnIHE0ipzqkPE/AUWnHUCZh-PQSijJB; SAPISID=zAM"
            "11h5Emm_jPABz/AGvvdW7CZa2PoAG45; __Secure-1PAPISID=zAM11h5Emm_jPABz/AGvvdW7CZa2PoAG45; __Secure-3PAPISID=zAM11h5Emm_jP"
            "ABz/AGvvdW7CZa2PoAG45; SIDCC=AKEyXzWKYT3EyTLrSnk3jqsoMWW9MrRPMOFkcwFDypJaJMoZMB7Ggfznw82RUG2ldPtAPSCObfHk; __Secure-1P"
            "SIDCC=AKEyXzWTjLlbYEyXHycXFUlSNjQ9YnRJOLTfK2ZRuRcCqlt0_4mCGilvfF-YFA67aoAOIMtPa-s; __Secure-3PSIDCC=AKEyXzUFBp4I7Mqll1"
            "OVPHj-R_nnmDgyGMQL7qwMAZMk3Eigmgz6-bp6ig5QRWuwlsUn_y3am9w"
        )

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9,uz;q=0.8,ru;q=0.7',
            'priority': 'u=0, i',
            'referer': 'https://takeout.google.com/',
            'sec-ch-ua': '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36',
            'x-browser-channel': 'stable',
            'x-browser-copyright': 'Copyright 2026 Google LLC. All Rights Reserved.',
            'x-browser-validation': 'eJd5Tw+MWpGUJD0D/pwqH9jwh9w=',
            'cookie': self.cookie_string
        }

        # httpx Client remains only for fast metadata inspections (HEAD requests)
        self.client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30.0)
        self.supports_ranges = False
        self.filename = None
        self.total_size = None
        self.buffer_size = 64 * 1024

    async def close(self):
        await self.client.aclose()

    def get_processor_metadata(self) -> Dict[str, Any]:
        """Returns HTTP-specific metadata fields."""
        return {
            "url": self.url,
            "supports_ranges": self.supports_ranges,
            "filename": self.filename,
        }

    async def prepare(self) -> Tuple[str, Optional[int]]:
        """Inspects the server for filename, size, and range support."""
        try:
            print('[HttpProcessor] Inspecting headers...')

            self.supports_ranges = True
            self.total_size = 33432001557
            self.filename = "takeout-20260629T165836Z-3-001.zip"

            response = await self.client.head(self.url)

            # Parse Filename
            content_disp = response.headers.get("content-disposition", "")
            if "filename=" in content_disp:
                self.filename = content_disp.split("filename=")[1].strip('"\'')

            # Parse Size
            content_length = response.headers.get("content-length")
            if content_length:
                self.total_size = int(content_length)

        except Exception as e:
            print(f"[HttpProcessor] Metadata issue: {e}. Falling back to default hardcoded details.")
            self.filename = self.filename or "takeout-20260629T165836Z-3-001.zip"
            self.supports_ranges = True
            self.total_size = 33432001557

        return self.filename, self.total_size

    async def _download_range(self, filepath: str, start: int, end: int) -> Optional[int]:
        """Internal helper for robust range downloading using system curl for perfect reliability."""
        expected_size = (end - start) + 1

        # Check if we already have a partial file from a previous interrupted attempt
        existing_bytes = 0
        if os.path.exists(filepath):
            existing_bytes = os.path.getsize(filepath)

            # Truncation check to ensure byte boundaries are strictly aligned
            if existing_bytes > expected_size:
                print(
                    f"[HttpProcessor] Partial file is larger than expected size ({existing_bytes} > {expected_size}). Truncating to {expected_size}...")
                with open(filepath, "r+b") as f:
                    f.truncate(expected_size)
                existing_bytes = expected_size

            print(
                f"[HttpProcessor] Found partial file ({existing_bytes / (1024 * 1024):.2f} MB). Resuming from byte {start + existing_bytes}...")

        if existing_bytes >= expected_size:
            return existing_bytes

        self.speed_manager.download = ProgressStream(total_size=expected_size)
        self.speed_manager.download.update(existing_bytes)

        retries = 15  # generous retries for network drops
        attempt = 0

        while existing_bytes < expected_size and attempt < retries:
            current_start = start + existing_bytes

            # Formulate the curl command targeting the exact sub-range slice
            cmd = [
                "curl",
                "-sL",  # Silent, follow redirects [1.1.9]
                "--connect-timeout", "30",
                "-r", f"{current_start}-{end}",  # Request specific byte range
                "-b", self.cookie_string,  # Pass cookies
                "-H",
                "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "-H", "accept-language: en-US,en;q=0.9,uz;q=0.8,ru;q=0.7",
                "-H", "priority: u=0, i",
                "-H", "referer: https://takeout.google.com/",
                "-H", "sec-ch-ua: \"Chromium\";v=\"148\", \"Google Chrome\";v=\"148\", \"Not/A)Brand\";v=\"99\"",
                "-H", "sec-ch-ua-mobile: ?0",
                "-H", "sec-ch-ua-platform: \"Linux\"",
                "-H", "upgrade-insecure-requests: 1",
                "-H",
                "user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
                "-H", "x-browser-channel: stable",
                "-H", "x-browser-copyright: Copyright 2026 Google LLC. All Rights Reserved.",
                "-H", "x-browser-validation: eJd5Tw+MWpGUJD0D/pwqH9jwh9w=",
                "-H", "x-browser-year: 2026",
                "-H", "x-client-data: CJC2yQEIo7bJAQipncoBCO7kygEIkqHLAQiHoM0BCOzJlDAIxc+UMAj10ZQwCKbUlDA=",
                self.url
            ]

            try:
                # Spawn curl subprocess to handle network transfer
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                # Stream curl's stdout directly to our local file chunk, updating progress in Python
                async with aiofiles.open(filepath, "ab") as f:
                    while True:
                        chunk = await process.stdout.read(self.buffer_size)
                        if not chunk:
                            break
                        await f.write(chunk)
                        existing_bytes += len(chunk)
                        if self.speed_manager.download:
                            self.speed_manager.download.update(len(chunk))
                            self.speed_manager.display()

                await process.wait()

                # Treat non-zero exit codes from curl (such as code 18/56) as a transient drop
                if process.returncode != 0:
                    err_msg = (await process.stderr.read()).decode().strip()
                    raise RuntimeError(f"curl exited with code {process.returncode}: {err_msg}")

                if existing_bytes < expected_size:
                    print(
                        f"\n[HttpProcessor] Stream completed prematurely ({existing_bytes}/{expected_size} bytes). Reconnecting...")
                    attempt += 1
                    await asyncio.sleep(2.0)
                else:
                    break

            except Exception as e:
                print(f"\n[HttpProcessor] Issue on download attempt {attempt + 1}/{retries}: {e}")
                attempt += 1
                if attempt >= retries:
                    raise
                await asyncio.sleep(2.0)

        self.speed_manager.display(force=True)
        self.speed_manager.download = None
        print()
        return existing_bytes

    async def yield_chunks(self, chunk_size_limit: int) -> AsyncGenerator[Tuple[str, int, bool], None]:
        """Encapsulates both Range-supported and Streaming downloads."""

        # --- PATH A: SERVER SUPPORTS RANGE REQUESTS ---
        if self.supports_ranges and self.total_size:

            # Scan local folder to find the lowest incomplete/existing part number.
            start_part_index = 1
            existing_parts = []
            if os.path.exists(self.temp_dir):
                for f in os.listdir(self.temp_dir):
                    if f.startswith(f"{self.filename}.{self.session_id}.kpart"):
                        try:
                            part_num = int(f.split(".kpart")[-1])
                            existing_parts.append(part_num)
                        except ValueError:
                            pass

            if existing_parts:
                start_part_index = min(existing_parts)
                print(
                    f"[HttpProcessor] Detected existing parts. Skipping completed ones and resuming pipeline from Part {start_part_index}...")

            total_bytes = 0
            part_index = 1
            while total_bytes < self.total_size:
                part_filepath = os.path.join(self.temp_dir, f"{self.filename}.{self.session_id}.kpart{part_index}")
                start_byte = total_bytes
                end_byte = min(start_byte + chunk_size_limit - 1, self.total_size - 1)
                is_last_part = (end_byte == self.total_size - 1)

                # Skip any part that is earlier than our active resuming part
                if part_index < start_part_index:
                    total_bytes += (end_byte - start_byte) + 1
                    part_index += 1
                    continue

                print(f"[HttpProcessor] Downloading Part {part_index} (Range: {start_byte}-{end_byte})...")
                downloaded_bytes = await self._download_range(part_filepath, start_byte, end_byte)
                total_bytes += downloaded_bytes

                yield part_filepath, part_index, is_last_part
                part_index += 1

        # --- PATH B: NO RANGE SUPPORT / UNKNOWN SIZE ---
        else:
            print("[HttpProcessor] Streaming continuously...")
            part_index = 1
            part_filepath = os.path.join(self.temp_dir, f"{self.filename}.{self.session_id}.kpart{part_index}")

            async with self.client.stream("GET", self.url) as response:
                response.raise_for_status()
                bytes_written = 0
                f = await aiofiles.open(part_filepath, "wb")
                self.speed_manager.download = ProgressStream(total_size=chunk_size_limit)

                try:
                    async for chunk in response.aiter_bytes(chunk_size=self.buffer_size):
                        await f.write(chunk)
                        bytes_written += len(chunk)
                        self.speed_manager.download.update(len(chunk))
                        self.speed_manager.display()

                        if bytes_written >= chunk_size_limit:
                            self.speed_manager.display(force=True)
                            self.speed_manager.download = None
                            await f.close()

                            yield part_filepath, part_index, False

                            part_index += 1
                            bytes_written = 0
                            part_filepath = os.path.join(self.temp_dir,
                                                         f"{self.filename}.{self.session_id}.kpart{part_index}")
                            f = await aiofiles.open(part_filepath, "wb")
                            self.speed_manager.download = ProgressStream(total_size=chunk_size_limit)
                finally:
                    if not f.closed:
                        await f.close()

                if bytes_written > 0:
                    self.speed_manager.display(force=True)
                    self.speed_manager.download = None
                    yield part_filepath, part_index, True
                else:
                    if os.path.exists(part_filepath): os.remove(part_filepath)
