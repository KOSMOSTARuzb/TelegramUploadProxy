import argparse
import hashlib
import os
import re
import json
import shutil

# Regex to match the kpart naming structure: [filename].[12-char-session-id].kpart[index]
KPART_REGEX = re.compile(r"^(.*?)\.([a-f0-9A-Z]{12})\.kpart(\d+)$")


class SessionVerifier:
    def __init__(self, target_dir: str):
        self.target_dir = target_dir

    def scan_sessions(self) -> dict:
        """
        Scans the directory for .kpart files and groups them by Session ID.
        """
        sessions = {}
        if not os.path.exists(self.target_dir):
            return sessions

        for entry in os.scandir(self.target_dir):
            if entry.is_file():
                match = KPART_REGEX.match(entry.name)
                if match:
                    filename_prefix, session_id, part_index = match.groups()
                    part_index = int(part_index)

                    if session_id not in sessions:
                        sessions[session_id] = {
                            "session_id": session_id,
                            "filename_prefix": filename_prefix,
                            "parts_found": {},
                        }

                    sessions[session_id]["parts_found"][part_index] = entry.path

        # Now validate and parse metadata for each session
        for session_id, data in sessions.items():
            self._verify_and_populate_metadata(data)

        return sessions

    # noinspection PyMethodMayBeStatic
    def _verify_and_populate_metadata(self, data: dict):
        """
        Extracts metadata from the highest available part index and checks for completeness.
        """
        parts_found = data["parts_found"]
        data["status"] = "VALID"
        data["error_msg"] = ""
        data["metadata"] = None
        data["original_filename"] = data["filename_prefix"]

        if not parts_found:
            data["status"] = "INVALID"
            data["error_msg"] = "No parts discovered on disk."
            return

        # 1. Identify the highest part on disk to inspect for metadata
        highest_part_index = max(parts_found.keys())
        highest_part_path = parts_found[highest_part_index]

        # 2. Extract and parse trailing metadata
        try:
            with open(highest_part_path, "rb") as f:
                f.seek(-4, os.SEEK_END)
                meta_len_bytes = f.read(4)
                meta_len = int.from_bytes(meta_len_bytes, byteorder="big")

                file_size = os.path.getsize(highest_part_path)
                if meta_len <= 0 or meta_len > (file_size - 4):
                    raise ValueError("Metadata length exceeds file boundary.")

                f.seek(-(4 + meta_len), os.SEEK_END)
                meta_bytes = f.read(meta_len)
                metadata = json.loads(meta_bytes.decode("utf-8"))

                required_fields = [
                    "processor_type", "total_parts"
                ]
                if "processor_type" in metadata:
                    data['processor_type']: str = metadata['processor_type'].lower()
                    if data['processor_type'] == 'torrentprocessor':
                        required_fields.append("file_index")

                # Check required fields
                if not all(field in metadata for field in required_fields):
                    raise KeyError("Required metadata key is missing.")


                data["metadata"] = metadata
                data["original_filename"] = metadata["filename"] if 'filename' in metadata else metadata['name']
                data["metadata_len"] = meta_len
                total_parts_expected = int(metadata["total_parts"])

        except Exception as e:
            data["status"] = "INVALID"
            data["error_msg"] = f"Failed to parse trailing metadata ({str(e)})"
            return

        # 3. Check for missing sequence parts
        missing_parts = []
        for i in range(1, total_parts_expected + 1):
            if i not in parts_found:
                missing_parts.append(i)

        if missing_parts:
            data["status"] = "INCOMPLETE"
            data["error_msg"] = f"Missing parts: {', '.join(map(str, missing_parts))}"
            return

        # 4. Check if we have extra parts that exceed the metadata specification
        if highest_part_index > total_parts_expected:
            data["status"] = "INVALID"
            data[
                "error_msg"] = f"Part boundary mismatch (Expected: {total_parts_expected}, Found: {highest_part_index})"
            return


def perform_merge_http(session_data: dict, output_dir: str):
    """
    Sequentially stitches chunks together, stripping metadata safely from the final part.
    """
    original_name = session_data["original_filename"]
    out_filepath = os.path.join(output_dir, original_name)
    parts = session_data["parts_found"]
    total_parts = session_data["metadata"]["total_parts"]
    metadata_len = session_data["metadata_len"]

    print(f"\n[Merger] Stitching file: {original_name}")
    print(f"[Merger] Destination: {out_filepath}")

    buffer_size = 10 * 1024 * 1024  # 10 MB RAM buffer for high-speed local merging

    with open(out_filepath, "wb") as out_f:
        for i in range(1, total_parts + 1):
            part_path = parts[i]
            part_size = os.path.getsize(part_path)

            print(f" -> Writing Part {i}/{total_parts}...", end="", flush=True)

            with open(part_path, "rb") as in_f:
                if i < total_parts:
                    # Write the entire part directly
                    while True:
                        chunk = in_f.read(buffer_size)
                        if not chunk:
                            break
                        out_f.write(chunk)
                else:
                    # Write the final part up to the metadata boundary
                    bytes_to_write = part_size - metadata_len - 4
                    written = 0
                    while written < bytes_to_write:
                        chunk_to_read = min(buffer_size, bytes_to_write - written)
                        chunk = in_f.read(chunk_to_read)
                        if not chunk:
                            break
                        out_f.write(chunk)
                        written += len(chunk)
            print(" Done")

    print(f"[Merger] Reassembly successfully finished!")

def verify_file_integrity(filepath: str, expected_hash: str) -> bool:
    """
    Computes the hash of the assembled file and compares it with the expected value.
    Reads in memory-safe chunks of 64 KB.
    """
    if len(expected_hash) == 64:
        hasher = hashlib.sha256()
    elif len(expected_hash) == 40:
        hasher = hashlib.sha1()
    else:
        return False  # Unsupported hash length

    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(64 * 1024)
            if not chunk:
                break
            hasher.update(chunk)

    return hasher.hexdigest().lower() == expected_hash.lower()

def perform_merge_file_tree(session_data: dict, output_dir: str):
    """
    Sequentially stitches chunks together, stripping metadata safely from the final part.
    """
    original_name = session_data["original_filename"]
    out_filepath = os.path.join(output_dir, original_name)
    parts = session_data["parts_found"]
    total_parts = session_data["metadata"]["total_parts"]
    metadata_len = session_data["metadata_len"]

    print(f"\n[Merger] Stitching directory: {original_name}")
    print(f"[Merger] Destination: {out_filepath}")

    buffer_size = 10 * 1024 * 1024  # 10 MB RAM buffer for high-speed local merging

    # --- 2. Initialize Stream State (Persists across the entire file tree) ---
    part_idx = 1
    part_file = None
    part_bytes_remaining = 0

    def open_next_part():
        nonlocal part_idx, part_file, part_bytes_remaining
        if part_file:
            part_file.close()

        if part_idx <= total_parts:
            part_path = parts[part_idx]
            part_file = open(part_path, "rb")
            part_size = os.path.getsize(part_path)

            # If we are on the final part, strip the trailing metadata safely
            if part_idx == total_parts:
                part_bytes_remaining = max(0, part_size - metadata_len)
            else:
                part_bytes_remaining = part_size

            part_idx += 1
        else:
            part_file = None
            part_bytes_remaining = 0

    # Open the first part to prime the stream
    open_next_part()

    def read_chunk_from_stream(n: int) -> bytes:
        """
        Reads up to n bytes from the contiguous stream of parts.
        Transitions to the next part automatically.
        """
        nonlocal part_file, part_bytes_remaining
        if not part_file:
            return b""

        # If the current part has been exhausted, move to the next kpart
        if part_bytes_remaining <= 0:
            open_next_part()
            if not part_file:
                return b""

        to_read = min(n, part_bytes_remaining)
        chunk = part_file.read(to_read)
        if chunk:
            part_bytes_remaining -= len(chunk)
        return chunk

    # --- 3. Process each file in the index sequentially ---
    for file in session_data["metadata"]["file_index"]:
        file_path: str = str(file.get("path"))
        file_size: int = int(file.get("size") or 0)
        is_file_pad: bool = file.get("is_pad", False)

        target_path = os.path.join(out_filepath, file_path)

        # CASE A: Skip pad files sequentially in the binary stream
        if is_file_pad:
            read_bytes = 0
            while read_bytes < file_size:
                to_read = min(buffer_size, file_size - read_bytes)
                chunk = read_chunk_from_stream(to_read)
                if not chunk:
                    break
                read_bytes += len(chunk)
            print(f"[Merger] Skipped pad file: {file_path} ({file_size:,} bytes)")
            continue

        # CASE B: Reassemble payload files
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        read_bytes = 0

        with open(target_path, "wb") as out_f:
            while read_bytes < file_size:
                # Read in memory-safe increments (buffer_size or whatever is left of the file)
                to_read = min(buffer_size, file_size - read_bytes)
                chunk = read_chunk_from_stream(to_read)
                if not chunk:
                    break  # Safety break for early EOF
                out_f.write(chunk)
                read_bytes += len(chunk)

        print(f"[Merger] Reassembled: {file_path} ({file_size:,} bytes)")
        # Verify the file if a metadata hash is available
        expected_hash = file.get("hash")
        if expected_hash:
            # Run the verification on a background thread to prevent thread blocking
            is_valid = verify_file_integrity(target_path, expected_hash)
            if is_valid:
                print(f" -> [OK] Cryptographic hash matches!")
            else:
                print(f" -> [CORRUPT] Hash mismatch for assembled file: {file_path}")
                print(expected_hash)

    # Ensure last part file handle is cleanly closed
    if part_file:
        part_file.close()

    print(f"[Merger] Reassembly successfully finished!")


def run_interactive_selection(sessions: dict) -> str | None:
    """
    Displays a table of found sessions, highlighting incomplete/invalid sessions,
    and blocks the selection of bad sessions.
    """
    session_list = list(sessions.values())

    print("\n" + "=" * 80)
    print("                      AVAILABLE SESSIONS FOR RECONSTRUCTION ")
    print("=" * 80)

    selectable_indices = []

    for idx, s in enumerate(session_list, 1):
        status = s["status"]
        status_color = ""
        # Handle status labeling safely
        if status == "VALID":
            status_color = "[  VALID   ]"
            selectable_indices.append(idx)
        elif status == "INCOMPLETE":
            status_color = "[INCOMPLETE]"
        else:
            status_color = "[ INVALID  ]"

        filename = s["original_filename"]
        parts_count = len(s["parts_found"])

        # Display session details
        print(f"[{idx}] {status_color} {filename}")
        print(f"    Session ID: {s['session_id']}")

        if s["metadata"]:
            print(f"    Parts: {parts_count}/{s['metadata']['total_parts']}")
        else:
            print(f"    Parts on disk: {parts_count}")

        if s["error_msg"]:
            print(f"    Reason: \033[91m{s['error_msg']}\033[0m")
        print("-" * 80)

    if not selectable_indices:
        print("\n[!] No valid or complete sessions found in this directory. Merging cannot proceed.")
        return None

    while True:
        try:
            choice = input(f"Select a session index to merge ({', '.join(map(str, selectable_indices))}): ").strip()
            if not choice:
                continue
            choice_idx = int(choice)
            if choice_idx in selectable_indices:
                return session_list[choice_idx - 1]["session_id"]
            else:
                print("[!] Selection index is invalid, incomplete, or out of bounds. Please choose a valid index.")
        except ValueError:
            print("[!] Invalid input format. Please enter an index number.")


def main():
    parser = argparse.ArgumentParser(description="Reassembles split .kpart files downloaded via Telegram.")
    parser.add_id = parser.add_argument(
        "target_dir",
        help="The local directory containing the downloaded chunk parts."
    )
    parser.add_argument(
        "-o", "--output",
        help="Optional destination path/directory where the reassembled file will be placed. Defaults to target_dir."
    )
    parser.add_argument(
        "-s", "--session",
        help="Optional specific 12-char Session ID. Bypasses selection menu."
    )

    args = parser.parse_args()

    # Determine paths
    target_dir = os.path.abspath(args.target_dir)
    output_dir = os.path.abspath(args.output) if args.output else target_dir
    os.makedirs(output_dir, exist_ok=True)

    # Scan and verify sessions on disk
    assert isinstance(target_dir, str), "target_dir must be a string"

    verifier = SessionVerifier(target_dir)
    sessions = verifier.scan_sessions()

    if not sessions:
        print(f"[!] No valid .kpart files detected in directory: {target_dir}")
        return

    selected_session_id = None

    if args.session:
        # User specified a session directly
        if args.session in sessions:
            selected_session_id = args.session
            session_data = sessions[selected_session_id]
            if session_data["status"] != "VALID":
                print(f"[!] Error: The specified session '{args.session}' is marked as {session_data['status']}.")
                print(f"    Reason: {session_data['error_msg']}")
                return
        else:
            print(f"[!] Error: Session ID '{args.session}' was not found in the target directory.")
            return
    else:
        # No session ID provided, evaluate how many exist
        valid_sessions = {k: v for k, v in sessions.items() if v["status"] == "VALID"}

        if len(sessions) == 1 and len(valid_sessions) == 1:
            # Exactly one session found, and it is valid. Proceed directly.
            selected_session_id = list(valid_sessions.keys())[0]
        else:
            # Multi-session or errors present. Prompt user interactive choice.
            selected_session_id = run_interactive_selection(sessions)

    if not selected_session_id:
        return
    selected_session = sessions[selected_session_id]
    if selected_session["processor_type"] == "httpprocessor":
        perform_merge_http(selected_session, output_dir)
    elif selected_session["processor_type"] == "torrentprocessor" or selected_session["processor_type"] == "localfileprocessor":
        perform_merge_file_tree(selected_session, output_dir)
    else:
        raise ValueError(f"Unknown processor type: {selected_session['processor_type']}")


if __name__ == "__main__":
    main()