import argparse
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

                # Check required fields
                if "filename" not in metadata or "total_parts" not in metadata:
                    raise KeyError("Required metadata key is missing.")

                data["metadata"] = metadata
                data["original_filename"] = metadata["filename"]
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


def perform_merge(session_data: dict, output_dir: str):
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

    if selected_session_id:
        perform_merge(sessions[selected_session_id], output_dir)


if __name__ == "__main__":
    main()