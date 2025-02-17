import os
import subprocess
import json
import concurrent.futures
from typing import Optional, Dict, List, Tuple
import math
import psutil
from mutagen.easyid3 import EasyID3
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, APIC, TIT2, TPE1, TALB


# Configuration
PLAYLIST_URL = (
    "https://www.youtube.com/playlist?list=PLn9b2rpdRYi7gNmL_I_fGdfm5HnQeLCE-"
)
DOWNLOAD_PATH = "/home/yann/Work/sync-jam-playlist/audio_downloads"
METADATA_FILE = os.path.join(DOWNLOAD_PATH, "playlist_metadata.json")
CONCURRENT_FRAGMENTS = "8"


os.makedirs(DOWNLOAD_PATH, exist_ok=True)


def get_optimal_config() -> Tuple[int, int, int]:
    """
    Determines optimal worker count and segment size based on system resources.
    Returns: (workers, segment_size, total_videos)
    """
    cpu_count = os.cpu_count() or 2
    available_memory_gb = psutil.virtual_memory().available / (1024**3)

    # Base workers calculation on CPU cores
    base_workers = cpu_count * 2

    # Adjust based on available memory (each worker might use ~200MB)
    max_workers_by_memory = int(available_memory_gb * 2)

    # Take the lower value to avoid overloading
    optimal_workers = min(base_workers, max_workers_by_memory, 8)
    workers = max(2, optimal_workers)  # Ensure at least 2 workers

    # Get total videos count
    cmd = ["yt-dlp", "--flat-playlist", "--print", "%(playlist_count)s", PLAYLIST_URL]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        total_videos = int(result.stdout.splitlines()[0])
        segment_size = min(200, max(100, total_videos // (workers * 2)))
    else:
        print("❌ Error: Could not fetch playlist size. Using defaults.")
        total_videos = 0
        segment_size = 150  # Default fallback

    return workers, segment_size, total_videos


# Get optimal configuration
MAX_WORKERS, SEGMENT_SIZE, TOTAL_VIDEOS = get_optimal_config()
print(
    f"🔧 Optimized configuration: {MAX_WORKERS} workers, {SEGMENT_SIZE} videos per segment"
)


def fetch_playlist_segment(start_index: int) -> Optional[Dict]:
    """Fetches a segment of the playlist metadata."""
    cmd = [
        "yt-dlp",
        "-J",
        "--flat-playlist",
        "--playlist-start",
        str(start_index),
        "--playlist-end",
        str(start_index + SEGMENT_SIZE - 1),
        PLAYLIST_URL,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return json.loads(result.stdout) if result.returncode == 0 else None


def fetch_playlist_info() -> Optional[Dict]:
    """Fetches the playlist metadata in parallel segments."""
    if TOTAL_VIDEOS == 0:
        print("❌ Error: Could not fetch playlist data.")
        return None

    num_segments = math.ceil(TOTAL_VIDEOS / SEGMENT_SIZE)
    print(
        f"📊 Playlist contains {TOTAL_VIDEOS} videos. Fetching in {num_segments} segments..."
    )

    segments: List[Dict] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_segment = {
            executor.submit(
                fetch_playlist_segment,
                i * SEGMENT_SIZE + 1,  # start_index (1-based)
            ): i
            for i in range(num_segments)
        }

        for future in concurrent.futures.as_completed(future_to_segment):
            segment_data = future.result()
            if segment_data:
                segments.append(segment_data)
            else:
                print(f"❌ Failed to fetch segment {future_to_segment[future]}")

    if not segments:
        return None

    # Combine all segments into one playlist
    combined_playlist = segments[0]
    combined_playlist["entries"] = []

    for segment in segments:
        combined_playlist["entries"].extend(segment.get("entries", []))

    return combined_playlist


def load_previous_metadata():
    """Loads previous playlist metadata."""
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"entries": []}


def save_metadata(metadata):
    """Saves playlist metadata for future comparison."""
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)


def get_existing_files():
    """Get a set of downloaded audio files (by ID)."""
    existing_files = set()
    for f in os.listdir(DOWNLOAD_PATH):
        if f.endswith(".mp3"):
            # Extract the ID from the filename (assuming format "Title - ID.mp3")
            video_id = f.split(" - ")[-1].replace(".mp3", "")
            existing_files.add(video_id)
    return existing_files


def download_audio(entry):
    """Downloads a single audio file using yt-dlp."""
    video_id = entry["id"]
    output_path = os.path.join(DOWNLOAD_PATH, "%(title)s - %(id)s.%(ext)s")

    if not isinstance(output_path, str):
        print("Error: output_path is not a string")
        return None

    if not isinstance(entry["url"], str):
        print(f"Error: Invalid URL for video {entry['title']}")
        return None

    cmd = [
        "yt-dlp",
        "-f",
        "bestaudio",
        "--extract-audio",
        "--audio-format",
        "mp3",
        "--embed-metadata",  # Add basic metadata
        "--embed-thumbnail",  # Add thumbnail as cover art
        "--parse-metadata",
        "%(uploader)s:%(meta_artist)s",  # Set video uploader as artist
        "--parse-metadata",
        "%(title)s:%(meta_title)s",  # Set video title as track title
        "--add-metadata",  # Ensure metadata is written to the file
        "--concurrent-fragments",
        CONCURRENT_FRAGMENTS,
        "--no-progress",
        "-o",
        output_path,
        entry["url"],
    ]

    print(f"Starting download: {entry['title']}")
    print(f"Running command: {cmd}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        filepath = os.path.join(DOWNLOAD_PATH, f"{entry['title']} - {video_id}.mp3")
        update_metadata(
            filepath,
            title=entry["title"],
            artist=entry.get("uploader", "Unknown"),
            album=entry.get("playlist_title", "YouTube Playlist"),
        )
        print(f"✔ Downloaded: {entry['title']}")
        return video_id
    else:
        print(f"❌ Failed: {entry['title']}\nError: {result.stderr}")
        return None


def parallel_download(playlist_data, existing_files):
    """Download missing audio files in parallel."""
    to_download = [
        entry for entry in playlist_data["entries"] if entry["id"] not in existing_files
    ]

    if not to_download:
        print("✅ All files are up to date.")
        return []

    print(f"🚀 Downloading {len(to_download)} new files with {MAX_WORKERS} threads...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(download_audio, to_download))

    return [r for r in results if r]


def update_metadata(filepath, title, artist, album=None):
    """Update MP3 metadata after download."""
    try:
        audio = MP3(filepath, ID3=ID3)

        # Create ID3 tag if it doesn't exist
        if not audio.tags:
            audio.add_tags()

        # Set the metadata
        audio.tags.add(TIT2(encoding=3, text=title))
        audio.tags.add(TPE1(encoding=3, text=artist))
        if album:
            audio.tags.add(TALB(encoding=3, text=album))

        audio.save()
        print(f"Updated metadata for: {filepath}")
    except Exception as e:
        print(f"Error updating metadata: {e}")


def sync_playlist():
    """Main function to sync playlist audio downloads."""
    print("🔄 Fetching playlist info...")
    playlist_data = fetch_playlist_info()
    if not playlist_data:
        print("❌ Error: Could not fetch playlist data.")
        return

    existing_files = get_existing_files()
    print(f"📂 Found {len(existing_files)} existing audio files.")

    # Download in parallel
    new_audios = parallel_download(playlist_data, existing_files)
    if new_audios:
        print(f"✔ Downloaded {len(new_audios)} new files.")
    print("✅ Sync complete.")

    # Save updated metadata
    save_metadata(playlist_data)


if __name__ == "__main__":
    sync_playlist()
