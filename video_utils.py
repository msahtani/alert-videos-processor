import subprocess
import os
import shutil


def ensure_browser_playable_mp4(video_path: str, quiet: bool = False) -> None:
    """
    Ensure MP4 video is browser-playable by adding faststart flag (no re-encoding).
    
    Args:
        video_path: Path to the video file to optimize
        quiet: If True, suppress output messages
    
    Raises:
        Exception: If ffmpeg is not found or conversion fails
    """
    if not os.path.exists(video_path):
        raise FileNotFoundError(f"Video file not found: {video_path}")
    
    # Check if ffmpeg is available
    if not shutil.which("ffmpeg"):
        if not quiet:
            print("⚠️  ffmpeg not found in PATH. Skipping browser optimization.")
            print("   Install ffmpeg: https://ffmpeg.org/download.html")
        return
    
    # Create temporary output file
    temp_output = video_path + ".temp.mp4"
    
    try:
        # Build ffmpeg command (stream copy - no re-encoding)
        # -i: input file
        # -c copy: copy streams without re-encoding
        # -movflags +faststart: enable streaming (move moov atom to beginning)
        # -y: overwrite output file
        cmd = [
            "ffmpeg",
            "-i", video_path,
            "-c", "copy",
            "-movflags", "+faststart",
            "-y",
            temp_output
        ]
        
        if quiet:
            # Suppress ffmpeg output
            result = subprocess.run(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True
            )
        else:
            result = subprocess.run(cmd, check=True)
        
        # Replace original file with optimized version
        if os.path.exists(temp_output):
            os.replace(temp_output, video_path)
            if not quiet:
                print(f"✅ Video optimized for browser playback: {os.path.basename(video_path)}")
    
    except subprocess.CalledProcessError as e:
        # Clean up temp file if it exists
        if os.path.exists(temp_output):
            os.remove(temp_output)
        raise Exception(f"ffmpeg conversion failed: {e}")
    
    except Exception as e:
        # Clean up temp file if it exists
        if os.path.exists(temp_output):
            os.remove(temp_output)
        raise


