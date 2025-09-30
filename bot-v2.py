import logging
import threading
import time
import datetime
import traceback
import fractions
import json
import os
import re
from urllib.parse import urlparse
from pathlib import Path # Still useful for parsing, just not disk ops
import io # For handling bytes as files

from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Form
import av
from PIL import Image, ImageEnhance, UnidentifiedImageError # Pillow is still needed for image manipulation
from apscheduler.schedulers.background import BackgroundScheduler
# SQLAlchemyJobStore is removed as we can't write to disk. APScheduler will use MemoryJobStore.

# --- Configuration & Global Constants ---
APP_VERSION = "2.0.1-inmemory" # Version reflects in-memory change

# --- FastAPI & Scheduler Initialization ---
app = FastAPI(title="Advanced Stream Bot (In-Memory Edition)", version=APP_VERSION)

# APScheduler for scheduled streams - will use default MemoryJobStore
scheduler = BackgroundScheduler(timezone="UTC")

# --- Global Data Stores (Managed per user where applicable) ---
# user_sessions: chat_id -> session_data_dict
# All data is now in-memory.
user_sessions = {}
# session_locks: chat_id -> threading.Lock() for thread-safe session updates
session_locks = {}

# Global list for general bot logs (rolling)
live_log_lines_global = []
MAX_GLOBAL_LOG_LINES = 100

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(module)s:%(lineno)d %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stream_bot_inmemory")

def append_global_live_log(line: str):
    global live_log_lines_global
    live_log_lines_global.append(line)
    if len(live_log_lines_global) > MAX_GLOBAL_LOG_LINES:
        live_log_lines_global.pop(0)

class GlobalListHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        append_global_live_log(log_entry)

global_list_handler = GlobalListHandler()
global_list_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.addHandler(global_list_handler)


# --- Per-User Session Management (In-Memory) ---

DEFAULT_USER_SETTINGS = {
    "input_url_playlist": [],
    "current_playlist_index": 0,
    "output_url": "rtmp://a.rtmp.youtube.com/live2/", 
    "quality_settings": "medium",
    "video_codec": "libx264",
    "audio_codec": "aac",
    "resolution": "source",
    "fps": 30,
    "gop_size": 60,
    "video_bitrate": "1500k",
    "audio_bitrate": "128k",
    "loop_count": 0,
    "stop_on_error_in_playlist": True,
    "ffmpeg_preset": "medium",
    "logo_enabled": False,
    "logo_data_bytes": None, # Store logo as bytes
    "logo_mime_type": None, # e.g., 'image/png'
    "logo_original_filename": None, # For display
    "logo_position": "top_right",
    "logo_scale": 0.1,
    "logo_opacity": 0.8,
    "advanced_mode": False,
    "current_step": None,
    "conversation_fields_list": [],
}

DEFAULT_SESSION_RUNTIME_STATE = {
    "streaming_state": "idle",
    "stream_start_time": None,
    "frames_encoded": 0,
    "bytes_sent": 0,
    "stream_thread_ref": None,
    "pyav_objects": {"input_container": None, "output_container": None, "video_out_stream": None, "audio_out_stream": None, "logo_image_pil": None},
    "live_log_lines_user": [],
    "error_notification_user": "",
    "stop_gracefully_flag": False,
    "current_loop_iteration": 0,
}

def get_user_session(chat_id: int) -> dict:
    if chat_id not in user_sessions:
        session_locks[chat_id] = threading.Lock()
        with session_locks[chat_id]:
            # Initialize new session with defaults as there's no file to load from
            user_sessions[chat_id] = {}
            # Deep copy default settings to avoid shared mutable objects
            for key, value in DEFAULT_USER_SETTINGS.items():
                user_sessions[chat_id][key] = list(value) if isinstance(value, list) else value
            
            for key, value in DEFAULT_SESSION_RUNTIME_STATE.items():
                user_sessions[chat_id][key] = list(value) if isinstance(value, list) else value
            
            logger.info(f"[Chat {chat_id}] New IN-MEMORY session created. Settings are NOT persistent.")
    return user_sessions[chat_id]

# save_user_settings_to_file, load_user_settings_from_file, load_all_sessions_on_startup are REMOVED.
# Settings are only saved in memory for the current runtime.

# --- User-specific Live Logging ---
def append_user_live_log(chat_id: int, line: str):
    session = get_user_session(chat_id) # Ensures session exists
    # No need for lock here if list append is atomic and reading is infrequent / tolerant
    # but for consistency with other session modifications:
    lock = session_locks.get(chat_id)
    if lock: # Should always exist after get_user_session
        with lock:
            session['live_log_lines_user'].append(f"{datetime.datetime.now().strftime('%H:%M:%S')} {line}")
            if len(session['live_log_lines_user']) > 50:
                session['live_log_lines_user'].pop(0)
    else: # Fallback if lock somehow not initialized
        logger.warning(f"[Chat {chat_id}] Lock not found for appending user log. This is unexpected.")
        session['live_log_lines_user'].append(f"{datetime.datetime.now().strftime('%H:%M:%S')} {line}")
        if len(session['live_log_lines_user']) > 50:
            session['live_log_lines_user'].pop(0)


# --- Input Validation ---
SUPPORTED_VIDEO_CODECS = ["libx264", "h264_nvenc", "h264_qsv", "libx265", "hevc_nvenc", "hevc_qsv", "copy"]
SUPPORTED_AUDIO_CODECS = ["aac", "opus", "libmp3lame", "copy"]
LOGO_POSITIONS = ["top_left", "top_right", "bottom_left", "bottom_right", "center"]
QUALITY_SETTINGS_MAP = {
    "low": {"video_bitrate": "800k", "audio_bitrate": "96k", "ffmpeg_preset": "superfast", "resolution": "854x480"},
    "medium": {"video_bitrate": "1500k", "audio_bitrate": "128k", "ffmpeg_preset": "medium", "resolution": "1280x720"},
    "high": {"video_bitrate": "3000k", "audio_bitrate": "192k", "ffmpeg_preset": "fast", "resolution": "1920x1080"},
    "source": {}
}

def validate_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc]) and result.scheme in ['http', 'https', 'rtmp', 'rtsp', 'udp', 'srt', 'file']
    except:
        return False

def validate_resolution(res: str) -> bool:
    if res.lower() == "source":
        return True
    return bool(re.match(r"^\d{3,4}x\d{3,4}$", res))

def parse_bitrate(bitrate_str: str) -> int:
    bitrate_str = str(bitrate_str).lower() # Ensure it's a string
    if bitrate_str.endswith('k'):
        return int(float(bitrate_str[:-1]) * 1000)
    elif bitrate_str.endswith('m'):
        return int(float(bitrate_str[:-1]) * 1000000)
    try:
        return int(bitrate_str)
    except ValueError:
        return 1500000 

def format_settings_for_display(session: dict) -> str:
    logo_status = "Disabled"
    if session.get('logo_enabled') and session.get('logo_data_bytes'):
        logo_status = f"Enabled ({session.get('logo_original_filename', 'Unknown name')})"

    display_data = {
        "Output URL": session.get('output_url'),
        "Quality": session.get('quality_settings'),
        "Video Codec": session.get('video_codec'),
        "Audio Codec": session.get('audio_codec'),
        "Resolution": session.get('resolution'),
        "FPS": session.get('fps'),
        "Video Bitrate": session.get('video_bitrate'),
        "Audio Bitrate": session.get('audio_bitrate'),
        "FFmpeg Preset": session.get('ffmpeg_preset'),
        "Loop Count": "Infinite" if session.get('loop_count') == -1 else session.get('loop_count'),
        "Playlist Length": len(session.get('input_url_playlist', [])),
        "Logo": logo_status,
    }
    if session.get('logo_enabled') and session.get('logo_data_bytes'):
        display_data["Logo Position"] = session.get('logo_position')
        display_data["Logo Scale"] = session.get('logo_scale')
        display_data["Logo Opacity"] = session.get('logo_opacity')
    
    return "\n".join([f"• *{k}*: `{v}`" for k, v in display_data.items() if v is not None])


# --- UI Helpers (send_message, edit_message_text, answer_callback_query, get_main_keyboard, get_uptime, compose_status_message) ---
# These functions remain largely the same as they don't interact with disk.
# Minor change in compose_status_message for logo display from bytes.
def send_message(chat_id: int, text: str, parse_mode="Markdown", reply_markup=None):
    logger.info(f"[Chat {chat_id}] Sending message: {text[:100]}...")
    response = {
        "method": "sendMessage",
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode
    }
    if reply_markup:
        response["reply_markup"] = reply_markup
    return response

def edit_message_text(chat_id: int, message_id: int, text: str, parse_mode="Markdown", reply_markup=None):
    logger.info(f"[Chat {chat_id}] Editing message {message_id}: {text[:100]}...")
    response = {
        "method": "editMessageText",
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": parse_mode
    }
    if reply_markup:
        response["reply_markup"] = reply_markup
    return response

def answer_callback_query(callback_query_id: str, text: str = None, show_alert: bool = False):
    response = {
        "method": "answerCallbackQuery",
        "callback_query_id": callback_query_id,
    }
    if text:
        response["text"] = text
    if show_alert:
        response["show_alert"] = True
    return response

def get_main_keyboard(session: dict):
    streaming = session.get('streaming_state') in ["streaming", "paused", "starting"]
    buttons = []
    if not streaming:
        buttons.append([{"text": "🚀 Start/Configure Stream", "callback_data": "cfg_start"}])
        buttons.append([
            {"text": "⚙️ Settings", "callback_data": "cfg_advanced"},
            {"text": "📜 Playlist (" + str(len(session.get('input_url_playlist',[]))) + ")", "callback_data": "cfg_playlist"}
        ])
        buttons.append([
            {"text": "🖼️ Logo", "callback_data": "cfg_logo"},
            {"text": "🕒 Schedule", "callback_data": "cfg_schedule"}
        ])
    else: 
        if session.get('streaming_state') == "streaming":
            buttons.append([{"text": "⏸ Pause", "callback_data": "stream_pause"}])
        elif session.get('streaming_state') == "paused":
            buttons.append([{"text": "▶️ Resume", "callback_data": "stream_resume"}])
        
        buttons.append([
            {"text": "⏹ Abort Stream", "callback_data": "stream_abort"},
            {"text": "📊 Status Update", "callback_data": "stream_status"}
        ])
        if session.get('loop_count', 0) == -1:
             buttons.append([{"text": "⏳ Stop Loop Gracefully", "callback_data": "stream_stop_loop_graceful"}])
    
    buttons.append([{"text": "📜 Global Logs", "callback_data": "show_global_logs"}, {"text": "❔ Help", "callback_data": "show_help"}])
    return {"inline_keyboard": buttons}


def get_uptime(start_time_obj):
    if start_time_obj:
        # Ensure start_time_obj is offset-aware if comparing with offset-aware now()
        if start_time_obj.tzinfo is None:
             start_time_obj = start_time_obj.replace(tzinfo=datetime.timezone.utc) # Assume UTC if naive
        uptime_delta = datetime.datetime.now(datetime.timezone.utc) - start_time_obj
        return str(uptime_delta).split('.')[0]
    return "0s"

def compose_status_message(chat_id: int, include_config: bool = False) -> str:
    session = get_user_session(chat_id)
    state = session['streaming_state']
    
    status_lines = [
        f"🤖 *Stream Bot Status for Chat ID {chat_id} (In-Memory Mode)*",
        f"_Settings, logos, and schedules are NOT persistent across bot restarts._",
        f"State: `{state}`",
    ]
    if session.get('error_notification_user'):
        status_lines.append(f"⚠️ Error: `{session['error_notification_user']}`")

    if state in ["streaming", "paused", "starting", "stopping"]:
        current_input_url_display = "N/A"
        playlist = session.get('input_url_playlist', [])
        playlist_idx = session.get('current_playlist_index', 0)
        if playlist and 0 <= playlist_idx < len(playlist):
            current_input_url_display = playlist[playlist_idx]

        status_lines.extend([
            f"Uptime: `{get_uptime(session['stream_start_time'])}`",
            f"Frames Encoded: `{session['frames_encoded']}`",
            f"Bytes Sent: `{session['bytes_sent'] / (1024*1024):.2f} MB`",
            f"Current Input: `{current_input_url_display}` (Item {playlist_idx+1}/{len(playlist)})",
            f"Loop Iteration: {session.get('current_loop_iteration',0)+1}/{'Infinite' if session.get('loop_count',0) == -1 else session.get('loop_count',0) or 1}"
        ])
    
    if include_config or state not in ["streaming", "paused"]:
        status_lines.append("\n📝 *Current Configuration:*")
        status_lines.append(format_settings_for_display(session))

    if state in ["streaming", "paused", "starting"]:
        status_lines.append("\n📋 *User Live Logs (last 10):*")
        user_logs_preview = "\n".join(session['live_log_lines_user'][-10:])
        status_lines.append(f"<pre>{user_logs_preview if user_logs_preview else 'No user logs yet.'}</pre>")
        
    return "\n".join(status_lines)


# --- Core Streaming Logic ---
# stream_engine_thread_target needs to load logo from session['logo_data_bytes']
def stream_engine_thread_target(chat_id: int):
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]
    
    active_input_container = None
    active_output_container = None
    active_video_out_stream = None
    active_audio_out_stream = None
    # logo_image_pil is now loaded from bytes each time, or stored in pyav_objects for the thread's duration
    # logo_width, logo_height = 0, 0 # These will be calculated
    # logo_pos_x, logo_pos_y = 0, 0   # These will be calculated


    def _cleanup_pyav_resources():
        # This function is called within the stream thread
        # Access pyav_objects directly from session inside the lock
        append_user_live_log(chat_id, "Cleaning up PyAV resources...")
        with lock: # Ensure thread-safe access to session's pyav_objects
            pyav_objs = session.get('pyav_objects', {})
            
            if pyav_objs.get('output_container'):
                try: pyav_objs['output_container'].close()
                except Exception as e_c: append_user_live_log(chat_id, f"Error closing output_container: {e_c}")
            pyav_objs['output_container'] = None
            pyav_objs['video_out_stream'] = None
            pyav_objs['audio_out_stream'] = None
            
            if pyav_objs.get('input_container'):
                try: pyav_objs['input_container'].close()
                except Exception as e_c: append_user_live_log(chat_id, f"Error closing input_container: {e_c}")
            pyav_objs['input_container'] = None
            
            pyav_objs['logo_image_pil'] = None # Clear processed PIL image

            # session['pyav_objects'] = pyav_objs # Already modified in place
        append_user_live_log(chat_id, "PyAV resources cleanup attempt complete.")

    try:
        with lock:
            session['streaming_state'] = "starting"
            session['error_notification_user'] = ""
            session['stream_start_time'] = datetime.datetime.now(datetime.timezone.utc)
            session['frames_encoded'] = 0
            session['bytes_sent'] = 0
            session['current_loop_iteration'] = 0
            session['current_playlist_index'] = 0
            session['stop_gracefully_flag'] = False
            # Reset pyav_objects for this run, ensures clean state
            session['pyav_objects'] = {
                "input_container": None, "output_container": None, 
                "video_out_stream": None, "audio_out_stream": None, 
                "logo_image_pil": None 
            }


        append_user_live_log(chat_id, f"Stream engine started. Output: {session['output_url']}")

        try:
            active_output_container = av.open(session['output_url'], mode='w', format='flv', timeout=10)
            with lock: session['pyav_objects']['output_container'] = active_output_container
            append_user_live_log(chat_id, f"Output RTMP connection established: {session['output_url']}")
        except Exception as e:
            raise Exception(f"Failed to open output RTMP stream: {e}")

        # --- Logo Setup (if enabled and data exists) ---
        # This is done once per stream start, assuming logo doesn't change mid-stream
        # The processed PIL image is stored in session['pyav_objects']['logo_image_pil']
        # For this thread's use only.
        if session.get('logo_enabled') and session.get('logo_data_bytes'):
            logo_bytes = session.get('logo_data_bytes')
            try:
                logo_image_pil_original = Image.open(io.BytesIO(logo_bytes)).convert("RGBA")
                
                if session.get('logo_opacity', 1.0) < 1.0:
                    alpha = logo_image_pil_original.split()[-1]
                    alpha = ImageEnhance.Brightness(alpha).enhance(session['logo_opacity'])
                    logo_image_pil_original.putalpha(alpha)
                
                with lock: # Store the processed PIL image for this stream thread
                    session['pyav_objects']['logo_image_pil'] = logo_image_pil_original 
                append_user_live_log(chat_id, f"Logo loaded from in-memory bytes: {session.get('logo_original_filename', 'N/A')}")
            except UnidentifiedImageError:
                append_user_live_log(chat_id, f"Error: Could not load logo image from bytes. Skipping logo.")
            except Exception as e:
                append_user_live_log(chat_id, f"Error processing logo from bytes: {e}. Skipping logo.")
        else:
            append_user_live_log(chat_id, "Logo not enabled or no logo data. Skipping logo.")


        total_loops = session.get('loop_count', 0)
        playlist = session.get('input_url_playlist', [])

        if not playlist:
            raise Exception("Playlist is empty. Nothing to stream.")

        # --- Main Loop (Loops & Playlist) ---
        while True: 
            with lock: 
                if session['stop_gracefully_flag'] or session['streaming_state'] == "stopping":
                    append_user_live_log(chat_id, "Graceful stop or abort signal received. Ending outer loop.")
                    break
                current_loop_iter = session['current_loop_iteration']

            if total_loops != -1 and current_loop_iter >= total_loops and total_loops != 0:
                 append_user_live_log(chat_id, f"Completed {total_loops} loop(s).")
                 break
            
            append_user_live_log(chat_id, f"Starting loop iteration {current_loop_iter + 1}")
            with lock: session['current_playlist_index'] = 0
            
            while session['current_playlist_index'] < len(playlist):
                with lock: 
                    if session['stop_gracefully_flag'] or session['streaming_state'] == "stopping":
                        append_user_live_log(chat_id, "Graceful stop or abort signal received. Ending playlist loop.")
                        break 
                    current_idx = session['current_playlist_index']
                
                current_input_url = playlist[current_idx]
                append_user_live_log(chat_id, f"Processing playlist item {current_idx + 1}/{len(playlist)}: {current_input_url}")

                # Local vars for this specific input processing cycle.
                # These do not directly use session['pyav_objects'] for their primary reference
                # but update it upon successful creation/assignment.
                _current_input_container = None
                # Output streams are more persistent across playlist items IF compatible.
                # For simplicity, we define them once based on the first item or re-check.

                try:
                    _current_input_container = av.open(current_input_url, timeout=10)
                    # Store in session for potential cleanup if error occurs before local var is cleared
                    with lock: session['pyav_objects']['input_container'] = _current_input_container 
                    append_user_live_log(chat_id, f"Input opened: {current_input_url}")

                    in_v_streams = _current_input_container.streams.video
                    in_a_streams = _current_input_container.streams.audio
                    if not in_v_streams: raise Exception("No video stream found in input.")
                    
                    in_v_s = in_v_streams[0]
                    target_width, target_height = in_v_s.width, in_v_s.height
                    if session['resolution'] != "source":
                        try:
                            target_width, target_height = map(int, session['resolution'].split('x'))
                        except ValueError:
                            append_user_live_log(chat_id, f"Invalid target resolution '{session['resolution']}'. Using source.")
                            target_width, target_height = in_v_s.width, in_v_s.height
                    
                    # Retrieve output streams from session if they exist, or create them
                    with lock:
                        active_video_out_stream = session['pyav_objects'].get('video_out_stream')
                        active_audio_out_stream = session['pyav_objects'].get('audio_out_stream')
                        logo_pil_original_ref = session['pyav_objects'].get('logo_image_pil') # This is the pre-opacity PIL image

                    # --- (Re)configure video output stream ---
                    # Only configure if not already done or if significantly changed (here, simplified to "first time")
                    is_first_input_or_reconfig_needed = active_video_out_stream is None
                    
                    # Local var for resized logo for current video dimensions
                    current_logo_pil_resized = None 
                    logo_pos_x, logo_pos_y = 0,0 # Reset for each potential reconfig

                    if is_first_input_or_reconfig_needed:
                        active_video_out_stream = active_output_container.add_stream(session['video_codec'], rate=session['fps'])
                        active_video_out_stream.width = target_width
                        active_video_out_stream.height = target_height
                        active_video_out_stream.pix_fmt = 'yuv420p'
                        active_video_out_stream.codec_context.gop_size = session.get('gop_size', int(session['fps'] * 2))
                        active_video_out_stream.codec_context.bit_rate = parse_bitrate(session['video_bitrate'])
                        active_video_out_stream.codec_context.thread_type = "AUTO"
                        active_video_out_stream.codec_context.options = {'preset': session.get('ffmpeg_preset', 'medium')}
                        if session['video_codec'] == 'copy':
                            active_video_out_stream.codec_context.copy_parameters(in_v_s.codec_context)
                        
                        with lock: session['pyav_objects']['video_out_stream'] = active_video_out_stream
                        append_user_live_log(chat_id, f"Video output stream configured: {target_width}x{target_height}@{session['fps']}fps, {session['video_bitrate']}")

                        # Logo scaling and positioning based on this video stream's output height
                        if logo_pil_original_ref: # The pre-opacity, original ratio PIL object
                            logo_aspect_ratio = logo_pil_original_ref.width / logo_pil_original_ref.height
                            logo_h = int(active_video_out_stream.height * session.get('logo_scale', 0.1))
                            logo_w = int(logo_h * logo_aspect_ratio)
                            logo_w = min(logo_w, active_video_out_stream.width) # cap width
                            logo_h = min(logo_h, active_video_out_stream.height) # cap height

                            if logo_w > 0 and logo_h > 0:
                                current_logo_pil_resized = logo_pil_original_ref.resize((logo_w, logo_h), Image.Resampling.LANCZOS)
                                append_user_live_log(chat_id, f"Logo resized to {logo_w}x{logo_h} for current video item.")
                                
                                # Calculate position
                                margin = 10
                                if session['logo_position'] == "top_right":
                                    logo_pos_x = active_video_out_stream.width - logo_w - margin
                                    logo_pos_y = margin
                                elif session['logo_position'] == "top_left":
                                    logo_pos_x = margin; logo_pos_y = margin
                                elif session['logo_position'] == "bottom_left":
                                    logo_pos_x = margin; logo_pos_y = active_video_out_stream.height - logo_h - margin
                                elif session['logo_position'] == "bottom_right":
                                    logo_pos_x = active_video_out_stream.width - logo_w - margin
                                    logo_pos_y = active_video_out_stream.height - logo_h - margin
                                elif session['logo_position'] == "center":
                                    logo_pos_x = (active_video_out_stream.width - logo_w) // 2
                                    logo_pos_y = (active_video_out_stream.height - logo_h) // 2
                                else: # Default top_right
                                    logo_pos_x = active_video_out_stream.width - logo_w - margin; logo_pos_y = margin
                                append_user_live_log(chat_id, f"Logo position set to ({logo_pos_x}, {logo_pos_y}) for current video item.")
                            else:
                                append_user_live_log(chat_id, "Logo resulted in 0 dimension after scaling for current video. Logo disabled for this item.")
                                current_logo_pil_resized = None # Disable for this item


                    # --- (Re)configure audio output stream ---
                    if is_first_input_or_reconfig_needed and in_a_streams:
                        in_a_s = in_a_streams[0]
                        active_audio_out_stream = active_output_container.add_stream(session['audio_codec'], rate=in_a_s.rate)
                        active_audio_out_stream.codec_context.bit_rate = parse_bitrate(session['audio_bitrate'])
                        if session['audio_codec'] == 'copy':
                             active_audio_out_stream.codec_context.copy_parameters(in_a_s.codec_context)
                        with lock: session['pyav_objects']['audio_out_stream'] = active_audio_out_stream
                        append_user_live_log(chat_id, f"Audio output stream configured: {in_a_s.rate}Hz, {session['audio_bitrate']}")
                    elif is_first_input_or_reconfig_needed and not in_a_streams:
                        append_user_live_log(chat_id, "No audio stream in input. Streaming video only.")
                        active_audio_out_stream = None
                        with lock: session['pyav_objects']['audio_out_stream'] = None


                    with lock:
                        if session['streaming_state'] != "paused":
                             session['streaming_state'] = "streaming"
                    
                    # --- Packet Processing Loop (for current input URL) ---
                    for packet in _current_input_container.demux(video=0, audio=0 if in_a_streams else -1):
                        with lock: 
                            if session['streaming_state'] == "stopping": break
                            while session['streaming_state'] == "paused":
                                lock.release(); time.sleep(0.2); lock.acquire()
                                if session['streaming_state'] == "stopping": break
                            if session['streaming_state'] == "stopping": break
                        
                        if packet.dts is None: continue

                        if packet.stream.type == 'video' and packet.stream.index == in_v_s.index:
                            for frame in packet.decode():
                                if not frame.width or not frame.height: continue

                                if frame.width != active_video_out_stream.width or frame.height != active_video_out_stream.height:
                                    frame = frame.reformat(active_video_out_stream.width, active_video_out_stream.height, format='yuv420p')
                                
                                if current_logo_pil_resized and active_video_out_stream: # Use the resized logo for *this* video item
                                    try:
                                        pil_frame = frame.to_image()
                                        # Ensure logo_w, logo_h from current_logo_pil_resized are used for bounds
                                        actual_x = min(logo_pos_x, pil_frame.width - current_logo_pil_resized.width)
                                        actual_y = min(logo_pos_y, pil_frame.height - current_logo_pil_resized.height)
                                        actual_x = max(0, actual_x)
                                        actual_y = max(0, actual_y)

                                        pil_frame.paste(current_logo_pil_resized, (actual_x, actual_y), current_logo_pil_resized)
                                        # frame = av.VideoFrame.from_image(pil_frame) # This creates a new frame
                                        # Preserve original frame's PTS if possible, or let encoder handle it
                                        new_pts = frame.pts # Keep original PTS
                                        frame = av.VideoFrame.from_image(pil_frame)
                                        frame.pts = new_pts

                                    except Exception as e_logo:
                                        append_user_live_log(chat_id, f"Error applying logo: {e_logo}.")
                                
                                for out_packet in active_video_out_stream.encode(frame):
                                    active_output_container.mux(out_packet)
                                    with lock:
                                        session['frames_encoded'] += 1
                                        session['bytes_sent'] += out_packet.size
                        
                        elif active_audio_out_stream and packet.stream.type == 'audio' and packet.stream.index == in_a_streams[0].index :
                            for frame in packet.decode():
                                if frame.pts is None: continue
                                for out_packet in active_audio_out_stream.encode(frame):
                                    active_output_container.mux(out_packet)
                                    with lock:
                                        session['bytes_sent'] += out_packet.size
                    else: 
                        append_user_live_log(chat_id, f"Finished processing input: {current_input_url}")

                except Exception as e_input:
                    tb_str = traceback.format_exc()
                    append_user_live_log(chat_id, f"Error processing input {current_input_url}: {e_input}\n{tb_str}")
                    with lock: session['error_notification_user'] = f"Error with {current_input_url}: {e_input}"
                    if session.get('stop_on_error_in_playlist', True):
                        append_user_live_log(chat_id, "Stopping stream due to error in playlist and stop_on_error_in_playlist=True.")
                        with lock: session['streaming_state'] = "stopping"
                        break 
                    else:
                        append_user_live_log(chat_id, "Skipping to next item in playlist (if any).")
                finally:
                    if _current_input_container: # Use the local var for this input item
                        try: _current_input_container.close()
                        except Exception as e_c: append_user_live_log(chat_id, f"Minor error closing input container: {e_c}")
                    # Clear from session too, as this specific input_container is done
                    with lock:
                        if session['pyav_objects']['input_container'] == _current_input_container:
                             session['pyav_objects']['input_container'] = None
                
                with lock:
                    if session['streaming_state'] == "stopping": break
                    session['current_playlist_index'] += 1
            
            with lock:
                if session['streaming_state'] == "stopping": break
                session['current_loop_iteration'] += 1
        
        append_user_live_log(chat_id, "All playlist items and loops processed or stream stopped.")

    except Exception as e_stream:
        tb_str = traceback.format_exc()
        append_user_live_log(chat_id, f"Fatal stream engine error: {e_stream}\n{tb_str}")
        with lock:
            session['error_notification_user'] = f"Stream failed: {e_stream}"
            session['streaming_state'] = "error"
    finally:
        append_user_live_log(chat_id, "Stream engine finalizing...")
        
        # Retrieve output streams from session for flushing
        # This assumes active_output_container is still the one from the start of the stream.
        _final_output_container = None
        _final_video_out_stream = None
        _final_audio_out_stream = None
        with lock:
            _final_output_container = session['pyav_objects'].get('output_container')
            _final_video_out_stream = session['pyav_objects'].get('video_out_stream')
            _final_audio_out_stream = session['pyav_objects'].get('audio_out_stream')

        if _final_output_container: # Check if output container was successfully opened
            append_user_live_log(chat_id, "Flushing encoders...")
            if _final_video_out_stream:
                for out_packet in _final_video_out_stream.encode():
                    try: _final_output_container.mux(out_packet)
                    except Exception as fe: append_user_live_log(chat_id, f"Err flushing V: {fe}")
            if _final_audio_out_stream:
                for out_packet in _final_audio_out_stream.encode():
                    try: _final_output_container.mux(out_packet)
                    except Exception as fe: append_user_live_log(chat_id, f"Err flushing A: {fe}")
            append_user_live_log(chat_id, "Encoders flushed.")

        _cleanup_pyav_resources()

        with lock:
            final_state = "completed" if not session['error_notification_user'] and session['streaming_state'] != "error" else "error"
            if session['streaming_state'] == "stopping": 
                final_state = "stopped"
            session['streaming_state'] = final_state
            session['stream_thread_ref'] = None
        
        append_user_live_log(chat_id, f"Stream engine finished. Final state: {final_state}.")
        logger.info(f"[Chat {chat_id}] Stream ended with state: {final_state}. Error: {session['error_notification_user']}")


# --- Telegram Command and Callback Handlers ---
# handle_telegram_update, CONVERSATION_STEPS*, start_settings_conversation, handle_conversation_step
# handle_playlist_command, handle_playlist_config_interaction remain similar,
# but they won't call save_user_settings_to_file. Changes are implicitly in-memory.

# handle_logo_upload needs to read file bytes and store them.
# handle_logo_config_interaction needs to reflect in-memory storage.
# schedule_stream_job, handle_schedule_command, handle_schedule_config_interaction are fine as APScheduler will use MemoryJobStore.
# Stream Control Command Handlers (start, pause, resume, abort) are mostly fine, no disk writes.
# get_help_text needs to be updated about non-persistence.

async def handle_telegram_update(update: dict):
    if "message" in update:
        message = update["message"]
        chat_id = message["chat"]["id"]
        user_name = message["from"].get("username", f"User_{chat_id}")
        session = get_user_session(chat_id)
        lock = session_locks[chat_id]

        text = message.get("text", "")
        command = text.split(' ')[0].lower() if text.startswith('/') else None
        
        if message.get("photo") or message.get("document"):
            if session.get("current_step") == "awaiting_logo_upload":
                # Need to pass the actual UploadFile if using FastAPI's file handling for webhook
                # For direct JSON, we'd get file_id and need a bot instance to download.
                # Assuming direct JSON from Telegram with file_id:
                return await handle_logo_data_from_telegram(chat_id, message)
            else:
                return send_message(chat_id, "Received a file, but I wasn't expecting one. Use /set_logo first if you want to upload a logo.")

        if command:
            logger.info(f"[Chat {chat_id} ({user_name})] Received command: {text}")
            if command == "/start" or command == "/menu":
                with lock: session['current_step'] = None 
                return send_message(chat_id, compose_status_message(chat_id, include_config=True), reply_markup=get_main_keyboard(session), parse_mode="HTML")
            elif command == "/help":
                return send_message(chat_id, get_help_text(), parse_mode="Markdown")
            elif command == "/settings":
                return start_settings_conversation(chat_id, advanced=True)
            elif command == "/stream":
                 return await start_stream_command_handler(chat_id)
            elif command == "/playlist":
                return await handle_playlist_command(chat_id, text)
            elif command == "/set_logo":
                 with lock: session["current_step"] = "awaiting_logo_upload"
                 return send_message(chat_id, "Please upload your logo image (PNG or JPG). Max 1MB recommended due to in-memory storage.")
            elif command == "/schedule":
                return await handle_schedule_command(chat_id, text)
            elif command == "/status":
                 return send_message(chat_id, compose_status_message(chat_id, include_config=True), reply_markup=get_main_keyboard(session), parse_mode="HTML")
            elif command == "/abort":
                return await stream_abort_handler(chat_id) 
            else: 
                return await handle_conversation_step(chat_id, text)
        else: 
            return await handle_conversation_step(chat_id, text)

    elif "callback_query" in update:
        cb_query = update["callback_query"]
        chat_id = cb_query["message"]["chat"]["id"]
        message_id = cb_query["message"]["message_id"]
        data = cb_query["data"]
        user_name = cb_query["from"].get("username", f"User_{chat_id}")
        session = get_user_session(chat_id)
        lock = session_locks[chat_id]
        
        logger.info(f"[Chat {chat_id} ({user_name})] Received callback: {data}")
        
        ack = answer_callback_query(cb_query["id"])
        
        if data == "cfg_start":
            if not session.get('input_url_playlist') or not session.get('output_url'):
                # This is tricky as start_settings_conversation returns a message dict, not just text.
                settings_response = start_settings_conversation(chat_id, advanced=session.get('advanced_mode', False))
                response_text = "Playlist or Output URL is not set. Let's configure.\n" + settings_response['text']
                # If settings_response has its own reply_markup, it might conflict.
                # For simplicity, let's assume start_settings_conversation is primarily text.
                return [ack, send_message(chat_id, response_text, reply_markup=settings_response.get('reply_markup') or get_main_keyboard(session))]

            else:
                return [ack, await start_stream_command_handler(chat_id, message_id_to_edit=message_id)]
        elif data == "cfg_advanced":
            settings_response = start_settings_conversation(chat_id, advanced=True)
            return [ack, edit_message_text(chat_id, message_id, settings_response['text'] + "\n\n" + compose_status_message(chat_id, include_config=True), reply_markup=settings_response.get('reply_markup') or get_main_keyboard(session), parse_mode="HTML")]
        elif data == "cfg_playlist":
            return [ack, await handle_playlist_config_interaction(chat_id, message_id)]
        elif data == "cfg_logo":
            return [ack, await handle_logo_config_interaction(chat_id, message_id)]
        elif data == "cfg_schedule":
            return [ack, await handle_schedule_config_interaction(chat_id, message_id)]
        
        elif data == "stream_pause":
            return [ack, await stream_pause_handler(chat_id, message_id)]
        elif data == "stream_resume":
            return [ack, await stream_resume_handler(chat_id, message_id)]
        elif data == "stream_abort":
            return [ack, await stream_abort_handler(chat_id, message_id_to_edit=message_id)]
        elif data == "stream_status":
            return [ack, edit_message_text(chat_id, message_id, compose_status_message(chat_id, include_config=False), reply_markup=get_main_keyboard(session), parse_mode="HTML")]
        elif data == "stream_stop_loop_graceful":
            with lock: session['stop_gracefully_flag'] = True
            append_user_live_log(chat_id, "Graceful loop stop requested.")
            return [ack, edit_message_text(chat_id, message_id, "Graceful loop stop initiated. Current iteration will complete.\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session), parse_mode="HTML")]

        elif data == "show_global_logs":
            global_logs_text = "📜 *Global Bot Logs (last 20):*\n<pre>" + "\n".join(live_log_lines_global[-20:]) + "</pre>"
            return [ack, edit_message_text(chat_id, message_id, global_logs_text + "\n\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session), parse_mode="HTML")]
        elif data == "show_help":
            return [ack, edit_message_text(chat_id, message_id, get_help_text() + "\n\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session))]
        
        elif data.startswith("set_quality_"):
            quality = data.split("_")[-1]
            with lock:
                session["quality_settings"] = quality
                if quality in QUALITY_SETTINGS_MAP and quality != "custom":
                    for key, val in QUALITY_SETTINGS_MAP[quality].items():
                        session[key] = val
                session["current_step"] = None 
            # save_user_settings_to_file(chat_id) # REMOVED
            return [ack, edit_message_text(chat_id, message_id, f"Quality set to `{quality}`.\n" + compose_status_message(chat_id, True), reply_markup=get_main_keyboard(session), parse_mode="HTML")]
        
        return [ack, send_message(chat_id, "Unknown action.")]

    return {"status": "ok", "message": "Update type not handled"}


CONVERSATION_STEPS_SIMPLE = [
    ("input_url_playlist", "Enter the first Input URL for your playlist (e.g., http://example.com/stream.m3u8). You can add more later.", validate_url),
    ("output_url", "Enter your RTMP Output URL (e.g., rtmp://a.rtmp.youtube.com/live2/YOUR_STREAM_KEY).", validate_url),
]
CONVERSATION_STEPS_ADVANCED = [
    ("input_url_playlist", "Enter the first Input URL for your playlist. You can add more later.", validate_url),
    ("output_url", "Enter your RTMP Output URL.", validate_url),
    ("quality_settings_prompt", "Choose Quality Preset: (low, medium, high, custom). Custom allows setting individual params next.", None),
    ("video_codec", f"Video Codec (e.g., libx264). Supported: {', '.join(SUPPORTED_VIDEO_CODECS)}.", lambda x: x in SUPPORTED_VIDEO_CODECS),
    ("audio_codec", f"Audio Codec (e.g., aac). Supported: {', '.join(SUPPORTED_AUDIO_CODECS)}.", lambda x: x in SUPPORTED_AUDIO_CODECS),
    ("resolution", "Resolution (e.g., 1280x720 or 'source').", validate_resolution),
    ("fps", "FPS (e.g., 30).", lambda x: str(x).isdigit() and 1 <= int(x) <= 120),
    ("video_bitrate", "Video Bitrate (e.g., 1500k, 3M).", lambda x: True),
    ("audio_bitrate", "Audio Bitrate (e.g., 128k).", lambda x: True),
    ("ffmpeg_preset", "FFmpeg Preset (for libx264: ultrafast, medium, slow etc.).", lambda x: True),
    ("loop_count", "Loop Count (0 for no loop, -1 for infinite, N for N times).", lambda x: str(x).lstrip('-').isdigit()),
]

def start_settings_conversation(chat_id: int, advanced: bool):
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]
    with lock:
        session['advanced_mode'] = advanced
        session['conversation_fields_list'] = CONVERSATION_STEPS_ADVANCED if advanced else CONVERSATION_STEPS_SIMPLE
        session['current_step_index'] = 0
        session['current_step'] = session['conversation_fields_list'][0][0]
    
    first_field_key, first_prompt, _ = session['conversation_fields_list'][0]
    
    # Prepare initial message (send_message or part of edit_message_text)
    initial_message_content = {}
    
    if first_field_key == "quality_settings_prompt":
        initial_message_content = {
            "text": first_prompt,
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": q.title(), "callback_data": f"set_quality_{q}"} for q in ["low", "medium", "high", "custom"]]
                ]
            }
        }
    else:
        current_value_msg = f"(Current: `{session.get(first_field_key, 'Not set')}`)" if session.get(first_field_key) else ""
        initial_message_content = {"text": f"{first_prompt}\n{current_value_msg}\n\nType /cancel to abort setup."}

    # This function is now called from callbacks too, so it needs to return a dict that can be used by send_message or edit_message_text
    return initial_message_content


async def handle_conversation_step(chat_id: int, user_text: str):
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]

    current_step_key_local = None
    validator_local = None
    current_step_index_local = -1

    with lock:
        if not session.get('current_step') or not session.get('conversation_fields_list'):
            return send_message(chat_id, "No setup in progress. Use /start or a menu button.", reply_markup=get_main_keyboard(session))

        if user_text.lower() == "/cancel":
            session['current_step'] = None
            session['current_step_index'] = 0 # Reset index
            return send_message(chat_id, "Setup cancelled.", reply_markup=get_main_keyboard(session))

        # Ensure current_step_index is valid
        if not (0 <= session.get('current_step_index', -1) < len(session['conversation_fields_list'])):
            session['current_step'] = None # Invalid state, reset
            session['current_step_index'] = 0
            logger.warning(f"[Chat {chat_id}] Invalid current_step_index, resetting conversation.")
            return send_message(chat_id, "Conversation state error, setup reset. Please start again.", reply_markup=get_main_keyboard(session))

        current_step_index_local = session['current_step_index']
        current_step_key_local, _, validator_local = session['conversation_fields_list'][current_step_index_local]
        
    user_input = user_text.strip()

    if validator_local and not validator_local(user_input):
        # Re-fetch prompt as it might have been clobbered if lock wasn't held long enough (should be fine here)
        prompt, _, _ = session['conversation_fields_list'][current_step_index_local] # Use local index
        return send_message(chat_id, f"Invalid input for {current_step_key_local.replace('_',' ').title()}. Please try again.\n{prompt}")

    with lock:
        # Re-verify current_step_key just in case, though primary logic relies on index
        if current_step_key_local != session['conversation_fields_list'][session['current_step_index']][0]:
            logger.error(f"[Chat {chat_id}] Conversation step mismatch! Aborting step.")
            return send_message(chat_id, "A state error occurred. Please try /cancel and restart setup.", reply_markup=get_main_keyboard(session))

        if current_step_key_local == "input_url_playlist":
            session["input_url_playlist"] = [user_input] if user_input else []
        elif current_step_key_local == "quality_settings_prompt":
            quality_input = user_input.lower()
            if quality_input in QUALITY_SETTINGS_MAP.keys() or quality_input == "custom":
                session["quality_settings"] = quality_input
                if quality_input in QUALITY_SETTINGS_MAP and quality_input != "custom":
                     for key, val in QUALITY_SETTINGS_MAP[quality_input].items(): session[key] = val
                append_user_live_log(chat_id, f"Quality set to {quality_input} via text.")
            else:
                # This return is problematic inside a lock. Need to refactor or just set error and continue.
                # For now, let it pass, but ideally, validation handles this before lock.
                # Re-prompting from here is complex.
                logger.warning(f"[Chat {chat_id}] Invalid quality input '{quality_input}' in conversation step. Not applied.")
        elif current_step_key_local == "loop_count" or current_step_key_local == "fps":
            session[current_step_key_local] = int(user_input)
        else:
            session[current_step_key_local] = user_input
        
        append_user_live_log(chat_id, f"Set {current_step_key_local} = {user_input}")

        session['current_step_index'] += 1
        if session['current_step_index'] < len(session['conversation_fields_list']):
            next_step_key, next_prompt, _ = session['conversation_fields_list'][session['current_step_index']]
            session['current_step'] = next_step_key

            if next_step_key == "quality_settings_prompt":
                 return send_message(chat_id, next_prompt, reply_markup={
                    "inline_keyboard": [
                        [{"text": q.title(), "callback_data": f"set_quality_{q}"} for q in ["low", "medium", "high", "custom"]]
                    ]})
            
            current_value_msg = f"(Current: `{session.get(next_step_key, 'Not set')}`)" if session.get(next_step_key) else ""
            return send_message(chat_id, f"{next_prompt}\n{current_value_msg}\n\nType /cancel to abort setup.")
        else:
            session['current_step'] = None
            session['current_step_index'] = 0
            # save_user_settings_to_file(chat_id) # REMOVED
            logger.info(f"[Chat {chat_id}] In-memory settings configured.")
            return send_message(chat_id, "All settings configured (in memory for this session)!\n" + compose_status_message(chat_id, True), reply_markup=get_main_keyboard(session), parse_mode="HTML")


async def handle_playlist_command(chat_id: int, text: str):
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]
    parts = text.split(maxsplit=2)
    action = parts[1].lower() if len(parts) > 1 else "show"
    msg = ""
    
    with lock:
        playlist = session.get('input_url_playlist', [])
        if action == "add" and len(parts) > 2:
            url_to_add = parts[2]
            if validate_url(url_to_add):
                playlist.append(url_to_add)
                # session['input_url_playlist'] = playlist # Modified in place
                msg = f"Added to playlist. Total items: {len(playlist)}."
            else:
                msg = "Invalid URL format."
        elif action == "remove" and len(parts) > 2:
            idx_str = parts[2]
            removed_item = None
            if idx_str.lower() == "last" and playlist: removed_item = playlist.pop()
            elif idx_str.isdigit():
                idx_to_remove = int(idx_str) -1 
                if 0 <= idx_to_remove < len(playlist): removed_item = playlist.pop(idx_to_remove)
            if removed_item: msg = f"Removed '{removed_item[:50]}...' from playlist. Total items: {len(playlist)}."
            else: msg = "Invalid index or playlist empty."
        elif action == "clear":
            session['input_url_playlist'] = []
            msg = "Playlist cleared."
        elif action == "show":
            if playlist:
                plist_str = "\n".join([f"{i+1}. `{url}`" for i, url in enumerate(playlist)])
                msg = f"*Current Playlist ({len(playlist)} items):*\n{plist_str}"
            else: msg = "Playlist is empty."
        else: msg = "Usage: /playlist [add <url>|remove <index|last>|clear|show]"
    
    # save_user_settings_to_file(chat_id) # REMOVED (for playlist changes)
    return send_message(chat_id, msg, reply_markup=get_main_keyboard(session))

async def handle_playlist_config_interaction(chat_id: int, message_id: int):
    session = get_user_session(chat_id)
    playlist = session.get('input_url_playlist', [])
    msg = ""
    if playlist:
        plist_str = "\n".join([f"{i+1}. `{url}`" for i, url in enumerate(playlist)])
        msg = f"*Current Playlist ({len(playlist)} items):*\n{plist_str}\n\nUse `/playlist add <url>`, `/playlist remove <index>`, `/playlist clear`."
    else:
        msg = "Playlist is empty. Use `/playlist add <url>` to add your first stream input."
    return edit_message_text(chat_id, message_id, msg + "\n\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session))


async def handle_logo_data_from_telegram(chat_id: int, message: dict):
    """
    This function is a placeholder.
    To get file bytes, the bot needs to:
    1. Get `file_id` from `message` (photo or document).
    2. Call Telegram API's `getFile` method with `file_id` to get a `file_path`.
    3. Download the file content from `https://api.telegram.org/file/bot<YOUR_BOT_TOKEN>/<file_path>`.
    This webhook setup cannot make these outgoing API calls.
    If you are using a library like `python-telegram-bot` which handles the webhook and provides
    the downloaded file bytes directly (e.g. via `message.effective_attachment.download_as_bytearray()`),
    then you would use that.

    For this example, we'll simulate that the bytes are somehow obtained.
    """
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]

    # --- SIMULATION ---
    # In a real bot that can make API calls or using a full bot framework:
    # actual_logo_bytes = await download_file_from_telegram(file_id) 
    # actual_mime_type = message["document"].get("mime_type") or "image/png" (if photo)
    # actual_filename = message["document"].get("file_name") or f"photo_logo_{chat_id}.png"
    # For now, we can't implement the download here. User will be told it's conceptual.
    
    # Simulate receiving some dummy data
    dummy_logo_bytes = b"dummy_logo_data_placeholder_not_a_real_image"
    dummy_mime_type = "image/png"
    dummy_filename = f"concept_logo_{chat_id}.png"
    # --- END SIMULATION ---

    # Check file size (conceptual, as we don't have real bytes here)
    MAX_LOGO_SIZE_BYTES = 1 * 1024 * 1024  # 1 MB
    # if len(actual_logo_bytes) > MAX_LOGO_SIZE_BYTES:
    #     return send_message(chat_id, f"Logo is too large (max {MAX_LOGO_SIZE_BYTES/1024/1024:.1f} MB). Please upload a smaller one.")

    with lock:
        session['logo_data_bytes'] = dummy_logo_bytes # actual_logo_bytes
        session['logo_mime_type'] = dummy_mime_type # actual_mime_type
        session['logo_original_filename'] = dummy_filename # actual_filename
        session['logo_enabled'] = True
        session['current_step'] = None
    
    # save_user_settings_to_file(chat_id) # REMOVED
    append_user_live_log(chat_id, f"CONCEPTUAL: Logo data for '{dummy_filename}' stored in memory.")
    return send_message(chat_id, f"Conceptual: Logo '{dummy_filename}' data stored in memory and enabled! (Actual file download/processing requires full bot capabilities beyond this webhook example.)\nLogo settings (position, etc.) are in Advanced Settings.", reply_markup=get_main_keyboard(session))

async def handle_logo_config_interaction(chat_id: int, message_id: int):
    session = get_user_session(chat_id)
    msg_parts = ["*Logo Configuration (In-Memory):*"]
    if session.get('logo_data_bytes') and session.get('logo_enabled'):
        msg_parts.append(f"Status: Enabled (`{session.get('logo_original_filename', 'N/A')}`)")
        msg_parts.append(f"Position: `{session.get('logo_position')}` (Change in Advanced Settings)")
        msg_parts.append(f"Scale: `{session.get('logo_scale')}` (Change in Advanced Settings)")
        msg_parts.append(f"Opacity: `{session.get('logo_opacity')}` (Change in Advanced Settings)")
        msg_parts.append("\nTo change logo, send /set_logo then upload a new image.")
        msg_parts.append("To disable: `/configure_logo enabled false` (conceptual command).")
    elif session.get('logo_data_bytes') and not session.get('logo_enabled'):
        msg_parts.append(f"Status: Disabled (`{session.get('logo_original_filename', 'N/A')}`)")
        msg_parts.append("To enable: `/configure_logo enabled true` (conceptual command).")
    else:
        msg_parts.append("No logo uploaded/stored. Send /set_logo then upload an image.")
    
    return edit_message_text(chat_id, message_id, "\n".join(msg_parts) + "\n\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session))


def schedule_stream_job(chat_id: int, trigger_time: datetime.datetime, job_name: str = None):
    session = get_user_session(chat_id) 
    job_id = f"stream_{chat_id}_{int(trigger_time.timestamp())}"
    if not job_name: job_name = f"Scheduled Stream for {chat_id} at {trigger_time.strftime('%Y-%m-%d %H:%M')}"
    
    scheduler.add_job(
        start_stream_for_scheduler, 
        'date', 
        run_date=trigger_time, 
        args=[chat_id], 
        id=job_id, 
        name=job_name,
        replace_existing=True
    )
    append_user_live_log(chat_id, f"Stream scheduled (in-memory): {job_name} (ID: {job_id}) at {trigger_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    logger.info(f"[Chat {chat_id}] Job {job_id} scheduled (in-memory) for {trigger_time}")

def start_stream_for_scheduler(chat_id: int):
    logger.info(f"[Scheduler][Chat {chat_id}] Triggering scheduled stream (in-memory).")
    session = get_user_session(chat_id)
    # Ensure the asyncio event loop is available if start_stream_command_handler is async
    # This can be tricky when APScheduler runs in a separate thread.
    # A common pattern is to use asyncio.run_coroutine_threadsafe if the scheduler
    # has access to the main event loop, or just run the core logic synchronously if possible.
    # For simplicity, let's assume start_stream_command_handler can be adapted or called carefully.
    
    # Simplified: this call will need an event loop if `start_stream_command_handler` is async.
    # If `start_stream_command_handler` is sync, it's fine.
    # Since it *is* async, this needs proper async handling from a sync thread.
    loop = asyncio.get_event_loop() # Get loop from the thread APScheduler is running in
    if loop.is_running():
        asyncio.create_task(start_stream_command_handler(chat_id, is_scheduled=True))
    else: # Fallback for environments where loop might not be running in APScheduler's thread
        asyncio.run(start_stream_command_handler(chat_id, is_scheduled=True))
    
    logger.info(f"[Scheduler][Chat {chat_id}] Scheduled stream start attempt initiated.")


async def handle_schedule_command(chat_id: int, text: str):
    parts = text.split(maxsplit=3)
    if len(parts) < 3:
        return send_message(chat_id, "Usage: `/schedule YYYY-MM-DD HH:MM:SS [Optional Job Name]`\nExample: `/schedule 2024-12-31 23:55:00 NYE`\nTimezone is UTC. Schedules are in-memory and lost on restart.")

    try:
        date_str, time_str = parts[1], parts[2]
        job_name = parts[3] if len(parts) > 3 else f"Stream for {chat_id}"
        trigger_time_naive = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
        trigger_time_utc = trigger_time_naive.replace(tzinfo=datetime.timezone.utc)

        if trigger_time_utc <= datetime.datetime.now(datetime.timezone.utc):
            return send_message(chat_id, "Scheduled time must be in the future.")

        schedule_stream_job(chat_id, trigger_time_utc, job_name)
        return send_message(chat_id, f"Stream scheduled (in-memory) as '{job_name}' for {trigger_time_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}. Current settings will be used.", reply_markup=get_main_keyboard(session))
    except ValueError as e:
        return send_message(chat_id, f"Invalid date/time format: {e}. Use YYYY-MM-DD HH:MM:SS.")

async def handle_schedule_config_interaction(chat_id: int, message_id: int):
    jobs = scheduler.get_jobs()
    user_jobs = [job for job in jobs if job.id.startswith(f"stream_{chat_id}_")]
    
    msg_parts = ["*Scheduled Streams (In-Memory - Lost on Restart):*"]
    if user_jobs:
        for job in user_jobs:
            run_time_str = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S %Z') if job.next_run_time else "N/A"
            msg_parts.append(f"• `{job.name}` at `{run_time_str}` (ID: `{job.id}`)")
            msg_parts.append(f"  To cancel: `/cancel_schedule {job.id}` (conceptual command).")
    else:
        msg_parts.append("No streams currently scheduled.")
    msg_parts.append("\nUse `/schedule YYYY-MM-DD HH:MM:SS [Name]` to schedule a new stream.")
    
    return edit_message_text(chat_id, message_id, "\n".join(msg_parts) + "\n\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session))


async def start_stream_command_handler(chat_id: int, message_id_to_edit: int = None, is_scheduled: bool = False):
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]
    response = None

    with lock:
        if session['streaming_state'] in ["streaming", "paused", "starting"]:
            msg = f"A stream is already active ({session['streaming_state']})."
            if message_id_to_edit: response = edit_message_text(chat_id, message_id_to_edit, msg + "\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session), parse_mode="HTML")
            else: response = send_message(chat_id, msg, reply_markup=get_main_keyboard(session))
            return response

        if not session.get('input_url_playlist') or not session.get('output_url'):
            msg = "Input playlist or Output URL is not set. Please configure first using /settings or menu."
            if message_id_to_edit: response = edit_message_text(chat_id, message_id_to_edit, msg + "\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session), parse_mode="HTML")
            else: response = send_message(chat_id, msg, reply_markup=get_main_keyboard(session))
            return response

        session['streaming_state'] = "starting"
        session['error_notification_user'] = ""
        
        # save_user_settings_to_file(chat_id) # REMOVED

        thread = threading.Thread(target=stream_engine_thread_target, args=(chat_id,), name=f"StreamThread-{chat_id}", daemon=True)
        session['stream_thread_ref'] = thread
        thread.start()
        append_user_live_log(chat_id, "Stream thread initiated.")
    
    await asyncio.sleep(0.2)

    status_msg = compose_status_message(chat_id)
    if is_scheduled and not message_id_to_edit:
        logger.info(f"[Chat {chat_id}] Scheduled stream started. Status: {status_msg[:100]}")
        # Conceptual proactive message:
        # await send_proactive_message(chat_id, "Your scheduled stream is now starting!\n" + status_msg)
        return {"status": "scheduled_stream_started_silently_inmemory"}
    elif message_id_to_edit:
        response = edit_message_text(chat_id, message_id_to_edit, "Stream starting...\n" + status_msg, reply_markup=get_main_keyboard(session), parse_mode="HTML")
    else:
        response = send_message(chat_id, "Stream starting...\n" + status_msg, reply_markup=get_main_keyboard(session), parse_mode="HTML")
    return response


async def stream_pause_handler(chat_id: int, message_id: int):
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]
    with lock:
        if session['streaming_state'] == "streaming":
            session['streaming_state'] = "paused"
            append_user_live_log(chat_id, "Stream paused by user.")
            msg = "Stream paused."
        else:
            msg = f"Stream is not streaming (state: {session['streaming_state']}). Cannot pause."
    return edit_message_text(chat_id, message_id, msg + "\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session), parse_mode="HTML")

async def stream_resume_handler(chat_id: int, message_id: int):
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]
    with lock:
        if session['streaming_state'] == "paused":
            session['streaming_state'] = "streaming"
            append_user_live_log(chat_id, "Stream resumed by user.")
            msg = "Stream resumed."
        else:
            msg = f"Stream is not paused (state: {session['streaming_state']}). Cannot resume."
    return edit_message_text(chat_id, message_id, msg + "\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session), parse_mode="HTML")

async def stream_abort_handler(chat_id: int, message_id_to_edit: int = None):
    session = get_user_session(chat_id)
    lock = session_locks[chat_id]
    thread_to_join = None
    msg = ""
    
    with lock:
        if session['streaming_state'] in ["streaming", "paused", "starting"]:
            session['streaming_state'] = "stopping" 
            thread_to_join = session.get('stream_thread_ref')
            append_user_live_log(chat_id, "Stream abort sequence initiated by user.")
            msg = "Stream aborting..."
        else:
            msg = f"No active stream to abort (state: {session['streaming_state']})."
            if message_id_to_edit: return edit_message_text(chat_id, message_id_to_edit, msg + "\n" + compose_status_message(chat_id), reply_markup=get_main_keyboard(session), parse_mode="HTML")
            return send_message(chat_id, msg, reply_markup=get_main_keyboard(session))

    if thread_to_join and thread_to_join.is_alive():
        logger.info(f"[Chat {chat_id}] Waiting for stream thread {thread_to_join.name} to join...")
        thread_to_join.join(timeout=7.0)
        if thread_to_join.is_alive():
            append_user_live_log(chat_id, "Warning: Stream thread did not terminate cleanly after abort signal.")
            logger.warning(f"[Chat {chat_id}] Stream thread {thread_to_join.name} still alive after join timeout.")
        else:
            append_user_live_log(chat_id, "Stream thread terminated.")
    
    with lock:
        if session['streaming_state'] == "stopping":
            session['streaming_state'] = "stopped"
        session['stream_thread_ref'] = None

    final_status_msg = "Stream aborted.\n" + compose_status_message(chat_id)
    if message_id_to_edit:
        return edit_message_text(chat_id, message_id_to_edit, final_status_msg, reply_markup=get_main_keyboard(session), parse_mode="HTML")
    else:
        return send_message(chat_id, final_status_msg, reply_markup=get_main_keyboard(session), parse_mode="HTML")


def get_help_text():
    return (
        f"*Advanced Stream Bot v{APP_VERSION} (In-Memory Edition) Help*\n\n"
        "⚠️ *IMPORTANT: All settings, uploaded logos, and schedules are stored in memory "
        "and will be LOST if the bot restarts due to environment restrictions.*\n\n"
        "Use the interactive menu buttons or the following commands:\n"
        "*/start* or */menu* - Show main menu and status.\n"
        "*/settings* - Start advanced configuration.\n"
        "*/stream* - Start stream with current settings.\n"
        "*/playlist add <url>* - Add URL to playlist.\n"
        "*/playlist remove <index|last>* - Remove URL from playlist.\n"
        "*/playlist clear* - Clear entire playlist.\n"
        "*/playlist show* - Display current playlist.\n"
        "*/set_logo* - Prompts to upload a logo image (stored in memory).\n"
        "   (Logo settings like position, scale are in Advanced Settings via menu)\n"
        "*/schedule YYYY-MM-DD HH:MM:SS [Name]* - Schedule a stream (UTC time, in-memory).\n"
        "   Example: `/schedule 2024-07-04 18:30:00 My Stream`\n"
        "*/status* - Show detailed stream status and config.\n"
        "*/abort* - Force stop the current stream.\n"
        "\n*While Streaming (via menu buttons):*\n"
        "• Pause/Resume Stream.\n"
        "• Abort Stream.\n"
        "• Status Update (refreshes message).\n"
        "• Stop Loop Gracefully (for infinite loops, current iteration completes).\n"
        "\n*Configuration Conversation:*\n"
        "When in a setup process (e.g., after /settings), reply to prompts.\n"
        "Type `/cancel` to exit setup anytime.\n"
    )

# --- FastAPI Webhook Endpoint ---
import asyncio

@app.on_event("startup")
async def startup_event():
    # load_all_sessions_on_startup() # REMOVED
    if not scheduler.running:
        scheduler.start()
        logger.info("APScheduler started (using MemoryJobStore).")
    logger.info(f"FastAPI application startup complete. Version: {APP_VERSION}. Data is IN-MEMORY.")
    logger.warning("IMPORTANT: All user settings, logos, and schedules will be lost on bot restart.")

@app.on_event("shutdown")
async def shutdown_event():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("APScheduler shutdown.")
    # No settings to save to file
    logger.info("FastAPI application shutdown. In-memory data will be lost.")


@app.post("/webhook")
async def telegram_webhook_endpoint(request: Request):
    try:
        update = await request.json()
        # logger.debug(f"Webhook received: {update}") # Can be too verbose
        
        response_data = await handle_telegram_update(update)
        
        if isinstance(response_data, list):
            main_response = None
            for r_item in response_data:
                if r_item and r_item.get("method") in ["sendMessage", "editMessageText", "sendPhoto", "sendDocument"]: # Added more types
                    main_response = r_item
                    break
            if not main_response:
                 main_response = response_data[0] if response_data else {"status": "ok", "message": "Empty response list from handler"}
            return main_response
        
        return response_data
    
    except json.JSONDecodeError:
        logger.error("Invalid JSON received in webhook.")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error processing webhook: {e}", exc_info=True)
        return {"status": "ok", "message": "Internal server error processing update."} # Ack to Telegram

@app.get("/")
async def root():
    return {"message": f"Advanced Stream Bot v{APP_VERSION} (In-Memory Edition) is running. Webhook is at /webhook."}


# --- Main Execution ---
# if __name__ == "__main__":
#     import uvicorn
#     logger.info(f"Starting Advanced Stream Bot v{APP_VERSION} (In-Memory Edition)...")
#     logger.warning("This version stores all data in memory. NO DATA WILL PERSIST ACROSS RESTARTS.")
#     uvicorn.run("__main__:app", host="127.0.0.1", port=8000, reload=True)
