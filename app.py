import logging
import threading
import time
import datetime
import traceback
import fractions

from fastapi import FastAPI, Request
import av

app = FastAPI()

# -------------------------------------------------------------------
# Configuration & Global Variables
# -------------------------------------------------------------------
# (No token or outgoing HTTP calls are used in this version.)
# Conversation state
user_inputs = {}
# The conversation fields will depend on the mode.
# Simple mode (default): Only "input_url" and "output_url" are required.
# Advanced mode (if user sends /setting): Additional fields are required.
conversation_fields = []
current_step = None
advanced_mode = False

# Default settings for advanced fields
default_settings = {
    "quality_settings": "medium",
    "video_codec": "libx264",
    "audio_codec": "aac",
    "output_url": "rtmp://a.rtmp.youtube.com/live2"
}

# Streaming state & statistics
streaming_state = "idle"  # "idle", "streaming", "paused", "stopped"
stream_chat_id = None     # Chat ID for periodic updates
stream_start_time = None
frames_encoded = 0
bytes_sent = 0

# Stream resource objects
video_stream = None
audio_stream_in = None
output_stream = None

# Thread references
stream_thread = None
live_log_thread = None

# Live logging globals
live_log_lines = []     # Rolling list (max 50 log lines)
live_log_display = ""   # Global variable updated every second by live_log_updater
error_notification = "" # Global variable to hold error details if any

# -------------------------------------------------------------------
# Enhanced Logging Setup
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()

def append_live_log(line: str):
    global live_log_lines
    live_log_lines.append(line)
    if len(live_log_lines) > 50:
        live_log_lines.pop(0)

class ListHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        append_live_log(log_entry)

list_handler = ListHandler()
list_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(list_handler)

# -------------------------------------------------------------------
# Helper Function: Compose Streaming Message (with extra stats)
# -------------------------------------------------------------------
def compose_streaming_message():
    global live_log_display, error_notification, streaming_state, frames_encoded, bytes_sent
    stats = f"State: {streaming_state} | Uptime: {get_uptime()} | Frames: {frames_encoded} | Bytes: {bytes_sent}\n"
    msg = ""
    if error_notification:
        msg += "<b>ERROR:</b> " + error_notification + "\n\n"
    msg += "🚀 <b>Streaming in progress!</b>\n"
    msg += stats + "\n"
    msg += "Live Logs:\n" + live_log_display + "\n\nUse the inline keyboard to control the stream."
    return msg

# -------------------------------------------------------------------
# Utility Functions & UI Helpers
# -------------------------------------------------------------------
def create_html_message(text: str):
    # Wrap text in <pre> tags for monospaced output using HTML parse mode
    return {"parse_mode": "HTML", "text": f"<pre>{text}</pre>"}

def get_inline_keyboard_for_stream():
    # Inline keyboard for streaming controls after the stream has started
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "⏸ Pause", "callback_data": "pause"},
                {"text": "▶️ Resume", "callback_data": "resume"},
                {"text": "⏹ Abort", "callback_data": "abort"}
            ],
            [
                {"text": "📊 Status", "callback_data": "status"}
            ]
        ]
    }
    return keyboard

def get_inline_keyboard_for_start():
    # Inline keyboard with a start button for when conversation is complete.
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "🚀 Start Streaming", "callback_data": "start_stream"}
            ]
        ]
    }
    return keyboard

def help_text():
    return (
        "*Stream Bot Help*\n\n"
        "*/start* - Begin setup for streaming (simple mode: only Input & Output URL)\n"
        "*/setting* - Enter advanced settings (Input URL, Quality Settings, Video Codec, Audio Codec, Output URL)\n"
        "*/help* - Display this help text\n"
        "*/logs* - Show the log history (live log display)\n\n"
        "After inputs are collected, press the inline *Start Streaming* button.\n\n"
        "While streaming, you can use inline buttons or commands:\n"
        "*/pause* - Pause the stream\n"
        "*/resume* - Resume a paused stream\n"
        "*/abort* - Abort the stream\n"
        "*/status* - Get current stream statistics"
    )

def send_guide_message(chat_id, message):
    # Return a response dictionary to be sent as the webhook reply
    logging.info(f"Sending message to chat {chat_id}: {message}")
    return {
        "method": "sendMessage",
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }

def reset_statistics():
    global stream_start_time, frames_encoded, bytes_sent
    stream_start_time = datetime.datetime.now()
    frames_encoded = 0
    bytes_sent = 0

def get_uptime():
    if stream_start_time:
        uptime = datetime.datetime.now() - stream_start_time
        return str(uptime).split('.')[0]
    return "0"

def validate_inputs():
    # Ensure all fields in conversation_fields have been provided
    missing = [field for field in conversation_fields if field not in user_inputs or not user_inputs[field]]
    if missing:
        return False, f"Missing fields: {', '.join(missing)}"
    return True, ""

# -------------------------------------------------------------------
# Error Notification Helper
# -------------------------------------------------------------------
def notify_error(chat_id, error_message):
    global error_notification
    error_notification = error_message
    logging.error(f"Error for chat {chat_id}: {error_message}")

# -------------------------------------------------------------------
# Live Log Updater (Background Thread)
# -------------------------------------------------------------------
def live_log_updater():
    global live_log_display, streaming_state
    try:
        while streaming_state in ["streaming", "paused"]:
            # Update the global live_log_display with the last 15 log lines in HTML format
            live_log_display = "<pre>" + "\n".join(live_log_lines[-15:]) + "</pre>"
            time.sleep(1)
    except Exception as e:
        logging.error(f"Error in live log updater: {e}")

# -------------------------------------------------------------------
# Logs History Handler (/logs)
# -------------------------------------------------------------------
def logs_history(chat_id):
    global live_log_display, error_notification
    log_text = live_log_display if live_log_display else "<pre>No logs available yet.</pre>"
    if error_notification:
        if log_text.startswith("<pre>"):
            log_text = f"<pre>ERROR: {error_notification}\n\n" + log_text[5:]
        else:
            log_text = f"<pre>ERROR: {error_notification}\n\n{log_text}</pre>"
    return {
         "method": "sendMessage",
         "chat_id": chat_id,
         "text": log_text,
         "parse_mode": "HTML"
    }

# -------------------------------------------------------------------
# Conversation Handlers
# -------------------------------------------------------------------
def handle_start(chat_id):
    global current_step, user_inputs, conversation_fields, advanced_mode
    # By default, use simple mode (unless advanced_mode was set via /setting)
    user_inputs = {}
    if not advanced_mode:
        conversation_fields = ["input_url", "output_url"]
    else:
        conversation_fields = ["input_url", "quality_settings", "video_codec", "audio_codec", "output_url"]
    current_step = conversation_fields[0]
    text = ("👋 *Welcome to the Stream Bot!*\n\n"
            "Let's set up your stream.\n"
            f"Please enter the *{current_step.replace('_', ' ')}*"
            f"{' (no default)' if current_step not in default_settings else f' _(default: {default_settings[current_step]})_'}:")
    logging.info(f"/start command from chat {chat_id} (advanced_mode={advanced_mode})")
    return {
        "method": "sendMessage",
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }

def handle_setting(chat_id):
    global advanced_mode, conversation_fields, current_step, user_inputs
    advanced_mode = True
    conversation_fields = ["input_url", "quality_settings", "video_codec", "audio_codec", "output_url"]
    user_inputs = {}
    current_step = conversation_fields[0]
    text = ("⚙️ *Advanced Mode Activated!*\n\n"
            "Please enter the *input url*:") 
    logging.info(f"/setting command from chat {chat_id} - advanced mode enabled")
    return {
        "method": "sendMessage",
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }

def handle_help(chat_id):
    logging.info(f"/help command from chat {chat_id}")
    return {
        "method": "sendMessage",
        "chat_id": chat_id,
        "text": help_text(),
        "parse_mode": "Markdown"
    }

def handle_conversation(chat_id, text):
    global current_step, user_inputs, conversation_fields
    if current_step:
        if text.strip() == "" and current_step in default_settings:
            user_inputs[current_step] = default_settings[current_step]
            logging.info(f"Using default for {current_step}: {default_settings[current_step]}")
        else:
            user_inputs[current_step] = text.strip()
            logging.info(f"Received {current_step}: {text.strip()}")

        idx = conversation_fields.index(current_step)
        if idx < len(conversation_fields) - 1:
            current_step = conversation_fields[idx + 1]
            prompt = f"Please enter the *{current_step.replace('_', ' ')}*"
            if current_step in default_settings:
                prompt += f" _(default: {default_settings[current_step]})_"
            return send_guide_message(chat_id, prompt)
        else:
            current_step = None
            valid, msg = validate_inputs()
            if not valid:
                return send_guide_message(chat_id, f"Validation error: {msg}")
            if not advanced_mode:
                user_inputs.setdefault("quality_settings", default_settings["quality_settings"])
                user_inputs.setdefault("video_codec", default_settings["video_codec"])
                user_inputs.setdefault("audio_codec", default_settings["audio_codec"])
            return {
                "method": "sendMessage",
                "chat_id": chat_id,
                "text": "All inputs received. Press *🚀 Start Streaming* to begin.",
                "reply_markup": get_inline_keyboard_for_start(),
                "parse_mode": "Markdown"
            }
    else:
        return send_guide_message(chat_id, "Unrecognized input. Type /help for available commands.")

# -------------------------------------------------------------------
# Background Streaming Functions
# -------------------------------------------------------------------
def stream_to_youtube(input_url, quality_settings, video_codec, audio_codec, output_url, chat_id):
    global video_stream, audio_stream_in, output_stream, streaming_state, frames_encoded, bytes_sent
    logging.info("Initiating streaming to YouTube")
    try:
        streaming_state = "streaming"
        reset_statistics()

        input_stream = av.open(input_url)
        output_stream = av.open(output_url, mode='w', format='flv')

        # Configure video stream
        video_stream = output_stream.add_stream(video_codec, rate=30)
        video_stream.width = input_stream.streams.video[0].width
        video_stream.height = input_stream.streams.video[0].height
        video_stream.pix_fmt = input_stream.streams.video[0].format.name
        video_stream.codec_context.options.update({'g': '30'})

        if quality_settings.lower() == "high":
            video_stream.bit_rate = 3000000
            video_stream.bit_rate_tolerance = 1000000
        elif quality_settings.lower() == "medium":
            video_stream.bit_rate = 1500000
            video_stream.bit_rate_tolerance = 500000
        elif quality_settings.lower() == "low":
            video_stream.bit_rate = 800000
            video_stream.bit_rate_tolerance = 200000

        # Configure audio stream
        audio_stream_in = input_stream.streams.audio[0]
        out_audio_stream = output_stream.add_stream(audio_codec, rate=audio_stream_in.rate)
        out_audio_stream.layout = "stereo"

        # video_stream.codec_context.time_base = fractions.Fraction(1, video_stream.rate)
        video_stream.codec_context.time_base = fractions.Fraction(1, 30)

        logging.info("Streaming started successfully.")

        # Start the live log updater in a background thread if not already running.
        global live_log_thread
        if live_log_thread is None or not live_log_thread.is_alive():
            live_log_thread = threading.Thread(target=live_log_updater)
            live_log_thread.daemon = True
            live_log_thread.start()
            logging.info("Live log updater thread started.")

        # Stream loop: process packets until state changes
        while streaming_state in ["streaming", "paused"]:
            for packet in input_stream.demux():
                if streaming_state == "stopped":
                    break
                if packet.stream == input_stream.streams.video[0]:
                    for frame in packet.decode():
                        if streaming_state == "paused":
                            time.sleep(0.5)
                            continue
                        for out_packet in video_stream.encode(frame):
                            output_stream.mux(out_packet)
                            frames_encoded += 1
                            if hasattr(out_packet, "size"):
                                bytes_sent += out_packet.size
                elif packet.stream == audio_stream_in:
                    for frame in packet.decode():
                        if streaming_state == "paused":
                            time.sleep(0.5)
                            continue
                        for out_packet in out_audio_stream.encode(frame):
                            output_stream.mux(out_packet)
                            if hasattr(out_packet, "size"):
                                bytes_sent += out_packet.size

            # Flush remaining packets
            for out_packet in video_stream.encode():
                output_stream.mux(out_packet)
            for out_packet in out_audio_stream.encode():
                output_stream.mux(out_packet)

            if streaming_state == "paused":
                time.sleep(1)

        # Clean up resources
        try:
            video_stream.close()
            out_audio_stream.close()
            output_stream.close()
            input_stream.close()
        except Exception as cleanup_error:
            logging.error(f"Error during cleanup: {cleanup_error}")

        logging.info("Streaming complete, resources cleaned up.")
        streaming_state = "idle"
    except Exception as e:
        error_message = f"An error occurred during streaming: {str(e)}\n\n{traceback.format_exc()}"
        logging.error(error_message)
        streaming_state = "idle"
        notify_error(chat_id, error_message)

def start_streaming(chat_id):
    global stream_thread, stream_chat_id
    valid, msg = validate_inputs()
    if not valid:
        return send_guide_message(chat_id, f"Validation error: {msg}")

    stream_chat_id = chat_id
    try:
        stream_thread = threading.Thread(
            target=stream_to_youtube,
            args=(
                user_inputs["input_url"],
                user_inputs["quality_settings"],
                user_inputs["video_codec"],
                user_inputs["audio_codec"],
                user_inputs["output_url"],
                chat_id,
            )
        )
        stream_thread.daemon = True
        stream_thread.start()
        logging.info("Streaming thread started.")
        # Immediately return a message that includes the live log display via compose_streaming_message()
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": "🚀 <b>Streaming initiated!</b>\n\n" + compose_streaming_message(),
            "reply_markup": get_inline_keyboard_for_stream(),
            "parse_mode": "HTML"
        }
    except Exception as e:
        error_message = f"Failed to start streaming: {str(e)}"
        logging.error(error_message)
        notify_error(chat_id, error_message)
        return send_guide_message(chat_id, error_message)

# -------------------------------------------------------------------
# Stream Control Handlers
# -------------------------------------------------------------------
def pause_stream(chat_id):
    global streaming_state
    if streaming_state == "streaming":
        streaming_state = "paused"
        logging.info("Streaming paused.")
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": "⏸ <b>Streaming paused.</b>",
            "parse_mode": "HTML"
        }
    return send_guide_message(chat_id, "Streaming is not active.")

def resume_stream(chat_id):
    global streaming_state
    if streaming_state == "paused":
        streaming_state = "streaming"
        logging.info("Streaming resumed.")
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": "▶️ <b>Streaming resumed.</b>",
            "parse_mode": "HTML"
        }
    return send_guide_message(chat_id, "Streaming is not paused.")

def abort_stream(chat_id):
    global streaming_state
    if streaming_state in ["streaming", "paused"]:
        streaming_state = "stopped"
        logging.info("Streaming aborted by user.")
        return {
            "method": "sendMessage",
            "chat_id": chat_id,
            "text": "⏹ <b>Streaming aborted.</b>",
            "parse_mode": "HTML"
        }
    return send_guide_message(chat_id, "No active streaming to abort.")

def stream_status(chat_id):
    stats = (
        f"*Stream Status:*\n\n"
        f"• **State:** {streaming_state}\n"
        f"• **Uptime:** {get_uptime()}\n"
        f"• **Frames Encoded:** {frames_encoded}\n"
        f"• **Bytes Sent:** {bytes_sent}\n"
    )
    return {
        "method": "sendMessage",
        "chat_id": chat_id,
        "text": stats,
        "parse_mode": "Markdown"
    }

# -------------------------------------------------------------------
# FastAPI Webhook Endpoint for Telegram Updates
# -------------------------------------------------------------------
@app.post("/webhook")
async def telegram_webhook(request: Request):
    update = await request.json()
    logging.debug(f"Received update: {update}")

    # Process messages from users
    if "message" in update:
        chat_id = update["message"]["chat"]["id"]
        text = update["message"].get("text", "").strip()

        if text.startswith("/setting"):
            return handle_setting(chat_id)
        elif text.startswith("/start"):
            return handle_start(chat_id)
        elif text.startswith("/help"):
            return handle_help(chat_id)
        elif text.startswith("/logs"):
            return logs_history(chat_id)
        elif text.startswith("/pause"):
            return pause_stream(chat_id)
        elif text.startswith("/resume"):
            return resume_stream(chat_id)
        elif text.startswith("/abort"):
            return abort_stream(chat_id)
        elif text.startswith("/status"):
            return stream_status(chat_id)
        else:
            return handle_conversation(chat_id, text)

    # Process inline keyboard callback queries
    elif "callback_query" in update:
        callback_data = update["callback_query"]["data"]
        chat_id = update["callback_query"]["message"]["chat"]["id"]
        message_id = update["callback_query"]["message"]["message_id"]

        if callback_data == "pause":
            response = pause_stream(chat_id)
        elif callback_data == "resume":
            response = resume_stream(chat_id)
        elif callback_data == "abort":
            response = abort_stream(chat_id)
        elif callback_data == "status":
            response = stream_status(chat_id)
        elif callback_data == "start_stream":
            response = start_streaming(chat_id)
        else:
            response = send_guide_message(chat_id, "❓ Unknown callback command.")

        # Always update the message text with the latest live logs and keep the inline keyboard
        if callback_data in ["pause", "resume", "abort", "status", "start_stream"]:
            response["method"] = "editMessageText"
            response["message_id"] = message_id
            response["text"] = compose_streaming_message()
            response["parse_mode"] = "HTML"
            response["reply_markup"] = get_inline_keyboard_for_stream()
        return response

    return {"status": "ok"}
