"""Bot replay publik: antrean rekaman portrait dengan suara dan deteksi akhir."""
import asyncio
import io
import ipaddress
import logging
import os
import re
import shutil
import socket
import tempfile
import time
from contextlib import asynccontextmanager
from collections import deque
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlsplit

from PIL import Image, ImageGrab, ImageChops
from playwright.async_api import async_playwright
from telegram import InputFile
from telegram.error import NetworkError, RetryAfter, TelegramError
from telegram.ext import Application, CommandHandler, MessageHandler, filters

LOG = logging.getLogger("replay")
WIDTH, HEIGHT, FPS = 720, 1280, 24
MAX_OUTPUT = 45 * 1024 * 1024
VIDEO_QUEUE_FRAMES = 8
AUDIO_QUEUE_PACKETS = 64
LINK = re.compile(r"https://(?:www\.)?ppreplaylink\.net/[A-Za-z0-9_-]{5,100}/?", re.I)


class ReplayError(Exception):
    pass


def memory_oom_kills():
    try:
        for line in Path("/sys/fs/cgroup/memory.events").read_text().splitlines():
            name, value = line.split()
            if name == "oom_kill":
                return int(value)
    except (OSError, ValueError):
        pass
    return None


def replay_url(text):
    value = text.strip()
    if not LINK.fullmatch(value):
        raise ReplayError("Kirim satu link https://www.ppreplaylink.net/... dalam satu pesan, le.")
    return value


def ending_text(text):
    normalized = re.sub(r"[^A-Z]", "", text.upper().replace("0", "O").replace("1", "I").replace("|", "I"))
    return any(value in normalized for value in ("TONTONLAGI", "WATCHAGAIN", "PLAYAGAIN")) and any(
        value in normalized for value in ("SALINTAUTAN", "COPYLINK"))


def continuation_text(text):
    normalized = re.sub(r"[^A-Z]", "", text.upper().replace("0", "O").replace("1", "I").replace("|", "I"))
    if any(value in normalized for value in ("TONTONLAGI", "WATCHAGAIN", "PLAYAGAIN", "SALINTAUTAN", "COPYLINK")):
        return False
    phrases = (
        "TEKANDIMANASAJAUNTUKMELANJUTKAN", "KLIKDIMANASAJAUNTUKMELANJUTKAN",
        "KETUKDIMANASAJAUNTUKMELANJUTKAN", "PRESSANYWHERETOCONTINUE",
        "CLICKANYWHERETOCONTINUE", "TAPANYWHERETOCONTINUE")
    if any(value in normalized for value in phrases):
        return True
    for match in re.finditer(r"TEKAN|KLIK|KETUK|PRESS|CLICK|TAP", normalized):
        for phrase in phrases:
            for size in (len(phrase) - 1, len(phrase), len(phrase) + 1):
                candidate = normalized[match.start():match.start() + size]
                if SequenceMatcher(None, candidate, phrase).ratio() >= .92:
                    return True
    return False


async def stop_process(process):
    if process is None:
        return
    if process.returncode is None:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
    try:
        await asyncio.wait_for(process.wait(), 5)
    except asyncio.TimeoutError:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await process.wait()


async def command(*args, timeout=20, input=None, env=None):
    process = await asyncio.create_subprocess_exec(
        *map(str, args), stdin=asyncio.subprocess.PIPE if input is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(input), timeout)
    finally:
        await stop_process(process)
    if process.returncode:
        LOG.error("Proses %s gagal (exit=%s): %s", Path(str(args[0])).name,
                  process.returncode, stderr.decode("utf-8", errors="replace")[-2000:])
        raise ReplayError(f"Proses {Path(str(args[0])).name} belum berhasil. Periksa log server.")
    return stdout, stderr


async def wait_ready(process, check, description):
    for _ in range(80):
        if process.returncode is not None:
            raise ReplayError(f"{description} gagal dimulai; periksa log container.")
        if await check():
            return
        await asyncio.sleep(0.1)
    raise ReplayError(f"{description} belum siap.")


@asynccontextmanager
async def desktop(folder):
    """Satu desktop dan satu perangkat audio virtual, khusus rekaman aktif."""
    folder = Path(folder)
    pulse_path = folder / "pulse"
    pulse_path.mkdir(mode=0o700)
    pulse_socket = pulse_path / "native"
    env = {key: value for key, value in os.environ.items()
           if key in {"PATH", "HOME", "LANG", "LC_ALL", "LD_LIBRARY_PATH", "SSL_CERT_FILE"}}
    env.update(DISPLAY=":99", PULSE_RUNTIME_PATH=str(pulse_path),
               PULSE_STATE_PATH=str(folder / "pulse-state"),
               PULSE_SERVER="unix:" + str(pulse_socket), PULSE_SINK="replay_sink")
    screen = pulse = None
    try:
        screen = await asyncio.create_subprocess_exec(
            "Xvfb", ":99", "-screen", "0", f"{WIDTH}x{HEIGHT}x24", "-nolisten", "tcp", "-ac",
            stdout=asyncio.subprocess.DEVNULL, env=env)
        pulse = await asyncio.create_subprocess_exec(
            "pulseaudio", "-n", "--daemonize=no", "--exit-idle-time=-1", "--log-target=stderr",
            "--load=module-null-sink sink_name=replay_sink rate=48000 channels=2",
            f"--load=module-native-protocol-unix socket={pulse_socket} auth-anonymous=1",
            stdout=asyncio.subprocess.DEVNULL, env=env)

        async def screen_ready():
            return Path("/tmp/.X11-unix/X99").exists()

        async def pulse_ready():
            return pulse_socket.exists()

        await wait_ready(screen, screen_ready, "Layar perekam")
        await wait_ready(pulse, pulse_ready, "Audio perekam")
        yield env
    finally:
        await stop_process(pulse)
        await stop_process(screen)


class PublicRequests:
    """Halaman replay hanya boleh meminta alamat internet publik."""

    def __init__(self):
        self.cache = {}

    async def allowed(self, url):
        parsed = urlsplit(url)
        if parsed.scheme in {"data", "blob", "about"}:
            return True
        host = parsed.hostname or ""
        if parsed.scheme not in {"http", "https"} or not host:
            return False
        if host not in self.cache:
            try:
                addresses = await asyncio.wait_for(asyncio.to_thread(
                    socket.getaddrinfo, host, parsed.port or 443, type=socket.SOCK_STREAM), 8)
                self.cache[host] = bool(addresses) and all(
                    ipaddress.ip_address(item[4][0]).is_global for item in addresses)
            except (OSError, ValueError, asyncio.TimeoutError):
                self.cache[host] = False
        return self.cache[host]

    async def route(self, route):
        if await self.allowed(route.request.url):
            await route.continue_()
        else:
            await route.abort()


def capture_screen_png():
    """Ambil piksel Xvfb langsung tanpa menunggu screenshot browser."""
    with ImageGrab.grab(xdisplay=":99") as image:
        if image.size != (WIDTH, HEIGHT):
            raise ReplayError("Ukuran layar perekam belum sesuai dengan video portrait.")
        output = io.BytesIO()
        image.save(output, format="PNG", compress_level=1)
        return output.getvalue()


def popup_ocr_image(picture):
    with Image.open(io.BytesIO(picture)) as image:
        left, top = int(WIDTH * .05), int(HEIGHT * .20)
        crop = image.convert("RGB").crop((left, top, int(WIDTH * .95), int(HEIGHT * .72)))
        red, green, blue = crop.split()
        white = ImageChops.darker(ImageChops.darker(red, green), blue)
        mask = white.point(lambda value: 0 if value >= 160 else 255)
        mask = mask.resize((mask.width * 2, mask.height * 2), Image.Resampling.LANCZOS)
        output = io.BytesIO()
        mask.save(output, format="PNG", compress_level=1)
        return output.getvalue(), left, top


async def read_popup_text(picture):
    processed, left, top = await asyncio.to_thread(popup_ocr_image, picture)
    output, _ = await command("tesseract", "stdin", "stdout", "-l", "eng", "--psm", "6", "tsv", input=processed)
    words, position, rank = [], None, 0
    for line in output.decode("utf-8", errors="replace").splitlines()[1:]:
        row = line.split("\t", 11)
        if len(row) != 12 or not row[11].strip():
            continue
        words.append(row[11])
        word = re.sub(r"[^A-Z]", "", row[11].upper())
        priority = {"MELANJUTKAN": 3, "CONTINUE": 3, "UNTUK": 2, "ANYWHERE": 2,
                    "TEKAN": 1, "KLIK": 1, "KETUK": 1, "PRESS": 1, "CLICK": 1, "TAP": 1}.get(word, 0)
        if priority and priority >= rank:
            try:
                x, y, width, height = map(int, row[6:10])
                position = (left + (x + width / 2) / 2, top + (y + height / 2) / 2)
                rank = priority
            except ValueError:
                pass
    return " ".join(words), position


async def inspect_popup(page, picture, base_text):
    state = getattr(page, "_replay_popup_state", None)
    if state is None:
        state = dict(scan=-100.0, click=-100.0, hits=0, end_seen=False)
        page._replay_popup_state = state
    now = time.monotonic()
    if now - state["scan"] < 6 and not state["end_seen"]:
        return False
    state["scan"] = now
    extra, position = await read_popup_text(picture)
    text = base_text + "\n" + extra
    if ending_text(text):
        state["end_seen"] = True
        return True
    state["end_seen"] = False
    if not continuation_text(text):
        state["hits"] = 0
        return False
    state["hits"] += 1
    if state["hits"] >= 2 and now - state["click"] >= 10:
        if position is None:
            LOG.info("Instruksi lanjut terbaca, tetapi posisi teks belum pasti; klik dilewati.")
            return False
        await page.mouse.click(*position)
        state["hits"], state["click"] = 0, time.monotonic()
        LOG.info("Popup replay dilanjutkan: instruksi eksplisit terdeteksi pada dua pemeriksaan.")
    return False


async def end_visible(page, *, ocr=True):
    """Periksa DOM dan screenshot: tombol game dapat digambar di dalam canvas."""
    for frame in page.frames:
        try:
            again = frame.get_by_text(re.compile(r"^\s*(?:TONTON\s+LAGI|WATCH\s+AGAIN|PLAY\s+AGAIN)\s*$", re.I)).first
            share = frame.get_by_text(re.compile(r"^\s*(?:SALIN\s+TAUTAN|COPY\s+LINK)\s*$", re.I)).first
            if await again.is_visible() and await share.is_visible():
                return True
        except Exception:
            continue
    if not ocr:
        return False
    screenshot = await asyncio.to_thread(capture_screen_png)
    result, _ = await command("tesseract", "stdin", "stdout", "-l", "eng", "--psm", "11", input=screenshot)
    text = result.decode("utf-8", errors="replace")
    if ending_text(text):
        return True
    return await inspect_popup(page, screenshot, text)


async def unmute(page, env):
    """Coba tombol suara berlabel; profil canvas mengikuti contoh replay pengguna."""
    _, volume = await command("ffmpeg", "-hide_banner", "-f", "pulse", "-i", "replay_sink.monitor",
                              "-t", "1", "-af", "volumedetect", "-f", "null", "-", env=env)
    match = re.search(rb"max_volume:\s*(-?\d+(?:\.\d+)?|-inf)\s*dB", volume)
    if match and match.group(1) != b"-inf" and float(match.group(1)) > -65:
        return
    for frame in page.frames:
        button = frame.get_by_role("button", name=re.compile(
            r"unmute|aktifkan suara|nyalakan suara|sound on", re.I)).first
        if await button.is_visible():
            await button.click(timeout=3000)
            return
    screenshot = await asyncio.to_thread(capture_screen_png)
    with Image.open(io.BytesIO(screenshot)) as image:
        thumb = image.convert("RGB").resize((72, 128))
        rows = []
        for y in range(128):
            colored = sum(max(thumb.getpixel((x, y))) > 35 for x in range(72))
            if colored > 12:
                rows.append(y)
        if rows and rows[-1] - rows[0] >= 50:
            top, bottom = rows[0] / 128, (rows[-1] + 1) / 128
            await page.mouse.click(WIDTH * 0.075, HEIGHT * (top + (bottom - top) * 0.92))


async def finish_capture(process):
    if process.returncode is not None:
        raise ReplayError("Perekam berhenti sebelum replay selesai; periksa ruang disk dan log server.")
    process.stdin.write(b"q\n")
    await process.stdin.drain()
    await asyncio.wait_for(process.wait(), 20)
    if process.returncode:
        raise ReplayError("Perekam gagal menyimpan video dan suara.")


async def capture_progress(process, ready):
    while line := await process.stdout.readline():
        if line.startswith(b"frame="):
            try:
                if int(line.split(b"=", 1)[1]) > 0:
                    ready.set()
            except ValueError:
                pass


async def finalize(raw, output):
    _, volume = await command("ffmpeg", "-hide_banner", "-i", raw, "-vn", "-af", "volumedetect",
                              "-f", "null", "-", timeout=120)
    match = re.search(rb"max_volume:\s*(-?\d+(?:\.\d+)?|-inf)\s*dB", volume)
    if not match or match.group(1) == b"-inf" or float(match.group(1)) < -65:
        raise ReplayError("Suara game belum terdeteksi. Replay mungkin masih mute; hasil belum dikirim.")
    if Path(raw).stat().st_size < MAX_OUTPUT:
        await command("ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", raw,
                      "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", "-movflags", "+faststart", output, timeout=120)
    else:
        duration, _ = await command("ffprobe", "-v", "error", "-show_entries", "format=duration",
                                    "-of", "default=nw=1:nk=1", raw)
        seconds = float(duration.strip())
        bitrate = min(1400000, int((MAX_OUTPUT * 0.90 * 8 / seconds) - 96000))
        if bitrate < 160000:
            raise ReplayError("Hasil terlalu besar untuk dikirim lewat bot ini.")
        await command("ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-i", raw,
                      "-map", "0:v:0", "-map", "0:a:0", "-c:v", "libx264", "-preset", "veryfast",
                      "-threads", "1", "-b:v", str(bitrate), "-maxrate", str(bitrate),
                      "-bufsize", str(bitrate * 2), "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "96k",
                      "-movflags", "+faststart", output, timeout=900)
    if Path(output).stat().st_size > MAX_OUTPUT:
        raise ReplayError("Hasil masih melewati batas kirim 45 MB; video belum dikirim.")


async def record(url, folder, *, max_seconds=900, configure=None, poll_seconds=2):
    raw, output = Path(folder) / "rekaman.mkv", Path(folder) / "replay.mp4"
    capture = progress_reader = None
    async with desktop(folder) as env, async_playwright() as playwright:
        context = await playwright.chromium.launch_persistent_context(
            user_data_dir=str(Path(folder) / "browser"),
            executable_path=shutil.which("chromium"), headless=False, env=env,
            viewport={"width": WIDTH, "height": HEIGHT}, is_mobile=True, has_touch=True,
            service_workers="block",
            user_agent="Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36",
            ignore_default_args=["--mute-audio"],
            args=["--no-sandbox", "--test-type", "--kiosk", "--window-position=0,0", f"--window-size={WIDTH},{HEIGHT}",
                  "--autoplay-policy=no-user-gesture-required", "--disable-dev-shm-usage",
                  "--hide-scrollbars", "--use-gl=angle", "--use-angle=swiftshader"])
        try:
            await context.route("**/*", PublicRequests().route)
            if configure is not None:
                await configure(context)
            page = context.pages[0] if context.pages else await context.new_page()
            page.on("dialog", lambda dialog: dialog.dismiss())
            session = await context.new_cdp_session(page)
            window = await session.send("Browser.getWindowForTarget")
            await session.send("Browser.setWindowBounds", {"windowId": window["windowId"],
                "bounds": {"left": 0, "top": 0, "width": WIDTH, "height": HEIGHT}})
            await session.send("Browser.setWindowBounds", {"windowId": window["windowId"],
                "bounds": {"windowState": "fullscreen"}})
            await page.wait_for_timeout(3500)
            capture = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                "-progress", "pipe:1", "-stats_period", "0.2",
                "-thread_queue_size", str(VIDEO_QUEUE_FRAMES), "-f", "x11grab", "-draw_mouse", "0", "-framerate", str(FPS),
                "-video_size", f"{WIDTH}x{HEIGHT}", "-i", ":99.0+0,0",
                "-thread_queue_size", str(AUDIO_QUEUE_PACKETS), "-f", "pulse", "-i", "replay_sink.monitor",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "24", "-threads", "1",
                "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "128k", "-ar", "48000", str(raw),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, env=env)
            ready = asyncio.Event()
            progress_reader = asyncio.create_task(capture_progress(capture, ready))
            try:
                await asyncio.wait_for(ready.wait(), 20)
            except asyncio.TimeoutError:
                raise ReplayError("Perekam video dan audio belum siap; replay belum dibuka.") from None
            started = time.monotonic()
            response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if response and response.status >= 400:
                raise ReplayError(f"Halaman replay menolak akses server (HTTP {response.status}).")
            await page.wait_for_timeout(5000)
            if not await end_visible(page, ocr=False):
                await unmute(page, env)
            hits = 0
            while time.monotonic() - started < max_seconds:
                if capture.returncode is not None:
                    raise ReplayError("Perekam terhenti; periksa ruang disk dan log server.")
                if shutil.disk_usage(folder).free < 512 * 1024 * 1024:
                    raise ReplayError("Ruang server hampir habis; rekaman dihentikan.")
                hits = hits + 1 if await end_visible(page) else 0
                if hits >= 2:
                    await finish_capture(capture)
                    break
                await asyncio.sleep(poll_seconds)
            else:
                raise ReplayError("Layar akhir belum terdeteksi dalam batas waktu. Rekaman belum dikirim sebagai hasil selesai.")
        finally:
            await stop_process(capture)
            if progress_reader is not None:
                await asyncio.gather(progress_reader, return_exceptions=True)
            await context.close()
    await finalize(raw, output)
    return output


class ReplayBot:
    def __init__(self, max_seconds=900):
        self.max_seconds = max_seconds
        self.pending = deque()
        self.users = set()
        self.worker = self.active = self.current = None
        self.stopping = False

    def authorized(self, update):
        return bool(update.effective_chat and update.effective_chat.type == "private"
                    and update.effective_user and not update.effective_user.is_bot
                    and update.effective_chat.id == update.effective_user.id)

    async def start(self, update, context):
        if self.authorized(update):
            await update.effective_message.reply_text(
                "Halo, le 👋 Kirim satu link replay ppreplaylink.net. "
                "Aku rekam sampai layar TONTON LAGI dan SALIN TAUTAN muncul, lalu kirim MP4 portrait beserta suara. "
                "Semua pengguna boleh mengirim link. Rekaman diproses satu per satu. "
                "Pakai /batal untuk membatalkan permintaanmu sendiri.")

    async def receive(self, update, context):
        if not self.authorized(update) or update.edited_message is not None:
            return
        message = update.effective_message
        try:
            url = replay_url(message.text or "")
        except ReplayError as error:
            await message.reply_text(str(error))
            return
        user = update.effective_user.id
        if user in self.users:
            await message.reply_text("Permintaanmu masih diproses atau mengantre, le. Tunggu selesai atau pakai /batal.")
            return
        if len(self.pending) >= 5:
            await message.reply_text("Antrean sedang penuh, le. Coba lagi setelah ada rekaman yang selesai.")
            return
        self.pending.append((user, message, url))
        self.users.add(user)
        position = len(self.pending)
        if self.worker is None or self.worker.done():
            self.worker = asyncio.create_task(self.process_queue())
        await self.notify(message, f"Permintaan diterima, le. Posisi antrean: {position}. Aku kabari saat mulai merekam.")

    async def process_queue(self):
        try:
            while self.pending:
                self.current = self.pending.popleft()
                user, message, url = self.current
                self.active = asyncio.create_task(self.work(message, url))
                try:
                    await self.active
                except asyncio.CancelledError:
                    if self.stopping:
                        raise
                finally:
                    self.users.discard(user)
                    self.current = self.active = None
        finally:
            if self.stopping:
                self.pending.clear()
                self.users.clear()

    async def work(self, message, url):
        oom_before = memory_oom_kills()
        try:
            await message.reply_text("Siap, le. Replay sedang dibuka dan direkam. Aku tunggu layar akhirnya muncul.")
            with tempfile.TemporaryDirectory(prefix="replay-") as folder:
                output = await record(url, folder, max_seconds=self.max_seconds)
                for attempt in range(3):
                    try:
                        with output.open("rb") as stream:
                            await message.reply_video(InputFile(stream, filename="replay.mp4", read_file_handle=False),
                                caption="Replay berhasil di rekam bro\nbilang makasih ke admin RUDAR18", supports_streaming=True,
                                width=WIDTH, height=HEIGHT, read_timeout=180, write_timeout=180)
                        break
                    except RetryAfter as error:
                        if attempt == 2:
                            raise
                        delay = error.retry_after
                        await asyncio.sleep((delay.total_seconds() if hasattr(delay, "total_seconds") else delay) + 1)
        except asyncio.CancelledError:
            raise
        except NetworkError:
            LOG.warning("Hasil kirim Telegram belum pasti; tidak diulang otomatis.")
            await self.notify(message, "Hasil kirim belum pasti, le. Cek chat ini dulu sebelum mengirim ulang link.")
        except Exception as error:
            LOG.exception("Replay gagal")
            oom_after = memory_oom_kills()
            if oom_before is not None and oom_after is not None and oom_after > oom_before:
                explanation = "Rekaman terhenti karena batas RAM perekam tercapai, le. Admin perlu memeriksa kapasitas server."
            else:
                explanation = str(error) if isinstance(error, ReplayError) else "Replay belum berhasil direkam, le. Periksa log container."
            await self.notify(message, explanation)

    async def notify(self, message, text):
        try:
            await message.reply_text(text)
        except TelegramError:
            pass

    async def look(self, update, context):
        if not self.authorized(update):
            return
        message = update.effective_message
        job = self.current
        if job is None or job[0] != update.effective_user.id:
            await message.reply_text("Pakai /lihat saat replay milikmu sedang direkam, le.")
            return
        try:
            picture = await asyncio.to_thread(capture_screen_png)
            text, _ = await command("tesseract", "stdin", "stdout", "-l", "eng", "--psm", "11", input=picture)
            if self.current is not job:
                await message.reply_text("Permintaanmu sudah selesai atau beralih; coba /lihat saat rekamanmu aktif.")
                return
            await message.reply_photo(InputFile(picture, filename="tampilan-replay.png"),
                                      caption="Tampilan yang sedang dilihat bot untuk replaymu.",
                                      read_timeout=60, write_timeout=60)
            reading = text.decode("utf-8", errors="replace").strip() or "(Belum ada teks yang terbaca.)"
            await message.reply_text("Teks yang terbaca:\n" + reading[:3000], parse_mode=None)
        except Exception:
            LOG.exception("Pemeriksaan layar replay gagal")
            await self.notify(message, "Layar perekam belum bisa diambil, le. Coba /lihat lagi sebentar atau periksa log.")

    async def cancel(self, update, context):
        if self.authorized(update):
            user = update.effective_user.id
            if self.current is not None and self.current[0] == user:
                self.active.cancel()
                await asyncio.gather(self.active, return_exceptions=True)
            else:
                self.pending = deque(job for job in self.pending if job[0] != user)
                self.users.discard(user)
            await update.effective_message.reply_text("Permintaanmu dibatalkan, le.")

    async def stop(self, application=None):
        self.stopping = True
        if self.worker and not self.worker.done():
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    token = os.environ.get("REPLAY_BOT_TOKEN", "").strip()
    maximum = os.environ.get("REPLAY_MAX_SECONDS", "900").strip()
    if not re.fullmatch(r"[0-9]{6,15}:[A-Za-z0-9_-]{25,80}", token):
        raise SystemExit("Isi REPLAY_BOT_TOKEN dengan token bot replay baru.")
    if not maximum.isdigit() or not 30 <= int(maximum) <= 1800:
        raise SystemExit("REPLAY_MAX_SECONDS harus 30 sampai 1800 detik.")
    bot = ReplayBot(int(maximum))
    app = Application.builder().token(token).post_stop(bot.stop).post_shutdown(bot.stop).build()
    app.add_handler(CommandHandler("start", bot.start))
    app.add_handler(CommandHandler("batal", bot.cancel))
    app.add_handler(CommandHandler("lihat", bot.look))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, bot.receive))
    app.run_polling(allowed_updates=["message"], drop_pending_updates=True)


if __name__ == "__main__":
    main()
