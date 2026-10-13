import os
import sys
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time
import re

# 1. إعدادات المتغيرات السرية (من GitHub Secrets)
TURSO_URL = os.getenv("TURSO_URL")
TURSO_TOKEN = os.getenv("TURSO_TOKEN")

if not TURSO_URL or not TURSO_TOKEN:
    print("[FATAL ERROR] TURSO_URL or TURSO_TOKEN environment variables are missing!")
    print("Please configure them in GitHub Secrets.")
    sys.exit(1)

# 2. إعدادات الأداء والقيود السحابية
CONCURRENCY = 30                  # عدد قنوات تيليجرام التي يتم فحصها في نفس اللحظة
BATCH_SIZE = 100                  # حجم الدفعة التي يتم سحبها من Turso
TIMEOUT_SECONDS = 12              # أقصى مهلة لانتظار رد من صفحة تيليجرام
MAX_RUN_TIME = 5.5 * 3600         # 5 ساعات ونصف (لتفادي إغلاق GitHub Actions الإجباري بعد 6 ساعات)

HEADERS_WEB = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

HEADERS_TURSO = {
    "Authorization": f"Bearer {TURSO_TOKEN}",
    "Content-Type": "application/json"
}

def parse_subs(text: str):
    """تحويل النصوص مثل '1.5M' أو '50K' إلى أرقام صحيحة"""
    if not text: return 0
    text = text.strip().replace(",", "").replace(" ", "").lower()
    multipliers = {"k": 1_000, "m": 1_000_000}
    try:
        if text[-1] in multipliers:
            return int(float(text[:-1]) * multipliers[text[-1]])
        return int(float(text))
    except:
        return 0

def build_stmt(sql, params):
    """بناء صيغة الاستعلام المتوافقة مع Turso API"""
    args = []
    for p in params:
        if p is None:
            args.append({"type": "null"})
        elif isinstance(p, int):
            args.append({"type": "integer", "value": str(p)})
        elif isinstance(p, float):
            args.append({"type": "float", "value": p})
        else:
            args.append({"type": "text", "value": str(p)})
    return {"sql": sql, "args": args}

async def execute_turso_pipeline(session, statements, max_retries=5):
    """إرسال حزمة استعلامات إلى Turso مع نظام إعادة المحاولة الذكي عند انقطاع الاتصال"""
    url = f"{TURSO_URL}/v2/pipeline"
    payload = {"requests": [{"type": "execute", "stmt": s} for s in statements]}
    
    for attempt in range(1, max_retries + 1):
        try:
            async with session.post(url, json=payload, headers=HEADERS_TURSO, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 429: # Too Many Requests
                    wait_time = attempt * 4
                    print(f"[!] Turso Rate Limit. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    err_txt = await resp.text()
                    raise Exception(f"HTTP {resp.status}: {err_txt}")
        except Exception as e:
            if attempt == max_retries:
                print(f"[ERROR] Turso Pipeline Failed completely after {max_retries} attempts: {e}")
                return None
            await asyncio.sleep(attempt * 2)

async def fetch_channel(session, semaphore, username):
    """سحب بيانات القناة والبوستات من تيليجرام مع معالجة الأخطاء"""
    url = f"https://t.me/s/{username}"
    async with semaphore:
        try:
            async with session.get(url, headers=HEADERS_WEB, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS)) as resp:
                if resp.status == 404:
                    return {"status": "dead", "username": username}
                if resp.status != 200:
                    return {"status": "error", "username": username}

                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")

                # التأكد من عدم حظر القناة
                err = soup.select_one(".tgme_page_title")
                if err and "Telegram: Contact" in err.get_text():
                    return {"status": "dead", "username": username}

                title_el = soup.select_one(".tgme_channel_info_header_title") or soup.select_one(".tgme_page_title")
                title = title_el.get_text(strip=True) if title_el else username

                desc_el = soup.select_one(".tgme_channel_info_description")
                desc = desc_el.get_text(strip=True) if desc_el else ""

                subs = 0
                for cnt in soup.select(".tgme_channel_info_counter"):
                    if "subscriber" in cnt.get_text(strip=True).lower():
                        val = cnt.select_one(".counter_value")
                        if val: subs = parse_subs(val.get_text(strip=True))
                        break

                posts = []
                for msg in soup.select(".tgme_widget_message_wrap"):
                    m_el = msg.select_one(".tgme_widget_message")
                    if not m_el: continue

                    p_id_raw = m_el.get("data-post", "").split("/")[-1]
                    try: p_id = int(p_id_raw)
                    except: continue

                    t_el = msg.select_one(".tgme_widget_message_text")
                    p_text = t_el.get_text(separator="\n", strip=True) if t_el else ""

                    p_img = ""
                    photo = msg.select_one(".tgme_widget_message_photo_wrap")
                    if photo and photo.get("style"):
                        m = re.search(r"url\('(.+?)'\)", photo["style"])
                        if m: p_img = m.group(1)

                    vid_el = msg.select_one("video")
                    p_vid = vid_el["src"] if vid_el and vid_el.get("src") else ""

                    v_el = msg.select_one(".tgme_widget_message_views")
                    views = v_el.get_text(strip=True) if v_el else ""

                    t_tag = msg.select_one("time")
                    date_str, ts = "", 0
                    if t_tag:
                        date_str = t_tag.get_text(strip=True)
                        dt = t_tag.get("datetime", "")
                        if dt:
                            try: ts = int(time.mktime(time.strptime(dt[:19], "%Y-%m-%dT%H:%M:%S")))
                            except: pass

                    if p_text or p_img or p_vid:
                        posts.append((username, p_id, p_text, p_img, p_vid, views, date_str, ts))

                if len(posts) == 0:
                    return {"status": "dead", "username": username}

                return {
                    "status": "ok",
                    "username": username,
                    "title": title,
                    "desc": desc,
                    "subs": subs,
                    "posts": posts
                }
        # التقاط أي خطأ في الاتصال بتيليجرام (لا يوقف السكربت)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return {"status": "error", "username": username}
        except Exception:
            return {"status": "error", "username": username}

async def run():
    script_start_time = time.time()
    print(f"[START] Robust Cloud Crawler | Concurrency: {CONCURRENCY} | Batch: {BATCH_SIZE}")

    semaphore = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as session:
        total_done = 0

        while True:
            # 1. فحص زمن التشغيل السحابي (Graceful Shutdown)
            run_duration = time.time() - script_start_time
            if run_duration > MAX_RUN_TIME:
                print(f"\n[INFO] Maximum execution time reached ({round(run_duration/3600, 2)} hours).")
                print("[INFO] Exiting gracefully to allow GitHub Actions to schedule the next run.")
                break

            cutoff = int(time.time()) - (24 * 3600) # نعيد فحص القناة بعد 24 ساعة
            
            # 2. سحب دفعة القنوات
            fetch_query = [build_stmt("""
                SELECT username FROM channels 
                WHERE last_checked IS NULL OR last_checked = 0 OR last_checked < ?
                ORDER BY last_checked ASC
                LIMIT ?
            """, [cutoff, BATCH_SIZE])]

            resp = await execute_turso_pipeline(session, fetch_query)
            if not resp:
                print("[!] Failed to fetch batch from Turso. Retrying in 10s...")
                await asyncio.sleep(10)
                continue

            try:
                rows = resp["results"][0]["response"]["result"]["rows"]
            except (KeyError, IndexError):
                rows = []

            if not rows:
                print("\n[FINISH] All channels are fully updated for today!")
                break

            usernames = [r[0]["value"] for r in rows]
            batch_start = time.time()

            # 3. سحب البيانات متزامناً
            tasks = [fetch_channel(session, semaphore, un) for un in usernames]
            results = await asyncio.gather(*tasks)

            # 4. بناء الـ Pipeline لـ Turso
            now = int(time.time())
            turso_batch = []
            ok_count, dead_count, err_count = 0, 0, 0

            for res in results:
                un = res["username"]
                
                if res["status"] == "dead":
                    turso_batch.append(build_stmt("INSERT OR IGNORE INTO archive_channels (username) VALUES (?)", [un]))
                    turso_batch.append(build_stmt("DELETE FROM channels WHERE username = ?", [un]))
                    dead_count += 1
                
                elif res["status"] == "ok":
                    posts = res["posts"]
                    last_ts = posts[-1][7] if posts else 0
                    
                    # تحديث بيانات القناة
                    turso_batch.append(build_stmt("""
                        UPDATE channels 
                        SET title=?, description=?, subscribers=?, posts_count=?, 
                            last_post_timestamp=?, last_checked=?
                        WHERE username=?
                    """, [res["title"], res["desc"], res["subs"], len(posts), last_ts, now, un]))

                    # إدراج البوستات (بدون تكرار)
                    for p in posts:
                        turso_batch.append(build_stmt("""
                            INSERT OR IGNORE INTO posts (username, post_id, text, image, video, views, date, timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, list(p)))
                    ok_count += 1
                
                else:
                    # في حالة الخطأ (Timeout أو حظر IP لثواني)، يتم تأجيل فحصها لليوم التالي لتجنب اللوب اللانهائي
                    turso_batch.append(build_stmt("UPDATE channels SET last_checked=? WHERE username=?", [now - (23 * 3600), un]))
                    err_count += 1

            # 5. التنفيذ النهائي على قاعدة البيانات
            if turso_batch:
                success = await execute_turso_pipeline(session, turso_batch)
                if not success:
                    print(f"[!] Pipeline failed to save data for {len(usernames)} channels. They will be retried automatically.")
                    # لو فشل الرفع تماما بنكمل اللوب عادي، لان last_checked متعدلتش، فبالتالي هيتسحبوا تاني الدفعة الجاية
                    await asyncio.sleep(5)
                    continue

            total_done += len(usernames)
            elapsed = round(time.time() - batch_start, 2)
            rate = round(len(usernames) / max(elapsed, 0.1), 1)

            print(f"[BATCH] Checked: {len(usernames):<3} | Time: {elapsed:<5}s ({rate} ch/s) | OK: {ok_count:<3} | Dead: {dead_count:<3} | Err: {err_count:<3} | Total: {total_done}")

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[INFO] Crawler stopped by user.")
    except Exception as e:
        print(f"\n[FATAL] Unexpected error: {e}")
