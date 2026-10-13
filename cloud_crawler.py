import asyncio
import os
import re
import sys
import time
from bs4 import BeautifulSoup
import aiohttp

# 1. إعدادات الاتصال بقاعدة بيانات Turso
TURSO_URL = os.environ.get("TURSO_URL", "").rstrip("/")
TURSO_TOKEN = os.environ.get("TURSO_TOKEN", "")

if not TURSO_URL or not TURSO_TOKEN:
    print("[ERROR] TURSO_URL or TURSO_TOKEN environment variables are missing!")
    sys.exit(1)

PIPELINE_URL = f"{TURSO_URL}/v2/pipeline"

# 2. إعدادات الأداء القصوى لـ GitHub Actions
CONCURRENCY = 120            # معالجة 120 قناة متزامنة في نفس اللحظة
BATCH_SIZE = 250             # فحص 250 قناة في كل دفعة
TIMEOUT_SECONDS = 6          # 6 ثوانٍ كحد أقصى لتخطي القنوات البطيئة فوراً
MAX_RUN_TIME = 5.3 * 3600    # التوقف الآمن بعد 5.3 ساعات قبل إغلاق الرانر (6 ساعات)
RECHECK_INTERVAL = 86400     # 24 ساعة كحد أدنى لإعادة فحص القناة

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
}


def build_arg(val):
    if val is None:
        return {"type": "null"}
    if isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    if isinstance(val, float):
        return {"type": "float", "value": val}
    return {"type": "text", "value": str(val)}


async def execute_turso_pipeline(session: aiohttp.ClientSession, statements: list) -> dict:
    requests_payload = [
        {"type": "execute", "stmt": {"sql": s["sql"], "args": [build_arg(a) for a in s.get("args", [])]}}
        for s in statements
    ]
    payload = {"requests": requests_payload}

    headers = {
        "Authorization": f"Bearer {TURSO_TOKEN}",
        "Content-Type": "application/json",
    }

    for attempt in range(1, 4):
        try:
            async with session.post(PIPELINE_URL, json=payload, headers=headers, timeout=30) as resp:
                if resp.status == 200:
                    return await resp.json()
                error_text = await resp.text()
                print(f"[TURSO WARNING] Attempt {attempt} failed ({resp.status}): {error_text[:200]}")
        except Exception as e:
            print(f"[TURSO ERROR] Attempt {attempt} network exception: {e}")

        await asyncio.sleep(attempt * 2)

    return {}


def parse_subscribers(sub_text: str) -> int:
    if not sub_text:
        return 0
    clean = sub_text.replace(" ", "").replace("subscribers", "").replace("members", "").strip()
    match = re.search(r"([\d\.]+)\s*([KkMm]?)", clean)
    if not match:
        return 0

    number = float(match.group(1))
    unit = match.group(2).upper()
    if unit == "K":
        return int(number * 1000)
    elif unit == "M":
        return int(number * 1000000)
    return int(number)


def parse_views(view_text: str) -> str:
    if not view_text:
        return "0"
    return view_text.strip()


async def scrape_channel(session: aiohttp.ClientSession, username: str, semaphore: asyncio.Semaphore) -> dict:
    url = f"https://t.me/s/{username}"
    result = {
        "username": username,
        "status": "error",
        "title": "",
        "description": "",
        "subscribers": 0,
        "posts": [],
        "posts_count": 0,
        "last_post_date": ""
    }

    async with semaphore:
        try:
            async with session.get(url, headers=HEADERS, timeout=TIMEOUT_SECONDS, allow_redirects=True) as resp:
                if resp.status == 404:
                    result["status"] = "dead"
                    return result
                if resp.status != 200:
                    result["status"] = "error"
                    return result

                html = await resp.text()

        except (asyncio.TimeoutError, aiohttp.ClientError):
            result["status"] = "error"
            return result
        except Exception:
            result["status"] = "error"
            return result

    try:
        # فحص وجود القناة وحالتها
        if "tgme_page_icon" not in html and "tgme_channel_info" not in html:
            result["status"] = "dead"
            return result

        soup = BeautifulSoup(html, "html.parser")

        # فحص القنوات المحذوفة أو الخاصة
        action_wrap = soup.find("div", class_="tgme_page_action")
        if action_wrap and "You can view and join" not in html and not soup.find("div", class_="tgme_widget_message"):
            if "If you have Telegram, you can view and join" not in html:
                result["status"] = "dead"
                return result

        # جلب تفاصيل القناة
        title_el = soup.find("div", class_="tgme_channel_info_title")
        if title_el:
            title_text = title_el.find("span")
            result["title"] = title_text.get_text(strip=True) if title_text else title_el.get_text(strip=True)

        desc_el = soup.find("div", class_="tgme_channel_info_description")
        if desc_el:
            result["description"] = desc_el.get_text(separator="\n", strip=True)

        sub_el = soup.find("div", class_="tgme_channel_info_counter")
        if sub_el:
            result["subscribers"] = parse_subscribers(sub_el.get_text(strip=True))

        # استخراج المنشورات
        post_elements = soup.find_all("div", class_="tgme_widget_message_wrap")
        posts = []

        for p_wrap in post_elements:
            p_node = p_wrap.find("div", class_="tgme_widget_message")
            if not p_node:
                continue

            post_data = p_node.get("data-post", "")
            if not post_data or "/" not in post_data:
                continue

            try:
                post_id = int(post_data.split("/")[-1])
            except ValueError:
                continue

            # نص المنشور
            text_el = p_node.find("div", class_="tgme_widget_message_text")
            text = text_el.get_text(separator="\n", strip=True) if text_el else ""

            # الصورة أو الفيديو
            image_url = ""
            video_url = ""

            photo_el = p_node.find("a", class_="tgme_widget_message_photo_wrap")
            if photo_el and "background-image:url('" in photo_el.get("style", ""):
                m = re.search(r"background-image:url\('([^']+)'\)", photo_el.get("style", ""))
                if m:
                    image_url = m.group(1)

            video_el = p_node.find("video", class_="tgme_widget_message_video")
            if video_el:
                video_url = video_el.get("src", "")

            # المشاهدات والتاريخ
            views_el = p_node.find("span", class_="tgme_widget_message_views")
            views = parse_views(views_el.get_text(strip=True) if views_el else "0")

            time_el = p_node.find("time", class_="time")
            post_date = time_el.get("datetime", "") if time_el else ""

            posts.append({
                "username": username,
                "post_id": post_id,
                "text": text,
                "image": image_url,
                "video": video_url,
                "views": views,
                "date": post_date
            })

        result["posts"] = posts
        result["posts_count"] = len(posts)
        if posts:
            result["last_post_date"] = posts[-1]["date"]

        result["status"] = "ok"

    except Exception:
        result["status"] = "error"

    return result


async def main():
    print(f"[START] High-Performance Cloud Crawler (Concurrency: {CONCURRENCY}, Batch: {BATCH_SIZE})...")

    start_global_time = time.time()
    total_processed = 0

    # إعداد الـ Connector بدون التحقق من SSL وبكاش DNS لتسريع فتح الاتصالات المتوازية
    connector = aiohttp.TCPConnector(
        limit=CONCURRENCY,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        ssl=False
    )

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            # التحقق من سقف الوقت الآمن
            elapsed = time.time() - start_global_time
            if elapsed >= MAX_RUN_TIME:
                print(f"[TIMEOUT] Approaching GitHub Runner limit ({elapsed/3600:.2f} hrs). Shutting down gracefully.")
                break

            now_ts = int(time.time())
            cutoff_ts = now_ts - RECHECK_INTERVAL

            # 1. جلب دفعة جديدة من القنوات
            fetch_stmt = {
                "sql": (
                    "SELECT username FROM channels "
                    "WHERE last_checked IS NULL OR last_checked < ? "
                    "ORDER BY last_checked ASC NULLS FIRST "
                    "LIMIT ?;"
                ),
                "args": [cutoff_ts, BATCH_SIZE]
            }

            res = await execute_turso_pipeline(session, [fetch_stmt])
            if not res or "results" not in res:
                print("[ERROR] Failed to fetch channels batch. Sleeping 10s...")
                await asyncio.sleep(10)
                continue

            rows = res["results"][0]["response"]["result"]["rows"]
            if not rows:
                print("[FINISH] All channels are fully updated for today!")
                break

            channel_usernames = [r[0]["value"] for r in rows]
            batch_start_time = time.time()

            # 2. فحص القنوات بالتوازي الفائق
            tasks = [scrape_channel(session, u, semaphore) for u in channel_usernames]
            results = await asyncio.gather(*tasks)

            # 3. إعداد استعلامات الحفظ للأنابيب (Pipeline)
            pipeline_statements = []
            stats = {"ok": 0, "dead": 0, "err": 0}

            for r in results:
                u = r["username"]
                st = r["status"]

                if st == "ok":
                    stats["ok"] += 1
                    # تحديث بيانات القناة
                    pipeline_statements.append({
                        "sql": (
                            "UPDATE channels SET title = ?, description = ?, subscribers = ?, "
                            "posts_count = ?, last_post_date = ?, last_checked = ? "
                            "WHERE username = ?;"
                        ),
                        "args": [r["title"], r["description"], r["subscribers"], r["posts_count"], r["last_post_date"], now_ts, u]
                    })

                    # حفظ البوستات
                    for p in r["posts"]:
                        pipeline_statements.append({
                            "sql": (
                                "INSERT OR IGNORE INTO posts (username, post_id, text, image, video, views, date) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?);"
                            ),
                            "args": [p["username"], p["post_id"], p["text"], p["image"], p["video"], p["views"], p["date"]]
                        })

                elif st == "dead":
                    stats["dead"] += 1
                    # إضافة للأرشيف ثم الحذف من القنوات
                    pipeline_statements.append({
                        "sql": "INSERT OR IGNORE INTO archive_channels (username) VALUES (?);",
                        "args": [u]
                    })
                    pipeline_statements.append({
                        "sql": "DELETE FROM channels WHERE username = ?;",
                        "args": [u]
                    })

                else:
                    stats["err"] += 1
                    # تأجيل الفحص لمدة يوم حتى لا تعلق الحلقة
                    pipeline_statements.append({
                        "sql": "UPDATE channels SET last_checked = ? WHERE username = ?;",
                        "args": [now_ts - 86400, u]
                    })

            # 4. إرسال كل العمليات لـ Turso دفعة واحدة
            if pipeline_statements:
                await execute_turso_pipeline(session, pipeline_statements)

            batch_duration = time.time() - batch_start_time
            rate = len(channel_usernames) / batch_duration if batch_duration > 0 else 0
            total_processed += len(channel_usernames)

            print(
                f"[BATCH] Checked: {len(channel_usernames):3d} | "
                f"Time: {batch_duration:5.2f}s ({rate:4.1f} ch/s) | "
                f"OK: {stats['ok']:3d} | Dead: {stats['dead']:2d} | Err: {stats['err']:2d} | "
                f"Total Processed: {total_processed}"
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOPPED] Execution manually interrupted.")
