import os
import csv
import time
import gc
import threading
import concurrent.futures
import pandas as pd
from flask import Flask, send_file, jsonify
from curl_cffi import requests
from bs4 import BeautifulSoup

app = Flask(__name__)

# --- الإعدادات ---
BASE_URL = "https://result.sd/"
PROBLEMS_FILE = "Only_Problems_2026.csv"
LIVE_FILE = "Live_Updates_2026.csv"
FINAL_OUTPUT = "Final_Merged_Updated_2026.csv"

file_lock = threading.Lock()
thread_lock = threading.Lock()
scraper_thread = None

# متغيرات لمتابعة التقدم وتشخيص الحالة
status_info = {
    "total_initial": 0,
    "already_done": 0,
    "completed_session": 0,
    "running": False,
    "start_time": 0,
    "finished": False,
    "message": "في انتظار بدء التشغيل..."
}

def save_live_row(row):
    with file_lock:
        with open(LIVE_FILE, "a", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow(row)
        status_info["completed_session"] += 1
    return row

# =========================================================
# دالة فحص الرقم
# =========================================================
def recheck_seat(seat):
    seat_str = str(seat)
    session = requests.Session(impersonate="chrome120")
    
    try:
        for attempt in range(3):
            try:
                r_get = session.get(BASE_URL, timeout=10)
                soup_get = BeautifulSoup(r_get.text, 'lxml')
                
                csrf_input = soup_get.find("input", {"name": "csrfmiddlewaretoken"})
                if not csrf_input:
                    time.sleep(1)
                    continue
                
                csrf_token = csrf_input['value']
                payload = {'csrfmiddlewaretoken': csrf_token, 'serial_number': seat_str}
                headers = {"Origin": BASE_URL.rstrip('/'), "Referer": BASE_URL}
                
                r_post = session.post(BASE_URL, data=payload, headers=headers, timeout=15)
                soup_post = BeautifulSoup(r_post.text, 'lxml')
                result_div = soup_post.find("div", class_="result")
                
                if result_div:
                    rows = result_div.find_all("div", class_="result-row")
                    name = status = percentage = ""
                    for row in rows:
                        lbl = row.find("div", class_="result-label")
                        val = row.find("div", class_="result-value")
                        if lbl and val:
                            l_t = lbl.text.strip()
                            v_t = val.text.strip()
                            if "الاسم" in l_t: name = v_t
                            elif "النتيجة" in l_t: status = v_t
                            elif "النسبة" in l_t: percentage = v_t
                    return save_live_row([seat_str, name, status, percentage])
                    
                if soup_post.find("ul", class_="errorlist"):
                    return save_live_row([seat_str, "", "رقم غير صحيح أو خطأ", ""])
                else:
                    return save_live_row([seat_str, "", "غير موجود", ""])
            except Exception:
                time.sleep(1)
                continue
        return save_live_row([seat_str, "", "غير موجود", ""])
    finally:
        session.close()

# =========================================================
# مهمة الخلفية (معالجة بالدفعات لمنع انهيار الذاكرة)
# =========================================================
def run_scraper():
    try:
        gc.collect()
        status_info["running"] = True
        status_info["message"] = "🔍 جاري تحضير البيانات بذكاء لترشيد الذاكرة..."
        print(status_info["message"], flush=True)

        target_problem_file = PROBLEMS_FILE
        if not os.path.exists(target_problem_file):
            csv_files = [f for f in os.listdir('.') if f.lower().endswith('.csv')]
            prob_candidates = [f for f in csv_files if 'problem' in f.lower() or 'only' in f.lower()]
            if prob_candidates:
                target_problem_file = prob_candidates[0]
            else:
                status_info["running"] = False
                status_info["message"] = f"❌ لم يتم العثور على {PROBLEMS_FILE}!"
                return

        seats_to_recheck_set = set()
        for chunk in pd.read_csv(target_problem_file, usecols=[0], chunksize=10000):
            col_name = chunk.columns[0]
            valid_seats = pd.to_numeric(chunk[col_name], errors='coerce').dropna().astype(int)
            seats_to_recheck_set.update(valid_seats)
            del chunk
            gc.collect()

        total_initial = len(seats_to_recheck_set)
        status_info["total_initial"] = total_initial

        done_seats = set()
        if os.path.exists(LIVE_FILE):
            try:
                for chunk in pd.read_csv(LIVE_FILE, usecols=[0], chunksize=10000):
                    live_col = chunk.columns[0]
                    valid_done = pd.to_numeric(chunk[live_col], errors='coerce').dropna().astype(int)
                    done_seats.update(valid_done)
                    del chunk
                    gc.collect()
                
                seats_to_recheck_set.difference_update(done_seats)
                status_info["already_done"] = total_initial - len(seats_to_recheck_set)
                print(f"🔄 تم تخطي {status_info['already_done']:,} رقم منجز مسبقاً.", flush=True)
            except Exception as e:
                print(f"⚠️ خطأ أثناء قراءة ملف التحديث: {e}", flush=True)
        else:
            with open(LIVE_FILE, "w", newline="", encoding="utf-8-sig") as f:
                csv.writer(f).writerow(["Seat", "Name", "Result", "Percentage"])

        seats_to_recheck = list(seats_to_recheck_set)
        del seats_to_recheck_set, done_seats
        gc.collect()

        if not seats_to_recheck:
            status_info["running"] = False
            status_info["finished"] = True
            status_info["message"] = "🎉 جميع الأرقام تم فحصها مسبقاً!"
            return

        status_info["start_time"] = time.time()
        status_info["message"] = f"🚀 السحب يعمل الآن! المتبقي: {len(seats_to_recheck):,} رقم"
        print(status_info["message"], flush=True)

        # 🔥 التعديل الجوهري: معالجة الأرقام في دفعات صغيرة (كل 500 رقم)
        batch_size = 500
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            for i in range(0, len(seats_to_recheck), batch_size):
                batch = seats_to_recheck[i : i + batch_size]
                # استخدام map يجعلنا نتنظر انتهاء الدفعة بدون تخزين 190 ألف كائن بالذاكرة
                list(executor.map(recheck_seat, batch))
                # تنظيف الرام بعد كل دفعة
                gc.collect()

        status_info["message"] = "📦 جاري ترتيب وحفظ الملف النهائي..."
        try:
            df_new = pd.read_csv(LIVE_FILE)
            df_new['Seat'] = pd.to_numeric(df_new['Seat'], errors='coerce')
            df_new = df_new.dropna(subset=['Seat'])
            df_new['Seat'] = df_new['Seat'].astype(int)
            df_final = df_new.drop_duplicates(subset=['Seat'], keep='last').sort_values(by='Seat')
            df_final.to_csv(FINAL_OUTPUT, index=False, encoding='utf-8-sig')
            del df_new, df_final
            gc.collect()
        except Exception as e:
            print(f"خطأ ترتيب نهائي: {e}", flush=True)

        status_info["running"] = False
        status_info["finished"] = True
        status_info["message"] = "🏁 اكتمل الفحص والدمج بالكامل!"

    except Exception as e:
        status_info["running"] = False
        status_info["message"] = f"❌ توقف بسبب خطأ: {str(e)}"
        print(f"Fatal Error: {e}", flush=True)

def ensure_scraper_running():
    global scraper_thread
    with thread_lock:
        if not status_info["finished"]:
            if scraper_thread is None or not scraper_thread.is_alive():
                scraper_thread = threading.Thread(target=run_scraper, daemon=True)
                scraper_thread.start()

@app.before_request
def boot_worker_on_request():
    ensure_scraper_running()

# =========================================================
# واجهة الويب
# =========================================================
@app.route("/")
def index():
    total = status_info["total_initial"]
    already_done = status_info["already_done"]
    session_done = status_info["completed_session"]
    total_done = already_done + session_done
    remaining = max(0, total - total_done)

    pct = (total_done / total * 100) if total > 0 else 0
    elapsed_sec = (time.time() - status_info["start_time"]) if status_info["start_time"] > 0 else 0
    elapsed_min = elapsed_sec / 60
    
    speed = session_done / elapsed_sec if elapsed_sec > 0 else 0
    rem_mins = (remaining / speed) / 60 if speed > 0 else 0

    state_text = "🟢 يعمل الآن..." if status_info["running"] else ("🏁 اكتمل الفحص!" if status_info["finished"] else "⚠️ قيد التحضير...")

    return f"""
    <html dir="rtl" style="font-family: Tahoma, sans-serif; text-align: center; padding: 20px; background: #f4f6f9;">
        <body>
            <h2>{state_text}</h2>
            <div style="background: white; padding: 20px; border-radius: 10px; max-width: 540px; margin: auto; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <p style="background: #eef5ff; padding: 10px; border-radius: 6px; color: #004085;">
                    <b>حالة النظام:</b> {status_info["message"]}
                </p>
                <h3 style="color: #2c3e50;">المنجز: {total_done:,} / {total:,} ({pct:.2f}%)</h3>
                <div style="background: #e9ecef; border-radius: 8px; height: 22px; margin: 15px 0;">
                    <div style="background: #28a745; width: {pct:.2f}%; height: 100%;"></div>
                </div>
                <p style="font-size: 14px; color: #555;">
                    📂 منجز سابقاً: <b>{already_done:,}</b> | ⚡ الجلسة: <b>{session_done:,}</b> | 🎯 المتبقي: <b>{remaining:,}</b>
                </p>
                <hr style="border: 0; border-top: 1px solid #eee;">
                <p>🚀 السرعة: <b>{speed:.1f}</b> رقم/ث</p>
                <p>⏱️ الوقت المنقضي: <b>{elapsed_min:.1f}</b> د | ⏳ المتبقي التقريبي: <b>{rem_mins:.1f}</b> د</p>
                <hr style="border: 0; border-top: 1px solid #eee;">
                <p>
                    <a href="/download_live" style="background: #007bff; color: white; padding: 12px 20px; text-decoration: none; border-radius: 6px; display: inline-block; margin: 5px;">📥 تنزيل ملف التحديث الحي</a>
                </p>
            </div>
            <script>setTimeout(() => location.reload(), 5000);</script>
        </body>
    </html>
    """

@app.route("/ping")
def ping():
    return jsonify({
        "status": "alive",
        "running": status_info["running"],
        "message": status_info["message"],
        "total_completed": status_info["already_done"] + status_info["completed_session"]
    })

@app.route("/download_live")
def download_live():
    if os.path.exists(LIVE_FILE):
        return send_file(LIVE_FILE, as_attachment=True)
    return "لم يتم إنشاء الملف بعد.", 404

if __name__ == "__main__":
    ensure_scraper_running()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
