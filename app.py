import os, time, re, random
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

LOCATIONS = [
    "Mumbai","Delhi","Bangalore","Hyderabad","Chennai","Pune","Kolkata",
    "Ahmedabad","Jaipur","Surat","Noida","Gurgaon","Indore","Bhopal",
    "USA","UK","Canada","Australia","Dubai","Singapore","London","New York"
]

def generate_queries(target_client, city="", count=8):
    tc = target_client.strip()
    base = [city, city, city] + random.sample(LOCATIONS, 5) if city else random.sample(LOCATIONS, count)
    base = list(dict.fromkeys(base))[:count]
    patterns = [
        '"{tc}" "{loc}" "@gmail.com"', 'intitle:"{tc}" "{loc}" "contact" "@gmail.com"',
        '"{tc}" "{loc}" "email" "gmail"', 'intitle:"{tc}" "{loc}" "gmail.com"',
        '"{tc} owner" "{loc}" "@gmail.com"', '"{tc}" "{loc}" "reach me" "gmail"',
    ]
    return [p.format(tc=tc, loc=loc) for p, loc in zip(patterns, base)]

def run_hunter():
    print("🚀 GitHub Hunter Bot Awake! Starting Continuous Hunt...", flush=True)
    
    # 1. Auto-generate tasks from pending campaigns
    camps = supabase.table("campaigns").select("*").eq("status", "pending").execute()
    if camps.data:
        for camp in camps.data:
            tc = camp.get("target_client") or camp.get("occupation", "business owner")
            for q in generate_queries(tc, camp.get("city", ""), 6):
                ex = supabase.table("task_queue").select("id").eq("campaign_id", camp["id"]).eq("query", q).execute()
                if not ex.data:
                    supabase.table("task_queue").insert({"campaign_id": camp["id"], "query": q, "status": "pending"}).execute()
        print("📋 New tasks generated from campaigns!", flush=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = browser.new_page()
        
        # 🔄 THE SMART LOOP: Jab tak kaam hai, Hunter nahi rukega!
        while True:
            # Claim a pending task
            claimed = supabase.rpc("claim_task", {"worker_name": "GitHub_Hunter"}).execute()
            if not claimed.data:
                print("ZZZ: No tasks left. Target met or queue empty. Hunter sleeping.", flush=True)
                break # Loop toot jayega aur bot band ho jayega

            task = claimed.data[0]
            task_id = task["id"]
            camp_id = task["campaign_id"]
            
            # Fetch Campaign Details
            c = supabase.table("campaigns").select("*").eq("id", camp_id).single().execute()
            if not c.data:
                supabase.table("task_queue").update({"status": "failed"}).eq("id", task_id).execute()
                continue
            camp = c.data

            # 🎯 TARGET TRACKING: Get User's Daily Limit
            prof = supabase.table("profiles").select("daily_limit").eq("id", camp["user_id"]).single().execute()
            target_limit = prof.data.get("daily_limit", 5) if prof.data else 5
            
            # Count exactly how many leads are already found for this campaign
            leads_count_res = supabase.table("leads").select("id", count="exact").eq("campaign_id", camp_id).execute()
            current_leads = leads_count_res.count if leads_count_res else 0

            # 🛑 STOP LOGIC: Agar target pehle hi complete ho gaya hai
            if current_leads >= target_limit:
                print(f"🏆 TARGET COMPLETE for Campaign {camp_id[:8]}! ({current_leads}/{target_limit}). Stopping campaign.", flush=True)
                
                # Campaign ko "completed" mark karo
                supabase.table("campaigns").update({"status": "completed"}).eq("id", camp_id).execute()
                
                # Baaki bache saare pending tasks uda do (time bachane ke liye)
                supabase.table("task_queue").update({"status": "completed"}).eq("campaign_id", camp_id).eq("status", "pending").execute()
                
                # Current task ko complete mark karo
                supabase.table("task_queue").update({"status": "completed"}).eq("id", task_id).execute()
                continue # Skip scraping, move to next task

            print(f"🎯 HUNTING: {task['query']} | Target Progress: {current_leads}/{target_limit}", flush=True)
            supabase.table("campaigns").update({"status": "processing"}).eq("id", camp_id).execute()

            try:
                page.goto(f"https://www.google.com/search?q={task['query'].replace(' ', '+')}&num=100", timeout=30000)
                time.sleep(8)
                page.mouse.wheel(0, 3000)
                time.sleep(3)
                
                raw_emails = list(set(re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", page.content())))
                print(f"📧 Found {len(raw_emails)} raw emails. Processing...", flush=True)

                inserted_this_round = 0
                for email in raw_emails:
                    # EXACT TARGET CHECK: Scraping karte time limit check karna
                    if current_leads + inserted_this_round >= target_limit:
                        print(f"🛑 Target of {target_limit} met exactly! Stopping insertion.", flush=True)
                        break # Limit poori hote hi turant ruk jayega
                        
                    try:
                        res = supabase.table("leads").insert({
                            "campaign_id": camp_id, "user_id": camp["user_id"], 
                            "email": email.lower(), "status": "raw"
                        }).execute()
                        if res.data:
                            inserted_this_round += 1
                    except: pass # Ignore duplicates
                    
                supabase.table("task_queue").update({"status": "completed"}).eq("id", task_id).execute()
                
                # Scrape hone ke theek baad wapas target check karna
                if current_leads + inserted_this_round >= target_limit:
                     print(f"🏆 TARGET COMPLETE after this query! Marking campaign as COMPLETED.", flush=True)
                     supabase.table("campaigns").update({"status": "completed"}).eq("id", camp_id).execute()
                     supabase.table("task_queue").update({"status": "completed"}).eq("campaign_id", camp_id).eq("status", "pending").execute()
                     
            except Exception as e:
                print(f"❌ Error: {e}", flush=True)
                supabase.table("task_queue").update({"status": "failed"}).eq("id", task_id).execute()
        
        print("✅ All queues processed. Hunter shutting down.", flush=True)
        browser.close()

if __name__ == "__main__":
    run_hunter()
