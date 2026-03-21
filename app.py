import os, time, re, random
from playwright.sync_api import sync_playwright
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def hunt_leads():
    print("🚀 Hunter Bot Awake! Searching for tasks...")
    # Claim a task
    claimed = supabase.rpc("claim_task", {"worker_name": "GitHub_Hunter"}).execute()
    
    if not claimed.data:
        print("😴 No tasks right now. Sleeping...")
        return

    task = claimed.data[0]
    camp_id = task["campaign_id"]
    query = task["query"]
    print(f"🎯 HUNTING: {query}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = browser.new_page()
        try:
            page.goto(f"https://www.google.com/search?q={query.replace(' ', '+')}&num=100", timeout=30000)
            time.sleep(5)
            page.mouse.wheel(0, 3000)
            time.sleep(2)
            
            raw_emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", page.content())
            emails = list(set(raw_emails))
            print(f"📧 Found {len(emails)} raw emails. Sending to Database...")

            for email in emails:
                try:
                    # Push raw email to Supabase for the Validator to check
                    supabase.table("leads").insert({
                        "campaign_id": camp_id,
                        "email": email.lower(),
                        "status": "raw" # Validator isko pick karega
                    }).execute()
                except: pass
            
            # Task Done
            supabase.table("task_queue").update({"status": "completed"}).eq("id", task["id"]).execute()
            print("✅ Hunter Job Done. Exiting...")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            supabase.table("task_queue").update({"status": "failed"}).eq("id", task["id"]).execute()
        finally:
            browser.close()

if __name__ == "__main__":
    hunt_leads()
