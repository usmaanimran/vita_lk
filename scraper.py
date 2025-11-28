import feedparser
import pandas as pd
import datetime
import logging
import os
import pytz
import yfinance as yf
import requests
import difflib
from dateutil import parser
from collections import Counter


try:
    from textblob import TextBlob
    TextBlob("test").sentiment
except (ImportError, LookupError):
    print("⚠️ TextBlob Corpora missing. Downloading now...")
    os.system('python -m textblob.download_corpora')
    from textblob import TextBlob


DATA_FOLDER = "data"
RISK_HISTORY_FILE = os.path.join(DATA_FOLDER, "risk_history.csv")
MARKET_DATA_FILE = os.path.join(DATA_FOLDER, "market_data.csv")
NEWS_LOG_FILE = os.path.join(DATA_FOLDER, "daily_news_scan.csv")

DEMO_MODE = False 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
SL_TIMEZONE = pytz.timezone('Asia/Colombo')


OPENWEATHER_API_KEY = "4a17807b0535b7463f3be3a1e3260951" 


RSS_FEEDS = [
    
    "http://www.adaderana.lk/rss.php",
    "https://www.dailymirror.lk/RSS_Feeds/breaking_news",
    "https://www.newsfirst.lk/feed/",
    "https://economynext.com/feed/",
    "http://www.ft.lk/rss_feed.jsp?feedname=HOME", 
    "https://www.lankabusinessonline.com/feed/",  
    "https://ceylontoday.lk/feed/",                 
    "https://www.themorning.lk/feed/",             
    "http://www.island.lk/feed/",                   
    "https://www.colombopage.com/index.xml",       
    "https://metdept.lk/feed/",                    
    "https://timesonline.lk/feed",                 

    "https://news.google.com/rss/search?q=Sri+Lanka+Economy+when:1d&hl=en-LK&gl=LK&ceid=LK:en",
    "https://news.google.com/rss/search?q=Sri+Lanka+Crisis+when:1d&hl=en-LK&gl=LK&ceid=LK:en"
]


RISK_KEYWORDS = {
    "high": {
        
        "strike": ["transport", "logistics"],
        "hartal": ["social", "political"],
        "curfew": ["social", "political"],
        "protest": ["social", "political"],
        "riot": ["social"],
        "clashes": ["social"],
        "violence": ["social"],
        "tear gas": ["social"],
        "arrest": ["social", "security"],
        "mob attack": ["social", "security"],
        "looting": ["social"],
        "arson": ["social"],
        "explosion": ["security"],
        "bomb": ["security"],
        "shooting": ["security"],
        "hostage": ["security"],
        "road blockade": ["logistics", "political"],
        "state of emergency": ["political", "security"],
        "military deployment": ["political", "security"],
        "extremist attack": ["security"],

        "default": ["economic"],
        "bankruptcy": ["economic"],
        "currency crash": ["economic"],
        "inflation surge": ["economic"],
        "hyperinflation": ["economic"],
        "liquidity crisis": ["economic"],
        "credit crunch": ["economic"],
        "foreign reserves": ["economic"],
        "import ban": ["economic", "logistics"],
        "export ban": ["economic", "logistics"],
        "fuel shortage": ["economic", "energy"],
        "diesel shortage": ["economic", "energy"],
        "petrol queue": ["economic", "energy"],
        "price hike": ["economic"],
        "essential shortage": ["economic"],
        "market crash": ["economic"],
        
        "cement shortage": ["supply_chain", "construction"],
        "wheat flour shortage": ["supply_chain", "food"],
        "medicine shortage": ["supply_chain", "health"],
        "lp gas shortage": ["supply_chain", "energy"],
        "fertilizer shortage": ["supply_chain", "agriculture"],
        "fertilizer ban": ["supply_chain", "agriculture"],
        "rice price": ["supply_chain", "food"],

        "flood": ["environmental", "logistics"],
        "landslide": ["environmental", "logistics"],
        "submerged": ["environmental", "logistics"],
        "inundated": ["environmental", "logistics"],
        "overflow": ["environmental", "logistics"],
        "cyclone": ["environmental"],
        "earthquake": ["environmental"],
        "tsunami": ["environmental"],
        "wildfire": ["environmental"],
        "drought": ["environmental"],
        "heatwave": ["environmental"],
        "storm surge": ["environmental"],
        "gale winds": ["environmental"],
        "building collapse": ["environmental"],

        "power cut": ["economic", "energy"],
        "blackout": ["energy"],
        "grid failure": ["energy"],
        "nationwide outage": ["energy"],
        "transformer explosion": ["energy"],
        "gas shortage": ["energy"],
        "water supply cut": ["infrastructure"],
        "norochcholai": ["energy", "powerplant"], 
        "kerawalapitiya": ["energy", "powerplant"],
        "kelanitissa": ["energy", "powerplant"],
        "unit tripped": ["energy", "powerplant"],
        "tripped": ["energy", "powerplant"],
        "breakdown": ["infrastructure", "energy"],
        "tripped generator": ["energy", "powerplant"],
        "maintenance shutdown": ["infrastructure", "energy"],

        "port shutdown": ["logistics"],
        "airport closure": ["logistics"],
        "ship delay": ["logistics"],
        "container backlog": ["logistics"],
        "bridge collapse": ["logistics"],
        "road collapse": ["logistics"],
        "railway strike": ["logistics"],
        "cargo disruption": ["logistics"],

        "chemical leak": ["industrial"],
        "toxic spill": ["industrial"],
        "radiation": ["industrial"],
        "factory explosion": ["industrial"],
        "hazardous material": ["industrial"]
    },

    "medium": {
      
        "union": ["labor", "political"],
        "trade union action": ["labor", "political"],
        "sick note": ["labor"],
        "work to rule": ["labor"],
        "picketing": ["labor"],
        "audit": ["political"],
        "corruption": ["political"],
        "bribery": ["political"],
        "imf": ["economic", "political"],
        "election": ["political"],
        "parliament": ["political"],
        
        "ftz": ["industrial", "logistics"],
        "free trade zone": ["industrial"],
        "biyagama": ["industrial", "labor"],
        "katunayake": ["industrial", "labor"],
        "koggala": ["industrial", "labor"],

        "recession": ["economic"],
        "layoffs": ["economic"],
        "profit warning": ["economic"],
        "downgrade": ["economic"],
        "rating drop": ["economic"],
        "factory shutdown": ["economic"],
        "pricing pressure": ["economic"],
        "wheat price": ["supply_chain", "food"],
        "fertilizer import halt": ["supply_chain", "agriculture"],
        "medicine import halt": ["supply_chain", "health"],

        "monsoon": ["environmental"],
        "heavy rain": ["environmental"],
        "thunderstorm": ["environmental"],
        "tidal warning": ["environmental"],
        "humidity alert": ["environmental"],

        "transformer fault": ["energy"],
        "voltage drop": ["energy"],
        "water contamination": ["infrastructure"],
        "drainage failure": ["infrastructure"],

        "traffic jam": ["logistics"],
        "highway closure": ["logistics"],
        "runway damage": ["logistics"],
        "ferry delay": ["logistics"],

        "dengue": ["health"],
        "epidemic": ["health"],
        "food poisoning": ["health"],

        "malware": ["cyber"],
        "ransomware": ["cyber"],
        "data breach": ["cyber"],
        "server outage": ["cyber"],
        "ddos": ["cyber"],

        "kelani": ["environmental"],
        "gin": ["environmental"],
        "nilwala": ["environmental"]
    },
    
    "low": {
       "tension rising": ["social"],
       "concerns": ["social"],
       "alert issued": ["environmental"],
       "advisory": ["environmental"],
       "panic": ["social"],
       "uncertainty": ["economic"],
       "public frustration": ["social"],
       "shortage reports": ["economic"],
       "distribution delay": ["logistics"],
       "supplier issue": ["logistics"],
       "inventory low": ["logistics"],
       "temperature anomaly": ["environmental"],
       "poor visibility": ["environmental"],
       "rumor": ["social"],
       "unconfirmed report": ["social"],
       "protests planned": ["social", "political"]
   }
}

IGNORE_KEYWORDS = [
    "cricket", "match", "innings", "wicket", "goal", "premier league",
    "movie", "film", "cinema", "actor", "actress", "singer", "celebrity",
    "concert", "gossip", "fashion", "dance", "music", "song", "musical",
    "gaming", "tournament", "review", "trailer", "esports", "recipe",
    "television", "soap", "drama", "celebrity gossip", "box office"
]


def get_market_data():
    logging.info("Fetching Market Data...")
    
    market_data = {
        "usd_lkr": 300.00,
        "oil_price": 75.00,
        "source": "Hardcoded (Init)"
    }

 
    if os.path.exists(MARKET_DATA_FILE):
        try:
            old_df = pd.read_csv(MARKET_DATA_FILE)
            if not old_df.empty:
                market_data["usd_lkr"] = old_df["usd_lkr"].iloc[-1]
                market_data["oil_price"] = old_df["oil_price"].iloc[-1]
                market_data["source"] = "Fail-Safe (CSV)"
        except Exception:
            pass

   
    try:
        tickers = yf.Tickers("LKR=X BZ=F")
        usd_hist = tickers.tickers["LKR=X"].history(period="1d")
        if not usd_hist.empty:
            market_data["usd_lkr"] = round(usd_hist["Close"].iloc[-1], 2)
            market_data["source"] = "Live (Yahoo)"
        
        oil_hist = tickers.tickers["BZ=F"].history(period="1d")
        if not oil_hist.empty:
            market_data["oil_price"] = round(oil_hist["Close"].iloc[-1], 2)
        
        logging.info(f"Market Data Success: USD {market_data['usd_lkr']} | Source: {market_data['source']}")

    except Exception as e:
        logging.warning(f"Yahoo API Failed: {e}. Using {market_data['source']}")

    pd.DataFrame([market_data]).to_csv(MARKET_DATA_FILE, index=False)
    return market_data


def detect_emerging_threats(all_titles):
    words = []
    for title in all_titles:
        clean = ''.join(e for e in title.lower() if e.isalnum() or e.isspace())
        words.extend(clean.split())
    
    bigrams = zip(words, words[1:])
    counts = Counter(bigrams)
    
    emerging_risk_score = 0
    top_emerging_threat = ""
    
  
    stopwords = [
        
        "the", "in", "of", "to", "for", "a", "and", "on", "at", "with", "from", "by", 
        "today", "yesterday", "after", "before", "during", "read", "more", "click", "here", 
        "watch", "video", "live", "full", "story", "update", "report", "news", "breaking",
        
        "sri", "lanka", "colombo", "daily", "government", "president", "minister", "cabinet",
        "parliament", "state", "national", "island", "country", "people", "public",
        
        "al", "jazeera", "bbc", "cnn", "reuters", "times", "guardian", "economynext", 
        "adaderana", "ft", "morning", "island", "mirror", "first", "watch", "lbo", "ceylon", 
        "sunday", "online",
        
        "auction", "market", "meeting", "talks", "visit", "says", "sells", "extra", 
        "price", "rate", "bond", "treasury", "bill", "yield", "rupee", "cents", 
        "stocks", "shares", "bank", "central", "issue", "debt", "loan", "imf",
        "output", "lost", "gain", "loss", "toll", "rise", "drop", "fall", "high", "low", "death",
        
        "dissanayake", "wickremesinghe", "rajapaksa", "premadeasa", "harini", "amarasuriya",
        "crisis", "economic", "policy", "reform", "budget"
    ]

    for phrase, count in counts.items():
        if count >= 2:
            phrase_str = f"{phrase[0]} {phrase[1]}"
            if phrase[0] in stopwords or phrase[1] in stopwords:
                continue
            
            is_known = False
            for risk_type in RISK_KEYWORDS.values():
                for key in risk_type:
                    if key in phrase_str:
                        is_known = True
            
            if not is_known:
                emerging_risk_score += 15 
                top_emerging_threat = phrase_str
                logging.info(f"🚨 EMERGING THREAT DETECTED: '{phrase_str}' (Count: {count})")
    
    return emerging_risk_score, top_emerging_threat


def calculate_news_risk():
    rss_urls = RSS_FEEDS
    
    total_news_score = 0
    current_scan_headlines = [] 
    all_titles_raw = [] 
    
    logging.info("Scanning Expanded Intelligence Network (12 Sources)...")
    
    
    time_threshold = datetime.datetime.now(SL_TIMEZONE) - datetime.timedelta(hours=6)
    
    for url in rss_urls:
        try:
            feed = feedparser.parse(url)
           
            for entry in feed.entries[:10]:
                
                
                article_time = None
                if hasattr(entry, 'published'):
                    try:
                        article_time = parser.parse(entry.published)
                       
                        if article_time.tzinfo is None:
                            article_time = article_time.replace(tzinfo=pytz.utc)
                        
                        article_time = article_time.astimezone(SL_TIMEZONE)
                    except:
                        pass 
                
           
                if article_time and article_time < time_threshold:
                    continue

                title_raw = entry.title.lower()
                
                if any(k in title_raw for k in IGNORE_KEYWORDS):
                    continue
                
                all_titles_raw.append(title_raw)

                tokens = title_raw.split()
                score = 0
                sector_tag = "General"
                
                high_keys = list(RISK_KEYWORDS["high"].keys())
                med_keys = list(RISK_KEYWORDS["medium"].keys())
                low_keys = list(RISK_KEYWORDS["low"].keys())

                for token in tokens:
                    matches_high = difflib.get_close_matches(token, high_keys, n=1, cutoff=0.85)
                    if matches_high:
                        score = 25
                        matched_word = matches_high[0]
                        sector_tag = RISK_KEYWORDS["high"][matched_word][0].capitalize()
                        break
                    
                    matches_med = difflib.get_close_matches(token, med_keys, n=1, cutoff=0.85)
                    if matches_med:
                        score = 10
                        matched_word = matches_med[0]
                        sector_tag = RISK_KEYWORDS["medium"][matched_word][0].capitalize()
                        
                    matches_low = difflib.get_close_matches(token, low_keys, n=1, cutoff=0.85)
                    if matches_low and score == 0:
                        score = 5
                        matched_word = matches_low[0]
                        sector_tag = RISK_KEYWORDS["low"][matched_word][0].capitalize()
                
                if score == 0:
                    for word, sectors in RISK_KEYWORDS["high"].items():
                        if word in title_raw:
                            score = 25
                            sector_tag = sectors[0].capitalize()

                blob = TextBlob(title_raw)
                if blob.sentiment.polarity < -0.3:
                    score += 5

                total_news_score += score
                
                if score > 0:
                    current_scan_headlines.append({
                        "Headline": entry.title,
                        "Risk": score,
                        "Sector": sector_tag,
                        "Link": entry.link,
                        "Timestamp": datetime.datetime.now(SL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
                    })
                    
        except Exception as e:
            logging.error(f"Feed Error {url}: {e}")


    emerging_score, emerging_topic = detect_emerging_threats(all_titles_raw)
    if emerging_score > 0:
        total_news_score += emerging_score
        
        search_query = emerging_topic.replace(' ', '+')
        smart_link = f"https://www.google.com/search?q={search_query}+Sri+Lanka+News"
        
        current_scan_headlines.insert(0, {
            "Headline": f"⚠️ Emerging Trend: {emerging_topic.upper()}",
            "Risk": emerging_score,
            "Sector": "Uncategorized",
            "Link": smart_link,
            "Timestamp": datetime.datetime.now(SL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        })


    if current_scan_headlines:
        new_df = pd.DataFrame(current_scan_headlines)
        
        if os.path.exists(NEWS_LOG_FILE):
            try:
                old_df = pd.read_csv(NEWS_LOG_FILE)
                combined_df = pd.concat([old_df, new_df])
                
                combined_df.drop_duplicates(subset=["Headline"], keep='last', inplace=True)
             
                combined_df['SortKey'] = combined_df['Headline'].apply(lambda x: 0 if "Emerging Trend" in str(x) else 1)
                combined_df = combined_df.sort_values(by=['SortKey', 'Timestamp'], ascending=[True, False])
                combined_df = combined_df.drop(columns=['SortKey'])
                
                
                if len(combined_df) > 100:
                    combined_df = combined_df.head(100)
                
                combined_df.to_csv(NEWS_LOG_FILE, index=False)
            except Exception:
                new_df.to_csv(NEWS_LOG_FILE, index=False)
        else:
            new_df.to_csv(NEWS_LOG_FILE, index=False)
        
    return min(100, total_news_score), current_scan_headlines


def get_env_social_risk(news_score, eco_risk, headlines):
    env_risk = 0
    weather_source = "Simulation"
    
    if OPENWEATHER_API_KEY:
        try:
            cities = {
                "Colombo": 1248469,    "Kelaniya": 1241940,
                "Kandy": 1241622,      "Galle": 1246294,
                "Jaffna": 1242833,     "Trincomalee": 1226260,
                "Anuradhapura": 1251081,"Ratnapura": 1229621,  
                "Batticaloa": 1250161, "Badulla": 1249931,    
                "Puttalam": 1229699    
            }
            
            max_rain = 0
            for city_name, city_id in cities.items():
                url = f"https://api.openweathermap.org/data/2.5/weather?id={city_id}&appid={OPENWEATHER_API_KEY}"
                r = requests.get(url, timeout=1.0) 
                if r.status_code == 200:
                    data = r.json()
                    rain_1h = data.get('rain', {}).get('1h', 0)
                    if rain_1h > max_rain: max_rain = rain_1h
            
            if max_rain > 10: env_risk = 40
            if max_rain > 50: env_risk = 90
            if max_rain >= 0: weather_source = "Live API (All-Island)"
                
        except Exception:
            logging.warning("Weather API Failed. Switching to Fallback.")

    if env_risk == 0:
        current_month = datetime.datetime.now().month
        is_monsoon = current_month in [5, 6, 9, 10, 11]
        env_risk = 40 if is_monsoon else 10
        weather_source = "Seasonality Logic"
    
    for h in headlines:
        title_l = h["Headline"].lower()
        if "flood" in title_l or "landslide" in title_l or "overflow" in title_l:
            env_risk = 90
            weather_source = "News Verification"
            logging.info("🌊 DETECTED FLOOD NEWS: Escalating Environmental Risk")

    social_risk = int((news_score * 0.5) + (eco_risk * 0.3) + 20)
    
    logging.info(f"Env Risk: {env_risk} (Source: {weather_source}) | Social Risk: {social_risk}")
    
    return env_risk, min(100, social_risk)


def calculate_synergy_risk(news_score, market_data, env_score, social_score):
    eco_risk = 0
    if market_data["usd_lkr"] > 320: eco_risk += 30
    elif market_data["usd_lkr"] > 300: eco_risk += 15
    if market_data["oil_price"] > 90: eco_risk += 15
    
    base_score = max(news_score, eco_risk, env_score)
    
    multiplier = 1.0
    if eco_risk > 20 and news_score > 30:
        multiplier = 1.25 
        logging.info("⚠️ SYSTEMIC SYNERGY DETECTED: Applying 1.25x Risk Multiplier")
    
    final_score = int(base_score * multiplier)
    return min(100, final_score), eco_risk


def analyze_history(current_score):
    momentum = 0
    is_anomaly = False
    
    if os.path.exists(RISK_HISTORY_FILE):
        try:
            df = pd.read_csv(RISK_HISTORY_FILE)
            if not df.empty:
                last_score = df["Total_Risk"].iloc[-1]
                momentum = current_score - last_score
                
                recent_window = df["Total_Risk"].tail(24)
                if len(recent_window) > 1:
                    mean = recent_window.mean()
                    std = recent_window.std()
                    
                    if std > 0.01:
                        z_score = (current_score - mean) / std
                        if z_score > 2.0:
                            is_anomaly = True
                            logging.warning(f"🚨 ANOMALY DETECTED: Risk > 2 Sigma above mean")
        except Exception:
            pass
            
    return momentum, is_anomaly


def run_scraper():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    if DEMO_MODE:
        logging.warning("🚨 DEMO MODE ACTIVE")

    fin_data = get_market_data()
    news_risk, headlines = calculate_news_risk()
    
    temp_eco_risk = 0
    if fin_data["usd_lkr"] > 300: temp_eco_risk += 15
    if fin_data["oil_price"] > 90: temp_eco_risk += 15
    
    env_risk, social_risk = get_env_social_risk(news_risk, temp_eco_risk, headlines)
    final_score, eco_risk = calculate_synergy_risk(news_risk, fin_data, env_risk, social_risk)
    momentum, is_anomaly = analyze_history(final_score)
    
    timestamp = datetime.datetime.now(SL_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    top_story = headlines[0]["Headline"] if headlines else "System Stable"
    
    new_record = {
        "Timestamp": timestamp,
        "Total_Risk": final_score,
        "News_Risk": news_risk,
        "Economic_Risk": eco_risk,
        "Environmental_Risk": env_risk,
        "Social_Risk": social_risk,
        "Top_Headline": top_story,
        "USD": fin_data["usd_lkr"],
        "Momentum": momentum,
        "Anomaly_Flag": is_anomaly
    }
    
    df = pd.DataFrame([new_record])
    
    if os.path.exists(RISK_HISTORY_FILE):
        df.to_csv(RISK_HISTORY_FILE, mode='a', header=False, index=False)
    else:
        df.to_csv(RISK_HISTORY_FILE, mode='w', header=True, index=False)

    logging.info(f"✅ RUN COMPLETE. Risk: {final_score}. Saved to {RISK_HISTORY_FILE}")

if __name__ == "__main__":
    run_scraper()
