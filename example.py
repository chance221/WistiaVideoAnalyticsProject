import requests

# 1. Define configuration parameters

VIDEO_HASHED_ID = "9k4tbcdfg0"  # e.g., 'abc123xyz'

# Correctly versioned API endpoint route
url = "https://api.wistia.com/modern/stats/events"
headers = {
    "Authorization": f"Bearer {API_TOKEN}"
}
params = {
    "media_id": VIDEO_HASHED_ID,
    "per_page": 100  # Pull up to 100 event objects per page
}

try:
    # 2. Make the API Request
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    events = response.json()

    print(events)
    print(f"--- Viewing Sessions for Video: {VIDEO_HASHED_ID} ---")
    
    # 3. Parse and interpret visitor payloads
    for event in events:
        visitor = event.get("visitor", {})
        visitor_key = visitor.get("key")
        email = visitor.get("email")
        percent_watched = event.get("percent_watched")
        
        # Format the user identity based on lead identification
        if email:
            identity = f"Identified Lead: {email}"
        else:
            # Anonymous viewers are tracked via a unique identifier string
            identity = f"Anonymous User Key: {visitor_key}"
            
        print(f"{identity} | Completion: {percent_watched}%")

except requests.exceptions.RequestException as e:
    print(f"API Connection Error: {e}")