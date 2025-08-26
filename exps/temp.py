# -*- coding: utf-8 -*-
import requests
import json
import math
import asyncio
from typing import List, Dict, Any, Union
from flask import Flask, render_template_string, request, jsonify
import sys

# --- Flask App Initialization ---
app = Flask(__name__)

# --- User Inputs ---
# Please enter your Google Places API Key.
# It can be obtained by enabling the Places API and Geocoding API in your Google Cloud account.
API_KEY_PLACES = "ddddY"  # <-- REPLACE with your actual key here

# This is provided by the canvas environment and should be left empty.
API_KEY_GEMINI = "ddddd-SdotpUnEIYM28"

# --- Request Parameters ---
RADIUS = 5000  # 5 km in meters
SEARCH_QUERY = "restaurant"
API_URL_GEMINI = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent"
LATITUDE = 50.4501
LONGITUDE = 30.5234

# --- Helper Functions ---
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculates the distance between two points on the Earth's surface using the haversine formula.
    Returns the distance in kilometers.
    """
    R = 6371.0  # Earth's radius in km
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return distance

def get_photo_url(photo_reference: str, api_key: str, max_width: int = 400) -> str:
    """
    Constructs a URL to get a photo from Google Places API using a photo reference.
    """
    return (f"https://maps.googleapis.com/maps/api/place/photo?"
            f"maxwidth={max_width}&photo_reference={photo_reference}&key={api_key}")

# --- LLM Analysis via Gemini API ---
async def analyze_place_with_llm(text_description: str) -> Dict[str, str]:
    """
    Uses the Gemini API to analyze a text description and return structured data.
    If a specific style or suitability cannot be determined, it generates a unique placeholder.
    """
    
    # Updated system prompt to avoid generic phrases like "Not specified" or "Suitable for all"
    system_prompt = """
    You are an experienced text analyst.
    Your task is to analyze a place's description and determine its "style/vibe" and "audience suitability".
    Return the result as a single JSON object.
    'style_vibe' should be a descriptive phrase (e.g., 'cozy, intimate'), and 'audience_suitability' should be a simple category (e.g., 'family-friendly', 'date night', 'adults only').
    If the description is not detailed enough to determine a specific vibe or audience, create a unique and polite general phrase based on the context, avoiding generic phrases like 'Not specified', 'Suitable for all', or 'Welcomes everyone'."""
    
    user_query = f"Analyze the following description: {text_description}"
    
    payload = {
        "contents": [{"parts": [{"text": user_query}]}],
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {
                    "style_vibe": {"type": "STRING"},
                    "audience_suitability": {"type": "STRING"}
                }
            }
        }
    }
    
    params = {}
    if API_KEY_GEMINI:
        params['key'] = API_KEY_GEMINI
    
    try:
        response = requests.post(API_URL_GEMINI, params=params, json=payload)
        response.raise_for_status()
        
        response_json = response.json()
        if 'candidates' in response_json and len(response_json['candidates']) > 0:
            llm_response_text = response_json['candidates'][0]['content']['parts'][0]['text']
            llm_response_json = json.loads(llm_response_text)
            
            vibe = llm_response_json.get("style_vibe", "A pleasant spot for a meal")
            suitability = llm_response_json.get("audience_suitability", "Good for most guests")
            
            # Final fallback check in case Gemini returns a bad value
            if vibe is None or vibe == "Not defined" or vibe == "Not specified":
                vibe = "A pleasant spot for a meal"
            if suitability is None or suitability in ["Not defined", "Suitable for all", "Welcomes everyone", "Not specified"]:
                suitability = "Good for most guests"
                
            return {"vibe": vibe, "suitability": suitability}
        else:
            return {"vibe": "A pleasant spot for a meal", "suitability": "Good for most guests"}
            
    except requests.exceptions.RequestException as e:
        print(f"Error requesting Gemini API: {e}", file=sys.stderr)
        return {"vibe": "A pleasant spot for a meal", "suitability": "Good for most guests"}
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing Gemini JSON response: {e}", file=sys.stderr)
        return {"vibe": "A pleasant spot for a meal", "suitability": "Good for most guests"}

# --- Main Request Logic ---
async def get_restaurants_data(api_key: str, lat: float, lng: float) -> List[Dict[str, Any]]:
    """
    Finds nearby restaurants and returns their details in JSON format.
    """
    if not api_key or api_key == "YOUR_GOOGLE_PLACES_API_KEY":
        return []

    search_url = (f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?"
                  f"location={lat},{lng}&radius={RADIUS}&type={SEARCH_QUERY}&key={api_key}")
    
    try:
        response = requests.get(search_url)
        response.raise_for_status()
        search_data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"API Error with Google Places: {e}", file=sys.stderr)
        return []
    
    if search_data.get("status") != "OK":
        print(f"API Error from Google Places: {search_data.get('error_message', search_data.get('status'))}", file=sys.stderr)
        return []
    
    restaurants = []
    
    for place in search_data["results"]:
        place_id = place["place_id"]
        
        details_url = (f"https://maps.googleapis.com/maps/api/place/details/json?"
                       f"place_id={place_id}&fields=name,types,geometry,opening_hours,editorial_summary,photos&key={api_key}")
        
        try:
            details_response = requests.get(details_url)
            details_response.raise_for_status()
            details_data = details_response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Error retrieving place details for {place.get('name', 'Unknown')}: {e}", file=sys.stderr)
            continue
        
        if details_data.get("status") == "OK":
            result = details_data["result"]
            
            place_lat = result["geometry"]["location"]["lat"]
            place_lng = result["geometry"]["location"]["lng"]
            distance = haversine(LATITUDE, LONGITUDE, place_lat, place_lng)
            
            is_open = result.get("opening_hours", {}).get("open_now", None)
            time_availability = "Open now" if is_open else "Closed" if is_open is False else "Unknown"
            
            description = result.get("editorial_summary", {}).get("overview", "")
            
            llm_analysis = await analyze_place_with_llm(description)
            
            photo_url = None
            if 'photos' in result and result['photos']:
                photo_reference = result['photos'][0]['photo_reference']
                photo_url = get_photo_url(photo_reference, API_KEY_PLACES)

            restaurant_data = {
                "name": result.get("name"),
                "photo_url": photo_url,
                "query": {
                    "category": result.get("types", []),
                    "style_vibe": llm_analysis["vibe"],
                    "audience_suitability": llm_analysis["suitability"],
                    "distance": f"{distance:.2f} km",
                    "time_availability": time_availability
                }
            }
            restaurants.append(restaurant_data)
        else:
            print(f"Details API Error for {place.get('name', 'Unknown')}: {details_data.get('error_message', details_data.get('status'))}", file=sys.stderr)

    return restaurants

# --- Flask Routes ---
@app.route('/')
def index():
    """
    Serves the main HTML page with a loading indicator.
    """
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Nearby Restaurants in Kyiv</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <style>
            body { font-family: 'Inter', sans-serif; }
            .loader {
                border-top-color: #3498db;
                -webkit-animation: spinner 1.5s linear infinite;
                animation: spinner 1.5s linear infinite;
            }
            @-webkit-keyframes spinner {
                0% { -webkit-transform: rotate(0deg); }
                100% { -webkit-transform: rotate(360deg); }
            }
            @keyframes spinner {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
        </style>
    </head>
    <body class="bg-gray-100 p-8 flex flex-col items-center min-h-screen">
        <div id="loading-container" class="text-center mt-20">
            <div class="loader ease-linear rounded-full border-4 border-t-4 border-gray-200 h-12 w-12 mb-4"></div>
            <p class="text-gray-600">Loading results...</p>
        </div>
        <div id="content" class="container mx-auto"></div>

        <script>
            async function fetchAndRenderResults() {
                try {
                    const response = await fetch('/data');
                    if (!response.ok) {
                        throw new Error(`HTTP error! status: ${response.status}`);
                    }
                    const restaurants = await response.json();
                    
                    const container = document.getElementById('content');
                    container.innerHTML = ''; // Clear loading indicator
                    
                    if (restaurants.length === 0) {
                        container.innerHTML = '<p class="text-red-500 text-center">Could not find any results or an API error occurred. Please ensure your API key is correct and necessary APIs are enabled in your Google Cloud Console.</p>';
                        return;
                    }

                    let html = '<h1 class="text-4xl font-bold text-center mb-8 text-gray-800">Restaurants Near Kyiv</h1>';
                    html += '<div class="grid gap-6 md:grid-cols-2 lg:grid-cols-3">';
                    
                    restaurants.forEach(restaurant => {
                        html += `
                        <div class="bg-white rounded-lg shadow-lg overflow-hidden transform transition-transform duration-300 hover:scale-105">
                            ${restaurant.photo_url ? `<img src="${restaurant.photo_url}" onerror="this.onerror=null;this.src='https://placehold.co/400x200/cccccc/333333?text=Photo+Not+Available';" alt="${restaurant.name}" class="w-full h-48 object-cover">` : `<img src="https://placehold.co/400x200/cccccc/333333?text=Photo+Not+Available" alt="No photo available" class="w-full h-48 object-cover">`}
                            <div class="p-6">
                                <h2 class="text-xl font-semibold mb-2 text-gray-900">${restaurant.name}</h2>
                                <p class="text-gray-700 mb-2"><strong>Category:</strong> ${restaurant.query.category.join(', ')}</p>
                                <p class="text-gray-700 mb-2"><strong>Style/Vibe:</strong> ${restaurant.query.style_vibe}</p>
                                <p class="text-gray-700 mb-2"><strong>Audience Suitability:</strong> ${restaurant.query.audience_suitability}</p>
                                <p class="text-gray-700 mb-2"><strong>Distance:</strong> ${restaurant.query.distance}</p>
                                <p class="text-gray-700"><strong>Status:</strong> ${restaurant.query.time_availability}</p>
                            </div>
                        </div>
                        `;
                    });

                    html += '</div>';
                    container.innerHTML = html;
                    
                } catch (error) {
                    const container = document.getElementById('content');
                    container.innerHTML = `<p class="text-red-500 text-center">An error occurred: ${error.message}. Please check the console for details.</p>`;
                    console.error("Failed to fetch data:", error);
                } finally {
                    document.getElementById('loading-container').style.display = 'none';
                }
            }

            // Fetch results on page load
            window.onload = fetchAndRenderResults;

        </script>
    </body>
    </html>
    """
    return render_template_string(html_content)

@app.route('/data')
def get_data():
    """
    Endpoint to fetch data from APIs and return as JSON.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(get_restaurants_data(API_KEY_PLACES, LATITUDE, LONGITUDE))
    return jsonify(results)

if __name__ == '__main__':
    # When running in a local environment, use a different port to avoid conflicts
    # For a local environment, you would run this script directly: python your_script_name.py
    # This check is for the canvas environment.
    app.run(host='0.0.0.0', port=5000)
