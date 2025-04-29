import requests
import urllib.parse
import json
from flask import Flask, request, jsonify
from concurrent.futures import ThreadPoolExecutor
import logging # Added for better logging
from jiomart import search_jiomart_products

# --- Configuration & Constants ---
DMART_BASE_URL = "https://www.dmart.in"
DMART_IMAGE_BASE = "https://images.dmart.in/images/rwd/products/" # Needs verification
NINE_MINUTES_API_URL = "https://9minutes.in/api/fetch_products"
REQUEST_TIMEOUT = 20 # Increased timeout slightly for external aggregator

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Location Mapping (VERY BASIC - NEEDS PROPER IMPLEMENTATION) ---
def get_9minutes_location_string(pincode):
    """
    Maps pincode to the location string format required by 9minutes.in API.
    !!! THIS IS A MAJOR SIMPLIFICATION / BOTTLENECK !!!
    A real implementation needs a robust geocoding/mapping service.
    """
    if pincode == "500032":
        # Hardcoded example based on user input
        return "500032_gachibowli_circle_hyd"
    # Add other known mappings here if needed for testing
    # elif pincode == "400076":
    #     return "400076_powai_mum" # Example hypothetical format
    else:
        logging.warning(f"No location mapping available for pincode {pincode} for 9minutes.in API.")
        # Fallback or error? Let's return None to indicate failure.
        return None

# --- DMart Specific Functions (Keep as before) ---
def get_dmart_store_id(pincode):
    logging.info(f"[DMart] Attempting to find store ID for pincode: {pincode}")
    if pincode == "500049" or pincode == "500032":
         logging.info("[DMart] Using hardcoded store ID 10733 for pincode 500032 (DEMO ONLY)")
         return "10733"
    elif pincode == "400076":
         logging.info("[DMart] Using hardcoded store ID 10011 for pincode 400076 (DEMO ONLY)")
         return "10011"
    else:
         logging.warning(f"[DMart] No mapping found for pincode {pincode} in demo.")
         return None

def search_dmart_products(query, pincode):
    """
    Searches DMart and normalizes the response, using the CORRECTED image URL structure.
    """
    dmart_results = []
    store_id = get_dmart_store_id(pincode)
    if not store_id:
        logging.warning("[DMart] Could not get store ID. Skipping DMart search.")
        return dmart_results # Return empty list

    encoded_query = urllib.parse.quote(query)
    search_url = f"https://digital.dmart.in/api/v3/search/{encoded_query}?storeId={store_id}"
    search_headers = {
        'Origin': 'https://www.dmart.in',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }
    logging.info(f"[DMart] Calling Search API: {search_url}")
    try:
        response = requests.get(search_url, headers=search_headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        raw_product_list = data.get("products", [])
        logging.info(f"[DMart] Received response. Found {len(raw_product_list)} product entries.")

        for product_item in raw_product_list:
            parent_name = product_item.get("name")
            target_url_path = product_item.get("targetUrl")
            full_product_url = f"{DMART_BASE_URL}{target_url_path}" if target_url_path else None

            skus = product_item.get("sKUs", [])
            for sku_item in skus:
                try:
                    if sku_item.get("buyable") != "true" or sku_item.get("invType") == "OOS":
                        continue # Skip non-buyable or OOS items

                    mrp_str = sku_item.get("priceMRP")
                    selling_price_str = sku_item.get("priceSALE")
                    variant = sku_item.get("variantTextValue")
                    # --- Corrected Image URL Logic ---
                    product_image_key = sku_item.get("productImageKey")
                    img_code = sku_item.get("imgCode")
                    image_url = None # Default to None
                    if product_image_key and img_code:
                        image_url = f"https://cdn.dmart.in/images/products/{product_image_key}_{img_code}_P.jpg"
                    else:
                        # Fallback or log warning if key components are missing
                        image_key_fallback = sku_item.get("imageKey") # Check fallback
                        if image_key_fallback and img_code:
                             image_url = f"https://cdn.dmart.in/images/products/{image_key_fallback}_{img_code}_P.jpg"
                             logging.debug(f"[DMart] Used fallback imageKey for SKU {sku_item.get('skuUniqueID')}")
                        else:
                            logging.warning(f"[DMart] Missing productImageKey or imgCode for SKU {sku_item.get('skuUniqueID')}. Cannot construct image URL.")
                    # --- End Corrected Image URL Logic ---

                    barcode = sku_item.get("articleNumber") # Use articleNumber as potential barcode/EAN

                    if parent_name and mrp_str and selling_price_str:
                        # Normalize data to target structure
                        normalized_product = {
                            "name": parent_name,
                            "mrp": float(mrp_str) if mrp_str else None,
                            "selling_price": float(selling_price_str) if selling_price_str else None,
                            "image": image_url, # Use the constructed URL
                            "variant": variant,
                            "barcode": barcode or "", # Ensure string, default empty
                            "deeplink": full_product_url or "" # Ensure string, default empty
                        }
                        dmart_results.append(normalized_product)

                except (ValueError, TypeError) as e:
                    logging.error(f"[DMart] Error converting price for SKU {sku_item.get('skuUniqueID')}: {e}")
                    continue
                except Exception as e:
                    logging.error(f"[DMart] Error parsing one SKU item: {e} - SKU: {sku_item}")
                    continue

        logging.info(f"[DMart] Successfully normalized {len(dmart_results)} SKUs.")

    # Basic exception handling for the request
    except requests.exceptions.RequestException as e:
        logging.error(f"[DMart] API call failed: {e}")
    except json.JSONDecodeError:
        logging.error("[DMart] Failed to decode JSON response.")
    except Exception as e:
         logging.error(f"[DMart] An unexpected error occurred: {e}")

    return dmart_results

# --- Functions using 9minutes.in API ---

def call_9minutes_api(query, pincode):
    """
    Helper function to call the 9minutes.in API.
    Returns the parsed JSON response or None on failure.
    """
    location_string = get_9minutes_location_string(pincode)
    if not location_string:
        logging.error(f"[9minutes Helper] Cannot proceed without location string for pincode {pincode}.")
        return None # Cannot proceed without valid location mapping

    encoded_query = urllib.parse.quote(query)
    api_url = f"{NINE_MINUTES_API_URL}?query={encoded_query}&location={location_string}"

    headers = {
        'Accept': '*/*',
        'Connection': 'keep-alive',
        'Referer': 'https://9minutes.in/', # Important based on curl example
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36' # Good practice
    }

    logging.info(f"[9minutes Helper] Calling API: {api_url}")
    try:
        response = requests.get(api_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status() # Check for HTTP errors
        data = response.json()
        logging.info(f"[9minutes Helper] Successfully received data.")
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"[9minutes Helper] API call failed: {e}")
        return None
    except json.JSONDecodeError:
        logging.error("[9minutes Helper] Failed to decode JSON response.")
        return None
    except Exception as e:
        logging.error(f"[9minutes Helper] An unexpected error occurred: {e}")
        return None

def search_instamart_products(query, pincode):
    """Gets Instamart products by calling the 9minutes.in API."""
    logging.info(f"[Instamart] Getting data via 9minutes.in API for query='{query}', pincode='{pincode}'")
    data = call_9minutes_api(query, pincode)
    if data and isinstance(data.get("instamart_products"), list):
         # Data is already normalized by 9minutes.in, just return the list
         results = data["instamart_products"]
         logging.info(f"[Instamart] Extracted {len(results)} products via 9minutes.in.")
         # Optional: Add type conversion safety if needed
         for item in results:
             item['mrp'] = float(item['mrp']) if item.get('mrp') is not None else None
             item['selling_price'] = float(item['selling_price']) if item.get('selling_price') is not None else None
         return results
    else:
        logging.warning("[Instamart] Failed to get valid Instamart data from 9minutes.in response.")
        return []

def search_zepto_products(query, pincode):
    """Gets Zepto products by calling the 9minutes.in API."""
    logging.info(f"[Zepto] Getting data via 9minutes.in API for query='{query}', pincode='{pincode}'")
    data = call_9minutes_api(query, pincode)
    if data and isinstance(data.get("zepto_products"), list):
         results = data["zepto_products"]
         logging.info(f"[Zepto] Extracted {len(results)} products via 9minutes.in.")
         # Optional: Add type conversion safety if needed
         for item in results:
             item['mrp'] = float(item['mrp']) if item.get('mrp') is not None else None
             item['selling_price'] = float(item['selling_price']) if item.get('selling_price') is not None else None
         return results
    else:
        logging.warning("[Zepto] Failed to get valid Zepto data from 9minutes.in response.")
        return []

def search_blinkit_products(query, pincode):
    """Gets Blinkit products by calling the 9minutes.in API."""
    logging.info(f"[Blinkit] Getting data via 9minutes.in API for query='{query}', pincode='{pincode}'")
    data = call_9minutes_api(query, pincode)
    if data and isinstance(data.get("blinkit_products"), list):
         results = data["blinkit_products"]
         logging.info(f"[Blinkit] Extracted {len(results)} products via 9minutes.in.")
         # Optional: Add type conversion safety if needed
         for item in results:
             item['mrp'] = float(item['mrp']) if item.get('mrp') is not None else None
             item['selling_price'] = float(item['selling_price']) if item.get('selling_price') is not None else None
         return results
    else:
        logging.warning("[Blinkit] Failed to get valid Blinkit data from 9minutes.in response.")
        return []


# --- Flask API Setup ---
app = Flask(__name__)

@app.route('/search_all', methods=['GET'])
def search_all_platforms():
    """
    API endpoint to search across all integrated platforms concurrently.
    Accepts 'query' and 'pincode' as URL parameters.
    """
    query = request.args.get('query')
    pincode = request.args.get('pincode')

    # Input validation
    if not query or not pincode:
        return jsonify({"error": "Missing 'query' or 'pincode' parameter"}), 400
    if not pincode.isdigit() or len(pincode) != 6:
         return jsonify({"error": "Pincode must be 6 digits"}), 400

    logging.info(f"\n--- New Request Start ---")
    logging.info(f"Received request: query='{query}', pincode='{pincode}'")

    results = {} # Store results temporarily

    # Use ThreadPoolExecutor to run searches concurrently
    # One worker per platform function
    with ThreadPoolExecutor(max_workers=4) as executor:
        logging.info("Submitting tasks to executor...")
        # Submit tasks: DMart direct, others via 9minutes helper
        future_instamart = executor.submit(search_instamart_products, query, pincode)
        future_zepto = executor.submit(search_zepto_products, query, pincode)
        future_blinkit = executor.submit(search_blinkit_products, query, pincode)
        future_dmart = executor.submit(search_dmart_products, query, pincode)
        future_jiomart = executor.submit(search_jiomart_products, query, pincode)
        logging.info("Tasks submitted.")

        # Wait for completion and retrieve results safely
        logging.info("Waiting for results...")
        try:
            results["instamart_products"] = future_instamart.result()
        except Exception as e:
            logging.error(f"Exception retrieving Instamart results: {e}")
            results["instamart_products"] = [] # Ensure key exists

        try:
            results["zepto_products"] = future_zepto.result()
        except Exception as e:
            logging.error(f"Exception retrieving Zepto results: {e}")
            results["zepto_products"] = []

        try:
            results["blinkit_products"] = future_blinkit.result()
        except Exception as e:
            logging.error(f"Exception retrieving Blinkit results: {e}")
            results["blinkit_products"] = []

        try:
            results["dmart_products"] = future_dmart.result()
        except Exception as e:
            logging.error(f"Exception retrieving DMart results: {e}")
            results["dmart_products"] = []
            
        try: results["jiomart_products"] = future_jiomart.result()
        except Exception as e: logging.error(f"Exception retrieving JioMart results: {e}"); results["jiomart_products"] = []


    logging.info("All searches completed or timed out.")

    # Ensure the final response has the correct keys, even if lists are empty
    final_response = {
        "instamart_products": results.get("instamart_products", []),
        "zepto_products": results.get("zepto_products", []),
        "blinkit_products": results.get("blinkit_products", []),
        "dmart_products": results.get("dmart_products", []),
        "jiomart_products": results.get("jiomart_products", [])
    }

    logging.info(f"Returning combined results. Instamart: {len(final_response['instamart_products'])}, Zepto: {len(final_response['zepto_products'])}, Blinkit: {len(final_response['blinkit_products'])}, DMart: {len(final_response['dmart_products'])}")
    logging.info(f"--- Request End ---")

    return jsonify(final_response)

# --- Main Execution ---
if __name__ == '__main__':
    print("Starting Flask Aggregator API...")
    print("WARNING: This API relies on unofficial methods and external services (9minutes.in, DMart APIs).")
    print("WARNING: Pincode-to-location mapping is currently hardcoded for specific examples.")
    print("Listening on http://0.0.0.0:5001/")
    # Use host='0.0.0.0' to make it accessible on your network
    app.run(host='0.0.0.0', port=5001, debug=False) # Set debug=False for cleaner production logs