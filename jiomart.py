import requests
import json
import urllib.parse
import logging
# --- Configuration & Constants ---
DMART_BASE_URL = "https://www.dmart.in"
# Base URL for DMart images
DMART_IMAGE_BASE = "https://cdn.dmart.in/images/products/"
NINE_MINUTES_API_URL = "https://9minutes.in/api/fetch_products"
# Timeout for external API calls in seconds (e.g., 20 seconds) <--- ADD THIS LINE
REQUEST_TIMEOUT = 20

# Setup basic logging
# ... (rest of the logging setup) ...
# Add this helper function somewhere before search_jiomart_products


def build_algolia_or_filter(key_name, values):
    """Helper to build Algolia filter strings like (key:v1 OR key:v2)."""
    if not values:
        return ""
    # Ensure unique values
    unique_values = set(values)
    # Format: key:value
    clauses = [f"{key_name}:{value}" for value in unique_values]
    return f"({' OR '.join(clauses)})"

# --- Function to get JioMart Location Codes ---


def get_jiomart_inventory_codes(pincode):
    """
    Calls the JioMart API to get store/region codes for a pincode.
    Returns the parsed JSON data or None on failure.
    """
    mapping_url = f"https://www.jiomart.com/collection/mcat_pincode/get_mcat_inventory_code/{pincode}"
    headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-GB,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'priority': 'u=0, i',  # Keep priority header if observed
        'referer': 'https://www.jiomart.com/',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
        # NOTE: Cookies are omitted initially. Add if required based on testing.
    }
    logging.info(f"[JioMart Mapping] Calling API: {mapping_url}")
    try:
        response = requests.get(
            mapping_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        # Basic validation of response structure
        if "region_codes" in data and "store_codes" in data:
            logging.info(
                f"[JioMart Mapping] Successfully received codes for pincode {pincode}.")
            return data
        else:
            logging.warning(
                f"[JioMart Mapping] Received unexpected JSON structure for pincode {pincode}: {str(data)[:200]}...")
            return None
    except requests.exceptions.HTTPError as e:
        # Specifically check for 404 which might mean pincode not serviceable/found
        if e.response.status_code == 404:
            logging.warning(
                f"[JioMart Mapping] Pincode {pincode} not found or not serviceable (404 Error).")
        else:
            logging.error(
                f"[JioMart Mapping] API returned HTTP error: {e.response.status_code} {e.response.text[:200]}...")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"[JioMart Mapping] API call failed: {e}")
        return None
    except json.JSONDecodeError:
        logging.error(
            f"[JioMart Mapping] Failed to decode JSON response for pincode {pincode}.")
        return None
    except Exception as e:
        logging.error(f"[JioMart Mapping] An unexpected error occurred: {e}")
        return None

# --- Updated JioMart Search Function ---


def search_jiomart_products(query, pincode):
    """
    Searches JioMart using the Algolia API, dynamically built filters,
    and the latest observed attributesToRetrieve list.
    NOTE: Selling price is not directly requested/found in this specific query.
    """
    jiomart_results = []
    jiomart_base_url = "https://www.jiomart.com"
    algolia_app_id = "3YP0HP3WSH"
    algolia_api_key = "aace3f18430a49e185d2c1111602e4b1"
    index_name = "prod_mart_master_vertical"
    algolia_url = f"https://{algolia_app_id}-dsn.algolia.net/1/indexes/*/queries"

    # --- Step 1: Get Location Codes ---
    location_data = get_jiomart_inventory_codes(pincode)
    if not location_data:
        logging.error(f"[JioMart] Failed to get inventory codes for pincode {pincode}. Aborting search.")
        return jiomart_results

    # --- Step 2: Build Dynamic Filters ---
    try:
        all_region_codes = set(code for codes in location_data.get("region_codes", {}).values() for code in codes)
        available_stores_filter = build_algolia_or_filter("available_stores", list(all_region_codes))

        all_store_codes = set(code for codes in location_data.get("store_codes", {}).values() for code in codes)
        inventory_clauses = ["inventory_stores:ALL", "inventory_stores_3p:ALL"]
        inventory_clauses.extend([f"inventory_stores:{code}" for code in all_store_codes])
        inventory_clauses.extend([f"inventory_stores_3p:{code}" for code in all_store_codes])
        inventory_filter = f"({ ' OR '.join(inventory_clauses) })"

        if not available_stores_filter or not all_store_codes:
             logging.warning(f"[JioMart] Could not extract sufficient codes for {pincode}. Filters might be incomplete.")
             return jiomart_results

        base_filters = "(mart_availability:JIO OR mart_availability:JIO_WA)"
        exclusions = "(NOT vertical_code:ALCOHOL) AND (NOT vertical_code:LOCALSHOPS)"
        final_filters = f"{base_filters} AND {available_stores_filter} AND {exclusions} AND {inventory_filter}"

    except Exception as e:
        logging.error(f"[JioMart] Error building Algolia filters from location data: {e}")
        return jiomart_results

    # --- Step 3: Construct Algolia Request ---
    # Use the attributes observed in the latest curl command
    attributes_to_get = [
        "product_code", "display_name", "brand", "category_level.level4",
        "food_type", "buybox_mrp", "vertical_code", "image_path", "url_path", "objectID"
    ]
    params = {
        "query": query,
        "page": 0,
        "hitsPerPage": 5, # Keep reasonable limit
        "analyticsTags": json.dumps(["web", pincode, "Query Search"]),
        "filters": final_filters,
        "attributesToRetrieve": json.dumps(attributes_to_get), # Updated list
        "attributesToHighlight": '[]',
        "clickAnalytics": "false",
        "userToken": "backend-aggregator-user-003" # Increment token slightly
    }
    encoded_params = urllib.parse.urlencode(params)
    request_body = { "requests": [{ "indexName": index_name, "params": encoded_params }] }
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json', # Sticking with standard JSON content type
        'x-algolia-application-id': algolia_app_id,
        'x-algolia-api-key': algolia_api_key,
        'Origin': 'https://www.jiomart.com',
        'Referer': 'https://www.jiomart.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    # --- Step 4: Call Algolia API ---
    logging.info(f"[JioMart] Calling Algolia API...")
    try:
        response = requests.post(algolia_url, headers=headers, json=request_body, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

         # Save the data variable to a file
        with open("algolia_response.json", "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)


        # --- Step 5: Parse and Normalize Response ---
        if not data or "results" not in data or not data["results"]:
            logging.warning("[JioMart] Algolia response missing 'results' array.")
            return jiomart_results

        hits = data["results"][0].get("hits", [])
        logging.info(f"[JioMart] Algolia returned {len(hits)} hits.")

        for hit in hits:
            try:
                # Use the new field names based on attributesToRetrieve
                name = hit.get("display_name")
                mrp = hit.get("buybox_mrp") # Assuming this is the MRP field
                # !! Selling price field was not requested/identified !!
                selling_price = None # Set to None as we don't have it
                logging.debug(f"[JioMart] Selling price not available in hit for {hit.get('objectID')}, setting to None.")

                # Variant info seems missing from requested attributes, set to None
                variant = None #hit.get("variant_text", hit.get("weight_string"))

                url_path = hit.get("url_path")
                deeplink = f"{jiomart_base_url}{url_path}" if url_path else jiomart_base_url

                # Use image_path for image URL construction
                image_relative_url = hit.get("image_path")
                # Image base needs confirmation - using previous guess
                image_base = "https://www.jiomart.com/images/product/150x150/"
                image_url = f"{image_base}{image_relative_url}" if image_relative_url else None

                # Use product_code as barcode
                barcode = hit.get("product_code")

                # Validate essential fields (Name is essential, MRP is good to have)
                if name:
                    normalized_product = {
                        "name": name,
                        "mrp": float(mrp) if mrp is not None else None,
                        "selling_price": selling_price, # Will be None based on current info
                        "image": image_url,
                        "variant": variant, # Will likely be None
                        "barcode": barcode or "",
                        "deeplink": deeplink
                    }
                    jiomart_results.append(normalized_product)
                else:
                     logging.warning(f"[JioMart] Skipping hit {hit.get('objectID')} due to missing display_name.")

            except (ValueError, TypeError) as e:
                logging.error(f"[JioMart] Error converting data type for hit {hit.get('objectID')}: {e}")
                continue
            except Exception as e:
                logging.error(f"[JioMart] Error parsing one Algolia hit {hit.get('objectID')}: {e}")
                continue

        logging.info(f"[JioMart] Successfully normalized {len(jiomart_results)} products (selling price likely missing).")

    # (Keep existing exception handling)
    except requests.exceptions.Timeout:
        logging.error("[JioMart] Request to Algolia API timed out.")
    # ... other except blocks
    except Exception as e:
        logging.error(f"[JioMart] An unexpected error occurred during JioMart search: {e}")


    return jiomart_results

if __name__ == "__main__":
    # Example usage
    pincode = "500049"  # Replace with a valid pincode
    query = "milk"  # Replace with a search term
    results = search_jiomart_products(query, pincode)
    print(json.dumps(results, indent=2))
