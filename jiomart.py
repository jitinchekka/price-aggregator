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
    Searches JioMart using the Algolia API and dynamically built filters.
    """
    jiomart_results = []
    algolia_app_id = "3YP0HP3WSH"
    algolia_api_key = "aace3f18430a49e185d2c1111602e4b1"
    index_name = "prod_mart_master_vertical"
    algolia_url = f"https://{algolia_app_id}-dsn.algolia.net/1/indexes/*/queries"

    # --- Step 1: Get Location Codes ---
    location_data = get_jiomart_inventory_codes(pincode)
    if not location_data:
        logging.error(
            f"[JioMart] Failed to get inventory codes for pincode {pincode}. Aborting search.")
        return jiomart_results

    # --- Step 2: Build Dynamic Filters ---
    try:
        # Extract unique region codes
        all_region_codes = set()
        for codes in location_data.get("region_codes", {}).values():
            all_region_codes.update(codes)
        available_stores_filter = build_algolia_or_filter(
            "available_stores", list(all_region_codes))

        # Extract unique store codes (both 1P and 3P)
        all_store_codes = set()
        for codes in location_data.get("store_codes", {}).values():
            all_store_codes.update(codes)

        # Build the combined inventory filter clause checking both keys + ALL
        # Pattern based on original curl: (inventory_stores:ALL OR inventory_stores:C1 OR ... OR inventory_stores_3p:ALL OR inventory_stores_3p:C1 OR ...)
        inventory_clauses = ["inventory_stores:ALL", "inventory_stores_3p:ALL"]
        inventory_clauses.extend(
            [f"inventory_stores:{code}" for code in all_store_codes])
        inventory_clauses.extend(
            [f"inventory_stores_3p:{code}" for code in all_store_codes])
        inventory_filter = f"({' OR '.join(inventory_clauses)})"

        if not available_stores_filter or not all_store_codes:
            logging.warning(
                f"[JioMart] Could not extract sufficient codes from mapping response for {pincode}. Filters might be incomplete.")
            # Decide on fallback behavior - maybe abort, or use a default filter? Aborting is safer.
            return jiomart_results

        # Combine all filter parts
        base_filters = "(mart_availability:JIO OR mart_availability:JIO_WA)"
        exclusions = "(NOT vertical_code:ALCOHOL) AND (NOT vertical_code:LOCALSHOPS)"
        final_filters = f"{base_filters} AND {available_stores_filter} AND {exclusions} AND {inventory_filter}"

    except Exception as e:
        logging.error(
            f"[JioMart] Error building Algolia filters from location data: {e}")
        return jiomart_results  # Abort if filter building fails

    # --- Step 3: Construct Algolia Request ---
    params = {
        "query": query,
        "page": 0,
        "hitsPerPage": 20,
        # Ensure pincode is included
        "analyticsTags": json.dumps(["web", pincode, "Query Search"]),
        "filters": final_filters,  # Use the dynamically built filter string
        # Specify needed fields
        "attributesToRetrieve": '["name","price","variant_text","weight_string","url_path","image_url","image","sku","objectID"]',
        "attributesToHighlight": '[]',
        "clickAnalytics": "false",
        "userToken": "backend-aggregator-user-001"
    }
    encoded_params = urllib.parse.urlencode(params)
    request_body = {"requests": [
        {"indexName": index_name, "params": encoded_params}]}
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'x-algolia-application-id': algolia_app_id,
        'x-algolia-api-key': algolia_api_key,
        'Origin': 'https://www.jiomart.com',
        'Referer': 'https://www.jiomart.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    # --- Step 4: Call Algolia API ---
    logging.info(f"[JioMart] Calling Algolia API...")
    try:
        response = requests.post(
            algolia_url, headers=headers, json=request_body, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

         # Save the data variable to a file
        with open("algolia_response.json", "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)


        # --- Step 5: Parse and Normalize Response ---
        if not data or "results" not in data or not data["results"]:
            logging.warning(
                "[JioMart] Algolia response missing 'results' array.")
            return jiomart_results

        hits = data["results"][0].get("hits", [])
        logging.info(f"[JioMart] Algolia returned {len(hits)} hits.")

        for hit in hits:
            try:
                name = hit.get("name")
                mrp = hit.get("price", {}).get("mrp")
                selling_price = hit.get("price", {}).get(
                    "offer_price", hit.get("price", {}).get("sale_price"))
                variant = hit.get("variant_text", hit.get(
                    "weight_string"))  # Prefer variant_text
                url_path = hit.get("url_path")
                deeplink = f"https://www.jiomart.com{url_path}" if url_path else "https://www.jiomart.com"

                # Image URL Construction - Needs Verification of Base URL & filename key
                image_filename = hit.get("image_url", hit.get(
                    "image"))  # Prefer image_url if exists
                # ASSUMPTION - VERIFY THIS
                image_base = "https://www.jiomart.com/images/product/150x150/"
                image_url = f"{image_base}{image_filename}" if image_filename else None

                barcode = hit.get("sku")

                if name and selling_price is not None:
                    normalized_product = {
                        "name": name,
                        "mrp": float(mrp) if mrp is not None else None,
                        "selling_price": float(selling_price),
                        "image": image_url,
                        "variant": variant,
                        "barcode": barcode or "",
                        "deeplink": deeplink
                    }
                    jiomart_results.append(normalized_product)

            except (ValueError, TypeError) as e:
                logging.error(
                    f"[JioMart] Error converting data for hit {hit.get('objectID')}: {e}")
                continue
            except Exception as e:
                logging.error(
                    f"[JioMart] Error parsing one Algolia hit {hit.get('objectID')}: {e}")
                continue

        logging.info(
            f"[JioMart] Successfully normalized {len(jiomart_results)} products.")

    # (Keep existing exception handling: Timeout, HTTPError, RequestException, JSONDecodeError etc.)
    except requests.exceptions.Timeout:
        logging.error("[JioMart] Request to Algolia API timed out.")
    except requests.exceptions.HTTPError as e:
        logging.error(
            f"[JioMart] Algolia API returned HTTP error: {e.response.status_code} {e.response.text[:200]}...")
    except requests.exceptions.RequestException as e:
        logging.error(f"[JioMart] Failed to connect to Algolia API: {e}")
    except json.JSONDecodeError:
        logging.error(
            "[JioMart] Could not decode JSON response from Algolia API.")
    except Exception as e:
        logging.error(
            f"[JioMart] An unexpected error occurred during JioMart search: {e}")

    return jiomart_results


if __name__ == "__main__":
    # Example usage
    pincode = "500049"  # Replace with a valid pincode
    query = "milk"  # Replace with a search term
    results = search_jiomart_products(query, pincode)
    print(json.dumps(results, indent=2))
