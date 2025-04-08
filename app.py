import streamlit as st
import datetime
import re
import html
import requests
import csv
import io  # <-- added import for in-memory file handling

# -------------------------------------------------------------------------
# CONFIG - Replace with your own values or load from environment variables
# -------------------------------------------------------------------------
OPTICAL_URL: str = "https://cms.ppp.staging.optical.gov.sg"
OPTICAL_TOKEN: str = "2QhCxtAV_-y0pfpLY5sZ2rRNYcidHp4t"

# -------------------------------------------------------------------------
# MAIN LOGIC
# -------------------------------------------------------------------------
def fetch_deals(base_url: str, token: str):
    """
    Fetch all deals from Directus, returning the JSON array of results.
    """
    fields_list = [
        "name",
        "stage",
        "product.name",
        "value",
        "owner.email",
        "organization.name",
        "contact.email",
        "referrer.email",
        "engagement.name",
        "engagement.date",
        "metrics.product.name",
        "metrics.label",
        "metric1_estimated",
        "metric1_actual",
        "metric2_estimated",
        "metric2_actual",
        "metric3_estimated",
        "metric3_actual",
        "metric4_estimated",
        "metric4_actual",
        "notes"
    ]

    query_params = "&".join(f"fields[]={field}" for field in fields_list)
    url = f"{base_url}/items/deals?{query_params}"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    response = requests.get(url, headers=headers, verify=False)
    response.raise_for_status()

    data = response.json()
    return data.get("data", [])

def strip_html_tags(text: str) -> str:
    """
    Remove HTML tags and decode ampersand-based HTML entities.
    """
    if not text:
        return ""
    no_tags = re.sub(r"<[^>]*>", "", text)
    return html.unescape(no_tags)

def flatten_deal(deal):
    """
    Extract each of the relevant fields into a single dict for CSV writing.
    """
    def safe_get(dictionary, path, default=None):
        keys = path.split(".")
        current = dictionary
        for k in keys:
            if not isinstance(current, dict) or (k not in current):
                return default
            current = current[k]
        return current

    return {
        "name": deal.get("name", ""),
        "stage": deal.get("stage", ""),
        "product_name": safe_get(deal, "product.name", ""),
        "value": deal.get("value", ""),
        "owner_email": safe_get(deal, "owner.email", ""),
        "organization_name": safe_get(deal, "organization.name", ""),
        "contact_email": safe_get(deal, "contact.email", ""),
        "referrer_email": safe_get(deal, "referrer.email", ""),
        "engagement_name": safe_get(deal, "engagement.name", ""),
        "engagement_date": safe_get(deal, "engagement.date", ""),
        "metrics_product_name": safe_get(deal, "metrics.product.name", ""),
        "metrics_label": safe_get(deal, "metrics.label", ""),
        "metric1_estimated": deal.get("metric1_estimated", ""),
        "metric1_actual": deal.get("metric1_actual", ""),
        "metric2_estimated": deal.get("metric2_estimated", ""),
        "metric2_actual": deal.get("metric2_actual", ""),
        "metric3_estimated": deal.get("metric3_estimated", ""),
        "metric3_actual": deal.get("metric3_actual", ""),
        "metric4_estimated": deal.get("metric4_estimated", ""),
        "metric4_actual": deal.get("metric4_actual", ""),
        "notes": strip_html_tags(deal.get("notes", "")),
    }

def generate_csv_in_memory(deals):
    """
    Creates an in-memory CSV (via StringIO) containing flattened deal data
    and returns the buffer.
    """
    columns = [
        "name",
        "stage",
        "product_name",
        "value",
        "owner_email",
        "organization_name",
        "contact_email",
        "referrer_email",
        "engagement_name",
        "engagement_date",
        "metrics_product_name",
        "metrics_label",
        "metric1_estimated",
        "metric1_actual",
        "metric2_estimated",
        "metric2_actual",
        "metric3_estimated",
        "metric3_actual",
        "metric4_estimated",
        "metric4_actual",
        "notes"
    ]

    # Use StringIO to build the CSV as a string in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    for deal in deals:
        writer.writerow(flatten_deal(deal))

    # Important: seek back to start so we can read this buffer again
    output.seek(0)
    return output

def upload_file_in_memory(file_buffer, file_name: str, base_url: str, token: str, mime_type: str = "text/csv"):
    """
    Uploads a file to Directus, streaming from memory (rather than disk).
    1. Get or create 'Reports' folder
    2. If file with same name exists, PATCH; otherwise POST
    """
    folders_endpoint = f"{base_url}/folders"
    files_endpoint = f"{base_url}/files"
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Get or create the 'Reports' folder
    try:
        response = requests.get(
            folders_endpoint,
            params={"filter[name][_eq]": "Reports"},
            headers=headers,
            verify=False
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise Exception(f"Error checking folders in Directus: {e.response.text}")

    folder_data = response.json().get("data", [])
    if folder_data:
        folder_id = folder_data[0]["id"]
    else:
        # Create folder
        try:
            create_res = requests.post(
                folders_endpoint,
                json={"name": "Reports", "parent": None},
                headers=headers,
                verify=False
            )
            create_res.raise_for_status()
            folder_id = create_res.json()["data"]["id"]
        except requests.exceptions.HTTPError as e:
            raise Exception(f"Error creating 'Reports' folder: {e.response.text}")

    # 2. Check if a file with the same name already exists
    try:
        existing_file_res = requests.get(
            files_endpoint,
            params={
                "filter[folder][_eq]": folder_id,
                "filter[filename_download][_eq]": file_name
            },
            headers=headers,
            verify=False
        )
        existing_file_res.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise Exception(f"Error searching for existing file: {e.response.text}")

    existing_file_data = existing_file_res.json().get("data", [])

    # Prepare data for upload
    # Note that for multipart/form-data, requests needs a tuple: (filename, file_obj, content_type)
    files_payload = {"file": (file_name, file_buffer, mime_type)}
    folder_payload = {"folder": folder_id}

    # 3. If file exists, PATCH, else POST
    if existing_file_data:
        existing_file_id = existing_file_data[0]["id"]
        try:
            patch_res = requests.patch(
                f"{files_endpoint}/{existing_file_id}",
                headers=headers,
                files=files_payload,
                data=folder_payload,
                verify=False
            )
            patch_res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise Exception(f"Error replacing file in Directus: {e.response.text}")
    else:
        try:
            post_res = requests.post(
                files_endpoint,
                headers=headers,
                files=files_payload,
                data=folder_payload,
                verify=False
            )
            post_res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise Exception(f"Error uploading file to Directus: {e.response.text}")

def run_pipeline():
    """
    Main pipeline:
      1. Create a timestamped filename
      2. Fetch deals
      3. Generate in-memory CSV
      4. Upload the in-memory CSV to 'Reports' folder
    """
    base_url = OPTICAL_URL
    token = OPTICAL_TOKEN

    csv_prefix = "deals"
    csv_filename = f"{csv_prefix}-{datetime.datetime.today().strftime('%Y%m%d-%H')}h.csv"

    deals = fetch_deals(base_url, token)
    csv_buffer = generate_csv_in_memory(deals)

    upload_file_in_memory(
        file_buffer=csv_buffer,
        file_name=csv_filename,
        base_url=base_url,
        token=token,
        mime_type="text/csv"
    )

    return {
        "csv_filename": csv_filename,
        "record_count": len(deals)
    }

# -------------------------------------------------------------------------
# STREAMLIT AS "API" ENDPOINT
# -------------------------------------------------------------------------
st.set_page_config(layout="centered", page_title="CRM Export API")
st.write("This is a minimal API endpoint via Streamlit. Call /?upload=1 to run the pipeline.")

query_params = st.query_params
if "upload" in query_params:
    try:
        result = run_pipeline()
        st.json({
            "status": "success",
            "file_uploaded": result["csv_filename"],
            "record_count": result["record_count"]
        })
    except Exception as e:
        st.json({"status": "error", "error_message": str(e)})
