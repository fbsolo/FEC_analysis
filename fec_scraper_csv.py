
#   Frank Solomon

#       x.com/silentaudit
#       ijuror.blogspot.com
#       bitvectors.blogspot.com
#       github.com/fbsolo

#
#   First, go to
#
#       https://api.open.fec.gov/developers/#/receipts/get_v1_schedules_schedule_a_
#
#   to get an FEC API key. Set up an AWS S3 bucket and an AWS EC2 instance.
#
#   Run this script in
#
#       AWS Systems Manager -> Node Tools -> Run Command
#
#   and pick the
#
#       AWS-RunShellScript
#
#   option. Place the script in the
#
#       Command parameters
#
#   box. In line 238
#
#       python3 -u fec_scraper_csv.py 2006-09-09 2>&1 | tee scraper_execution.log
#
#   at the bottom, pick a specific "YYYY-MM-DD" date to "scrape". Set the
#
#       Execution Timeout
#
#   textbox to
#
#       172800 (48 hours)
#
#   and click "Run". To watch the output, run
#
#		cd /tmp && tail -f scraper_execution.log
#
#	in a separate
#
#		AWS Systems Manager -> Session Manager
#
#	tab.

#Execution Timeout Override: Remember to find the explicit Execution Timeout (seconds) input parameter widget sitting directly under your code pane and change
# its text string to 172800 (48 hours).
# AWS-RunShellScript

#	CONFIGURATION: Set your specific bucket and file details here
#	BUCKET_NAME = '<S3_BUCKET_NAME>'	# Change to your bucket name
#   api_key = '<FEC_API_KEY>'    # Change to the FEC API key

cd /tmp
rm -f fec_scraper_csv.py

cat << 'END_OF_PYTHON_SCRIPT' > fec_scraper_csv.py
import boto3
import csv
import datetime
import io
import json
import os
import requests
import sys
import time

s3_client = boto3.client('s3')

def run_one_time_scraper(target_date_str):
    api_key = '<FEC_API_KEY>'
    bucket_name = '<S3_BUCKET_NAME>'
    
    base_url = "https://api.open.fec.gov/v1/schedules/schedule_a/"
    
    # Dynamic Optimization: Automatically match the two-year transaction period 
    # to the targeted date's year to prevent API parameters validation crashes
    try:
        target_year = int(target_date_str.split('-')[0])
        transaction_period = target_year if target_year % 2 == 0 else target_year + 1
    except Exception:
        transaction_period = 2020 
    
    params = {
        'api_key': api_key,
        'min_date': target_date_str,
        'max_date': target_date_str,
        'two_year_transaction_period': transaction_period,
        'per_page': 100,
        'sort': 'contribution_receipt_date'
    }
    
    #   The
    #
    #       https://api.open.fec.gov/developers/#/receipts/get_v1_schedules_schedule_a_
    #
    #   resource shows the available result-set columns for the selected_columns array. With your FEC API
    #   key, scroll down to
    #
    #       receipts -> GET /v1/schedules_schedule_a/ -> Parameters -> image_number
    #
    #   and type an image number in the textbox. Hit "execute" and the Code 200 response body will show a
    #   JSON block with the available columns.
    
    selected_columns = [
        'committee_id',
        'contributor_id',
        'contributor_name',
        'contributor_street_1',
        'contributor_street_2',
        'contributor_city',
        'contributor_state',
        'contributor_zip',
        'contributor_employer',
        'contributor_occupation',
        'contributor_aggregate_ytd',
        'contribution_receipt_date',
        'contribution_receipt_amount',
        'pdf_url',
        'sub_id',
        'orig_sub_id',
        'contribution_receipt_date'
    ]
    
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=selected_columns, extrasaction='ignore')
    writer.writeheader()

    total_records = 0
    page_count = 1  
    has_more = True

    print(f"Starting low-cost Ingestion loop for date: {target_date_str}")

    while has_more:
        max_retries = 5
        data = None
            
        for attempt in range(max_retries):
            try:
                response = requests.get(base_url, params=params, timeout=15)
                
                # --- FIXED RATE LIMIT HANDLING ---
                # Keep waiting out the 15-minute window if we are blocked. 
                # This does not advance the retry attempt or burn the data payload.
                while response.status_code == 429:
                    print(f" Quota fully exhausted on page {page_count}. Sleeping for 15 minutes to reset window...", flush=True)
                    time.sleep(900) 
                    response = requests.get(base_url, params=params, timeout=15)
                    
                if response.status_code == 422:
                    print(f" ERROR: API rejected parameters on page {page_count}.", file=sys.stderr)
                    print(f" Server response detail: {response.text}", file=sys.stderr)
                    return {"statusCode": 422, "body": "API parameter validation failure."}
                    
                response.raise_for_status()
                data = response.json()
                
                # --- PROACTIVE HEADER INSPECTION ---
                # Check how much quota is left. If we are running critically low, 
                # pause now before we throw a 429 error on the next page.
                remaining_quota = int(response.headers.get('X-RateLimit-Remaining', 1000))
                if remaining_quota <= 10:
                    print(f" Proactive Protection: Quota low ({remaining_quota} left). Sleeping 15 mins...", flush=True)
                    time.sleep(900)
                    
                break  
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    print(f" Request failed completely on attempt {max_retries}: {e}", file=sys.stderr, flush=True)
                time.sleep(2)

        if data is None:
            print(f" WARNING: Page {page_count} failed to fetch after all retries. Resubmitting parameters...", file=sys.stderr)
            has_more = True  
            continue 

        results = data.get('results', [])
        for row in results:
            writer.writerow(row)
        total_records += len(results)

        pagination = data.get('pagination', {})
        last_indexes = pagination.get('last_indexes', {}) or {}

        next_last_index = last_indexes.get('last_index')
        next_last_date = last_indexes.get('last_contribution_receipt_date')
        print(f"Processed page {page_count}. Records: {len(results)}.")

        if next_last_index and next_last_date and len(results) == 100:
            params['last_index'] = next_last_index
            params['last_contribution_receipt_date'] = next_last_date
            params.pop('page', None) 
            has_more = True
            page_count += 1
        else:
            has_more = False

    if total_records == 0:
        print("Completed execution. No records found to upload.")
        return {"statusCode": 200, "body": json.dumps("No records found.")}

    s3_key = f"schedule_a/receipt_date={target_date_str}/data_filtered.csv"
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print(f"Successfully saved {total_records} records across {page_count} pages to S3!")
        return {"statusCode": 200, "body": "Data uploaded successfully."}

    except Exception as e:
        print(f"S3 Upload failed: {e}")
        return {"statusCode": 500, "body": json.dumps(f"S3 error: {str(e)}")}

if __name__ == "__main__":
    if len(sys.argv) > 1:
        chosen_date = sys.argv[1] 
    else:
        print("Please provide a date parameter. Example: python3 fec_scraper_csv.py 2020-10-27")
        sys.exit(1)
        
    run_one_time_scraper(chosen_date)
END_OF_PYTHON_SCRIPT

sed -i 's/\r//g' fec_scraper_csv.py
sed -i 's/\t/    /g' fec_scraper_csv.py

# --- METADATA UPDATE: FIXED DATED FORMAT ---
# Changed format from 2020/01/13 to 2020-01-13 to align with standard API syntax
python3 -u fec_scraper_csv.py 2006-09-09 2>&1 | tee scraper_execution.log
