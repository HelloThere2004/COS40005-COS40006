import json
import os
import uuid
import boto3
import requests
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build
from markdownify import markdownify as md 

# --- CẤU HÌNH ---
S3_BUCKET = os.environ.get('S3_BUCKET_NAME')
s3_client = boto3.client('s3')

# Google Creds
try:
    GOOGLE_API_CREDENTIALS = json.loads(os.environ.get('GOOGLE_CREDENTIALS', '{}'))
except json.JSONDecodeError:
    GOOGLE_API_CREDENTIALS = {}

# [THAY ĐỔI 1]: Đổi Scope sang Drive để có quyền Export file
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

def get_aws_blog_markdown(url):
    """Trích xuất HTML AWS và chuyển sang Markdown."""
    if not url or not url.startswith('http'):
        return None, "Invalid URL"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        article_body = soup.select_one('article .blog-post-content')
        if not article_body:
            return None, "Content not found"

        markdown_text = md(str(article_body), heading_style="ATX", strip=['script', 'style'])
        return markdown_text.strip(), None
    except Exception as e:
        return None, str(e)

def get_google_doc_markdown_via_export(url):
    """
    [LOGIC MỚI]: Dùng Drive API export ra HTML -> convert sang Markdown
    """
    if not url or 'docs.google.com/document/d/' not in url:
        return None, "Invalid Google Doc URL"
    try:
        if not GOOGLE_API_CREDENTIALS:
            return None, "Missing Google Credentials"
        
        creds = service_account.Credentials.from_service_account_info(
            GOOGLE_API_CREDENTIALS, scopes=SCOPES)
        
        # [THAY ĐỔI 2]: Build service 'drive' thay vì 'docs'
        service = build('drive', 'v3', credentials=creds)
        
        # Lấy File ID từ URL
        file_id = url.split('/d/')[1].split('/')[0]
        
        # [THAY ĐỔI 3]: Gọi API export sang text/html
        response = service.files().export(
            fileId=file_id,
            mimeType='text/html'
        ).execute()
        
        # Response trả về là bytes, decode sang string
        html_content = response.decode('utf-8')
        
        # Dùng lại markdownify để chuyển đổi
        markdown_text = md(html_content, heading_style="ATX")
        
        return markdown_text.strip(), None

    except Exception as e:
        return None, f"Error exporting Google Doc: {str(e)}"

def upload_to_s3(content, folder, file_name):
    """Hàm upload."""
    key = f"{folder}/{file_name}"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=content.encode('utf-8'),
        ContentType='text/markdown' # Cả 2 giờ đều là Markdown
    )
    return key

def lambda_handler(event, context):
    processed_results = []
    print(f"Processing {len(event.get('Records', []))} records.")

    for record in event.get('Records', []):
        try:
            payload = json.loads(record.get('body', '{}'))
            aws_url = payload.get('aws_blog_url')
            doc_url = payload.get('google_doc_url')
            
            # 1. Trích xuất (Cả 2 đều ra Markdown)
            aws_md, aws_err = get_aws_blog_markdown(aws_url)
            doc_md, doc_err = get_google_doc_markdown_via_export(doc_url)
            
            if aws_err or doc_err:
                print(f"Error extracting: AWS={aws_err}, Doc={doc_err}")
                continue
            
            # 2. Tạo ID
            article_id = str(uuid.uuid4())[:8]
            
            # 3. Upload lên S3 (Cả 2 file đều đuôi .md)
            original_key = upload_to_s3(aws_md, 'original', f"{article_id}.md")
            translated_key = upload_to_s3(doc_md, 'translated', f"{article_id}.md")
            
            # 4. Output
            processed_results.append({
                "article_id": article_id,
                "s3_bucket": S3_BUCKET,
                "original_key": original_key,
                "translated_key": translated_key,
                "urls": { "aws": aws_url, "doc": doc_url }
            })
            
        except Exception as e:
            print(f"Critical error processing record: {e}")
            continue

    return {
        "status": "success",
        "data": processed_results
    }