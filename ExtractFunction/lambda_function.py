import json
import os
import copy # Bạn không cần copy nữa, nhưng để lại cũng không sao
import requests
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- CẤU HÌNH AWS & GOOGLE ---
try:
    # Tải credentials từ biến môi trường
    GOOGLE_API_CREDENTIALS = json.loads(os.environ.get('GOOGLE_CREDENTIALS', '{}'))
except json.JSONDecodeError:
    GOOGLE_API_CREDENTIALS = {} # Xử lý nếu JSON trong biến môi trường bị lỗi
SCOPES = ['https://www.googleapis.com/auth/documents.readonly']

def get_aws_blog_content(url):
    """Trích xuất nội dung văn bản từ một URL blog của AWS."""
    if not url or not url.startswith('http'):
        return f"Lỗi: URL blog AWS không hợp lệ: {url}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status() # Báo lỗi nếu status code là 4xx hoặc 5xx
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Selector 'article .blog-post-content' là khá đặc thù cho AWS Blog
        article_body = soup.select_one('article .blog-post-content')
        
        return article_body.get_text(separator='\n', strip=True) if article_body else "Không tìm thấy nội dung bài viết."
    except requests.RequestException as e:
        return f"Lỗi khi truy cập URL blog AWS: {e}"

def get_google_doc_content(url):
    """Trích xuất nội dung văn bản từ một URL Google Docs."""
    if not url or 'docs.google.com/document/d/' not in url:
        return f"Lỗi: URL Google Docs không hợp lệ. URL nhận được: '{url}'"
    
    try:
        if not GOOGLE_API_CREDENTIALS:
            return "Lỗi: Biến môi trường GOOGLE_CREDENTIALS chưa được cấu hình hoặc sai định dạng."
        
        creds = service_account.Credentials.from_service_account_info(
            GOOGLE_API_CREDENTIALS, scopes=SCOPES)
        service = build('docs', 'v1', credentials=creds)
        
        # Tách document_id từ URL
        document_id = url.split('/d/')[1].split('/')[0]
        
        # Gọi API Google Docs
        document = service.documents().get(documentId=document_id).execute()
        
        content = document.get('body', {}).get('content')
        doc_text = ''
        if content:
            for value in content:
                if 'paragraph' in value:
                    elements = value.get('paragraph', {}).get('elements', [])
                    for elem in elements:
                        if 'textRun' in elem:
                            doc_text += elem.get('textRun', {}).get('content', '')
        return doc_text.strip()
    except Exception as e:
        # Báo lỗi chi tiết hơn nếu có thể
        return f"Lỗi khi xử lý Google Docs API: {e}"

def lambda_handler(event, context):
    """Hàm xử lý của Lambda, trigger bởi SQS."""
    
    # Thay vì 'fully_formed_prompts', ta tạo list 'extracted_contents'
    extracted_contents = [] 
    records_count = len(event.get('Records', []))
    print(f"Bắt đầu xử lý {records_count} message(s) từ SQS.")

    for record in event.get('Records', []):
        payload_str = record.get('body', '{}')
        print(f"Received message body: {payload_str}") 

        try:
            payload = json.loads(payload_str)
            aws_blog_url = payload.get('aws_blog_url')
            google_doc_url = payload.get('google_doc_url')

            if not aws_blog_url or not google_doc_url:
                print(f"Warning: Bỏ qua message vì thiếu URL. Payload: {payload_str}")
                continue

            # === LOGGING CHI TIẾT ===
            print(f"Bắt đầu trích xuất AWS Blog: {aws_blog_url}")
            aws_content = get_aws_blog_content(aws_blog_url)
            if aws_content.startswith("Lỗi:") or aws_content == "Không tìm thấy nội dung bài viết.":
                 print(f"Kết quả trích xuất AWS Blog: {aws_content}")
            else:
                 print(f"Hoàn tất trích xuất AWS Blog. Độ dài: {len(aws_content)} ký tự.")

            print(f"Bắt đầu trích xuất Google Doc: {google_doc_url}")
            gdoc_content = get_google_doc_content(google_doc_url)
            if gdoc_content.startswith("Lỗi:"):
                print(f"Kết quả trích xuất Google Doc: {gdoc_content}")
            else:
                print(f"Hoàn tất trích xuất Google Doc. Độ dài: {len(gdoc_content)} ký tự.")
            # ========================
            
            if aws_content.startswith("Lỗi:") or gdoc_content.startswith("Lỗi:") or aws_content == "Không tìm thấy nội dung bài viết.":
                print(f"Warning: Bỏ qua message do lỗi trích xuất nội dung.")
                continue # Bỏ qua message này

            # === THAY ĐỔI LOGIC: Thêm nội dung vào list ===
            content_payload = {
                "original_article_content": aws_content,
                "translated_article_content": gdoc_content,
                "source_info": {
                    "aws_blog_url": aws_blog_url,
                    "google_doc_url": google_doc_url
                }
            }
            extracted_contents.append(content_payload)
            print(f"Đã trích xuất và lưu trữ nội dung thành công.")

        except Exception as e:
            print(f"Lỗi nghiêm trọng trong lambda_handler: {e} với payload: {payload_str}")
            continue

    # === LOG CUỐI CÙNG ===
    final_message = f'Hoàn tất xử lý. Đã trích xuất thành công nội dung từ {len(extracted_contents)} / {records_count} messages.'
    print(final_message)
    # =========================

    return {
        'statusCode': 200,
        'headers': { 'Content-Type': 'application/json' },
        'body': json.dumps({
            'message': final_message,
            # Trả về nội dung đã trích xuất
            'extracted_contents': extracted_contents 
        }, ensure_ascii=False, indent=2)
    }