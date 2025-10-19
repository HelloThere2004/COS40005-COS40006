import json
import os 
import requests
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- CẤU HÌNH ---
# Thông tin xác thực API Google (lưu dưới dạng biến môi trường trong Lambda)
# Bạn cần tạo một tệp credentials.json từ Google Cloud Console
# và lưu nội dung của nó vào một biến môi trường Lambda.
GOOGLE_API_CREDENTIALS = json.loads(os.environ['GOOGLE_CREDENTIALS'])
SCOPES = ['https://www.googleapis.com/auth/documents.readonly']

def get_aws_blog_content(url):
    """Trích xuất siêu dữ liệu (metadata) và nội dung từ một URL blog của AWS."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # 1. Trích xuất Tiêu đề
        title_element = soup.select_one('h1.blog-post-title')
        title = title_element.get_text(strip=True) if title_element else "Không tìm thấy tiêu đề."

        # 2. Trích xuất thông tin từ footer.blog-post-meta
        meta_element = soup.select_one('footer.blog-post-meta')
        authors = "Không tìm thấy tác giả."
        post_date = "Không tìm thấy ngày đăng."
        categories = []

        if meta_element:
            # Tác giả: nằm trong span đầu tiên, bắt đầu bằng "by "
            author_span = meta_element.find('span')
            if author_span:
                author_text = author_span.get_text(strip=True)
                if author_text.startswith('by '):
                    authors = author_text[3:]  # Loại bỏ "by "

            # Ngày đăng: nằm trong thẻ <time>
            time_element = meta_element.find('time')
            if time_element:
                post_date = time_element.get_text(strip=True)

            # Danh mục: tìm tất cả các thẻ <a> trong meta (trừ Permalink, Comments, Share)
            all_links = meta_element.find_all('a')
            exclude_texts = ['Permalink', 'Comments']
            for link in all_links:
                link_text = link.get_text(strip=True)
                if link_text not in exclude_texts and 'Share' not in link_text:
                    # Lấy text của thẻ a, đây là danh mục
                    categories.append(link_text)

        # 3. Trích xuất nội dung chính
        article_body_element = soup.select_one('article .blog-post-content')
        content = article_body_element.get_text(separator='\n', strip=True) if article_body_element else "Không tìm thấy nội dung."
        
        # 4. Trả về một dictionary chứa tất cả thông tin
        return {
            'title': title,
            'authors': authors,
            'post_date': post_date,
            'categories': categories,
            'content': content
        }
            
    except requests.RequestException as e:
        return {
            'title': "Lỗi",
            'authors': "Lỗi", 
            'post_date': "Lỗi",
            'categories': [],
            'content': f"Lỗi khi truy cập URL blog AWS: {e}"
        }

def get_google_doc_content(url):
    """Trích xuất nội dung văn bản từ một URL Google Docs."""
    try:
        creds = service_account.Credentials.from_service_account_info(
            GOOGLE_API_CREDENTIALS, scopes=SCOPES)
        
        service = build('docs', 'v1', credentials=creds)
        
        # Trích xuất DOCUMENT_ID từ URL
        document_id = url.split('/d/')[1].split('/')[0]

        document = service.documents().get(documentId=document_id).execute()
        
        content = document.get('body').get('content')
        
        doc_text = ''
        for value in content:
            if 'paragraph' in value:
                elements = value.get('paragraph').get('elements')
                for elem in elements:
                    if 'textRun' in elem:
                        doc_text += elem.get('textRun').get('content')
        
        return doc_text.strip()

    except Exception as e:
        return f"Lỗi khi truy cập Google Docs API: {e}"

def lambda_handler(event, context):
    """Hàm xử lý chính của AWS Lambda."""
    # Lấy URL từ sự kiện đầu vào
    aws_blog_url = event.get('aws_blog_url')
    google_doc_url = event.get('google_doc_url')

    if not aws_blog_url or not google_doc_url:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Vui lòng cung cấp cả aws_blog_url và google_doc_url.'})
        }

    # Trích xuất nội dung
    aws_content = get_aws_blog_content(aws_blog_url)
    gdoc_content = get_google_doc_content(google_doc_url)

    # Tạo đối tượng JSON kết quả
    result = {
        'aws_blog_content': {
            'source_url': aws_blog_url,
            'content': aws_content
        },
        'google_doc_translation': {
            'source_url': google_doc_url,
            'content': gdoc_content
        }
    }

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(result, ensure_ascii=False, indent=4)
    }