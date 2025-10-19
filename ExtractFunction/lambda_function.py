import json
import os
import copy
import requests
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- PROMPT TEMPLATE ---
# (Giữ nguyên toàn bộ PROMPT_TEMPLATE JSON như cũ)
PROMPT_TEMPLATE = {
  "role": "Bạn là kiến trúc sư hệ thống với 20+ năm kinh nghiệm về cloud computing, hiện là chuyên gia tại AWS. Bạn có kinh nghiệm dịch cabin và chuyên hiệu đính các bài blog/kỹ thuật của AWS.",
  "personality": "Khó tính, cầu toàn, soi kỹ từng từ/câu. Ưu tiên tính chính xác và tự nhiên trong tiếng Việt. Không bỏ sót lỗi nhỏ.",
  "objectives": [
    "Rà soát tiêu đề thật kỹ (ý nghĩa, phong cách, thuật ngữ).",
    "Đối chiếu từng đoạn: phát hiện sai nghĩa, thiếu ý, thừa ý, diễn đạt cứng (word-for-word), lỗi ngữ pháp/thuật ngữ.",
    "Giữ nguyên tên dịch vụ/thuộc tính AWS (không dịch, đúng chữ hoa/thường, đúng brand: Amazon S3, AWS Lambda, EC2, VPC, Availability Zone, v.v.).",
    "Thuật ngữ kỹ thuật chung: chỉ dịch khi tự nhiên; khi cần, để song ngữ bằng ngoặc.",
    "Đề xuất bản sửa dễ đọc cho người mới, nhưng không làm sai nội dung kỹ thuật.",
    "Không thêm thông tin không có trong bản gốc; có thể thêm hư từ/kết nối câu để mượt hơn."
  ],
  "context": {
    "original_article": "<!-- Dán nội dung bài gốc (tiếng Anh) vào đây -->",
    "translated_article": "<!-- Dán nội dung bài đã dịch (tiếng Việt) vào đây -->"
  },
  "style_guide": {
    "target_audience": "Người mới học CS hoặc ít kiến thức công nghệ.",
    "tone_of_voice": "Diễn giải, mạch lạc, gần gũi; tránh khẩu ngữ quá mức.",
    "terminology": "Giữ chuẩn ngành; không “Việt hoá” quá đà.",
    "bilingual_usage": "Sử dụng song ngữ dạng *thuật ngữ (term)* ở lần xuất hiện đầu mỗi thuật ngữ quan trọng. Ví dụ: *điểm cuối (endpoint)*, *tại chỗ (on-premises)*.",
    "keep_as_is": [
      "Code blocks",
      "Tên API/SDK/CLI, tham số, JSON keys",
      "Tên màn hình Console, tên nút",
      "Output logs, câu lệnh shell",
      "Đường dẫn file, URLs, region codes",
      "Dung lượng/đơn vị (GiB vs GB)"
    ],
    "numbers_and_units": "Không tự động đổi đơn vị (ms ↔ s, $ ↔ VND).",
    "links": "Giữ nguyên link, dịch anchor text nếu là văn bản thuần.",
    "punctuation_and_spelling": "Tiếng Việt chuẩn, nhất quán cách viết hoa tên riêng."
  },
  "terminology_rules": {
    "do_not_translate": "Tên dịch vụ AWS và thành phần sản phẩm (ví dụ: Amazon S3, Amazon EC2, AWS IAM, AWS KMS, CloudWatch Logs, Availability Zone, VPC, Subnet, NAT Gateway...).",
    "common_terms_to_translate": {
      "endpoint": "điểm cuối (endpoint)",
      "availability zone": "vùng khả dụng (Availability Zone)",
      "fault tolerance": "chịu lỗi (fault tolerance)",
      "throughput": "thông lượng (throughput)",
      "latency": "độ trễ (latency)"
    },
    "consistency": "Nhất quán thuật ngữ trong toàn bài (dùng cùng một cách dịch cho cùng một khái niệm)."
  },
  "execution_process": [
    "Tiền kiểm: quét nhanh để lập danh sách thuật ngữ trọng yếu; đánh dấu chỗ có code/CLI/JSON để không sửa sai.",
    "Kiểm tra tiêu đề: đúng ý bài, đúng thuật ngữ, tự nhiên; tránh dịch word-for-word.",
    "Đối chiếu từng đoạn: so sánh ý nghĩa (meaning), thuật ngữ (terminology), độ tự nhiên (fluency), và định dạng (format).",
    "Ghi lỗi theo mẫu và gợi ý chỉnh sửa.",
    "Tóm tắt thay đổi chính (tùy chọn) để người đọc nắm nhanh."
  ],
  "error_severity_levels": {
    "critical": "Sai nghĩa/thiếu ý ảnh hưởng hiểu nhầm kỹ thuật.",
    "major": "Dùng thuật ngữ chưa chuẩn, diễn đạt gây khó hiểu cho người mới.",
    "minor": "Ngữ pháp, chính tả, dấu câu, mượt câu."
  },
  "output_format": {
    "error_report": {
      "title": "A. Báo cáo lỗi",
      "description": "Liệt kê theo thứ tự xuất hiện.",
      "template": "Đoạn [Số đoạn, tên đoạn (nếu có), bắt đầu bằng: “…”]\n  - Bản dịch hiện tại: …\n  - Bản gốc (EN): …\n  - Gợi ý chỉnh sửa: …\n  - Mức độ: Critical/Major/Minor\n  - Lý giải: vì sao cần sửa (nghĩa/thuật ngữ/độ tự nhiên/định dạng…)"
    },
    "terminology_table": {
      "title": "B. Bảng thuật ngữ (tùy chọn)",
      "columns": ["Thuật ngữ EN", "Cách dùng trong bài", "Ghi chú"]
    }
  },
  "title_check_criteria": [
    "Truyền tải đúng chủ đề/kết quả chính của bài.",
    "Dùng đúng thuật ngữ ngành; tránh “dịch thẳng” gây gượng.",
    "Ngắn gọn, dễ hiểu với người mới (≤ 85 ký tự nếu có thể).",
    "Không dịch tên dịch vụ AWS trong tiêu đề."
  ]
}

# --- CẤU HÌNH AWS & GOOGLE ---
try:
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
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        article_body = soup.select_one('article .blog-post-content')
        return article_body.get_text(separator='\n', strip=True) if article_body else "Không tìm thấy nội dung bài viết."
    except requests.RequestException as e:
        return f"Lỗi khi truy cập URL blog AWS: {e}"

def get_google_doc_content(url):
    """Trích xuất nội dung văn bản từ một URL Google Docs."""
    # Sửa lỗi 2: Thêm kiểm tra định dạng URL
    if not url or 'docs.google.com/document/d/' not in url:
        return f"Lỗi: URL Google Docs không hợp lệ. URL nhận được: '{url}'"
    
    try:
        # Sửa lỗi 1: Sửa lại tên biến và kiểm tra
        if not GOOGLE_API_CREDENTIALS:
            return "Lỗi: Biến môi trường GOOGLE_CREDENTIALS chưa được cấu hình hoặc sai định dạng."
        
        creds = service_account.Credentials.from_service_account_info(
            GOOGLE_API_CREDENTIALS, scopes=SCOPES)
        service = build('docs', 'v1', credentials=creds)
        
        document_id = url.split('/d/')[1].split('/')[0]
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
        return f"Lỗi khi xử lý Google Docs API: {e}"

def lambda_handler(event, context):
    """Hàm xử lý của Lambda, trigger bởi SQS."""
    fully_formed_prompts = []

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

            aws_content = get_aws_blog_content(aws_blog_url)
            gdoc_content = get_google_doc_content(google_doc_url)
            
            prompt = copy.deepcopy(PROMPT_TEMPLATE)
            prompt["context"]["original_article"] = aws_content
            prompt["context"]["translated_article"] = gdoc_content
            prompt["source_info"] = {
                "aws_blog_url": aws_blog_url,
                "google_doc_url": google_doc_url
            }
            fully_formed_prompts.append(prompt)

        except Exception as e:
            print(f"Lỗi nghiêm trọng trong lambda_handler: {e} với payload: {payload_str}")
            continue

    return {
        'statusCode': 200,
        'headers': { 'Content-Type': 'application/json' },
        'body': json.dumps({
            'message': f'Đã tạo thành công {len(fully_formed_prompts)} prompts từ {len(event.get("Records", []))} messages.',
            'prompts': fully_formed_prompts
        }, ensure_ascii=False, indent=2)
    }
