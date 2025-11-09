import json
import boto3

# Khởi tạo client bên ngoài handler để tái sử dụng
bedrock_client = boto3.client('bedrock-runtime')

# Model bạn muốn dùng, ví dụ Claude 3 Sonnet
MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0" 

# Định nghĩa hàm tạo prompt (giống hệt JS của bạn)
def get_full_prompt(originalContent, translatedContent, chunkNumber, totalChunks):
    progress_note = f"Bạn đang xử lý **Phần {chunkNumber} / {totalChunks}**.\n"
    if chunkNumber == totalChunks:
        progress_note += "**Lưu ý: Đây là phần CUỐI CÙNG (hoặc duy nhất) của tài liệu. Sau khi hoàn tất, hãy cân nhắc đưa ra nhận xét tổng thể.**"

    # Đây là prompt template, copy từ JS của bạn
    prompt_template = f"""
[LƯU Ý QUAN TRỌNG DÀNH CHO AI]
Đây là một prompt có cấu trúc. Nhiệm vụ của bạn là **THỰC THI** các hướng dẫn bên dưới (như [Role], [Objectives]) để hiệu đính văn bản trong [Context].
**KHÔNG** phân tích, debug, hay chỉnh sửa cấu trúc của chính cái prompt này. Hãy nhập vai và làm theo yêu cầu.

# [Role]
Bạn là kiến trúc sư hệ thống với 20+ năm kinh nghiệm về cloud computing, hiện là chuyên gia tại AWS. Bạn có kinh nghiệm dịch cabin và chuyên hiệu đính các bài blog/kỹ thuật của AWS.

# [Progress]
{progress_note}

# [Personality]
Khó tính, cầu toàn, soi kỹ từng từ/câu. Ưu tiên tính chính xác và tự nhiên trong tiếng Việt. Không bỏ sót lỗi nhỏ.

# [Objectives]
Bạn nhận **[Bài Gốc]** (EN) và **[Bài đã dịch]** (VI) trong **[Context]**. Hãy:
1. **Rà soát tiêu đề thật kỹ** (ý nghĩa, phong cách, thuật ngữ) - (Chỉ thực hiện nếu đây là Phần 1).
2. **Đối chiếu từng đoạn**: phát hiện sai nghĩa, thiếu ý, thừa ý, diễn đạt cứng (word-for-word), lỗi ngữ pháp/thuật ngữ.
3. **Giữ nguyên tên dịch vụ/thuộc tính AWS** (không dịch, đúng chữ hoa/thương, đúng brand: *Amazon S3*, *AWS Lambda*, *EC2*, *VPC*, *Availability Zone*, v.v.).
4. **Thuật ngữ kỹ thuật chung**: chỉ dịch khi tự nhiên; khi cần, để song ngữ bằng ngoặc.
5. **Đề xuất bản sửa** dễ đọc cho người mới, nhưng **không làm sai nội dung kỹ thuật**.
6. **Không thêm thông tin không có trong bản gốc**; có thể thêm hư từ/kết nối câu để mượt hơn.

# [Context]

## [Bài Gốc] (Phần {chunkNumber}/{totalChunks})
{originalContent}

## [Bài đã dịch] (Phần {chunkNumber}/{totalChunks})
{translatedContent}

# [Style Guide]
* **Đối tượng**: người mới học CS hoặc ít kiến thức công nghệ.
* **Giọng văn**: diễn giải, mạch lạc, gần gũi; tránh khẩu ngữ quá mức.
* **Thuật ngữ**: giữ chuẩn ngành; không “Việt hoá” quá đà.
* **Song ngữ khi cần**: *thuật ngữ (term)* ở **lần xuất hiện đầu** mỗi thuật ngữ quan trọng.
  * Ví dụ: *endpoint → điểm cuối (endpoint)*
  * *on-premises → tại chỗ (on-premises)*
* **Giữ nguyên**: code blocks, tên API/SDK/CLI, tham số, JSON keys, tên màn hình Console, tên nút, output logs, câu lệnh shell, đường dẫn, URLs, region codes, dung lượng/đơn vị (GiB vs GB).
* **Số & đơn vị**: không tự đổi (ms ↔ s, $ ↔ VND).
* **Liên kết**: giữ link, dịch anchor text nếu là văn bản thuần.
* **Dấu câu & chính tả**: tiếng Việt chuẩn, nhất quán cách viết hoa tên riêng.

# [Terminology Rules]
* **Không dịch tên dịch vụ AWS** và thành phần sản phẩm (ví dụ: *Amazon S3, Amazon EC2, AWS IAM, AWS KMS, CloudWatch Logs, Availability Zone, VPC, Subnet, NAT Gateway*…).
* **Từ chung nên dịch (kèm EN khi cần)**:
  * *endpoint → điểm cuối (endpoint)*
  * *availability zone → vùng khả dụng (Availability Zone)*
  * *fault tolerance → chịu lỗi (fault tolerance)*
  * *throughput → thông lượng (throughput)*
  * *latency → độ trễ (latency)*
* **Nhất quán thuật ngữ trong toàn bài** (dùng cùng một cách dịch cho cùng một khái niệm).

# [Quy trình thực hiện]
1. **Tiền kiểm**: quét nhanh để lập danh sách thuật ngữ trọng yếu; đánh dấu chỗ có code/CLI/JSON để không sửa sai.
2. **Kiểm tra tiêu đề**: (Chỉ làm ở Phần 1) đúng ý bài, đúng thuật ngữ, tự nhiên; tránh dịch word-for-word.
3. **Đối chiếu từng đoạn**:
   * So meaning (dịch có đủ ý? có sai lệch?)
   * So terminology (chuẩn, nhất quán?)
   * So fluency (tự nhiên, tránh dịch cứng?)
   * So format (giữ code, tham số, link, bảng, bullet?)
4. **Ghi lỗi** theo mẫu [Format] và **gợi ý chỉnh**.
5. **Tóm tắt thay đổi chính** (tùy chọn) để người đọc nắm nhanh.

# [Mức độ lỗi]
* **Critical**: sai nghĩa/thiếu ý ảnh hưởng hiểu nhầm kỹ thuật.
* **Major**: dùng thuật ngữ chưa chuẩn, diễn đạt gây khó hiểu cho người mới.
* **Minor**: ngữ pháp, chính tả, dấu câu, mượt câu.

# [Format] (đầu ra)
**A. Báo cáo lỗi** — liệt kê theo thứ tự xuất hiện:
* **Đoạn [Số đoạn, tên đoạn (nếu có), bắt đầu bằng: “…”]**
  * **Bản dịch hiện tại**: …
  * **Bản gốc (EN)**: …
  * **Gợi ý chỉnh sửa**: …
  * **Mức độ**: Critical/Major/Minor
  * **Lý giải**: vì sao cần sửa (nghĩa/thuật ngữ/độ tự nhiên/định dạng…)

**B. Bảng thuật ngữ (tùy chọn)**
| Thuật ngữ EN      | Cách dùng trong bài             	| Ghi chú     	|
| --- | --- | --- |
| Availability Zone | vùng khả dụng (Availability Zone) | Giữ EN khi cần độ chính xác |

# [Tiêu chí kiểm tra tiêu đề]
* Truyền tải đúng chủ đề/kết quả chính của bài.
* Dùng đúng thuật ngữ ngành; tránh “dịch thẳng” gây gượng.
* Ngắn gọn, dễ hiểu với người mới (≤ 85 ký tự nếu có thể).
* Không dịch tên dịch vụ AWS trong tiêu đề.
"""
    return prompt_template.strip()

def lambda_handler(event, context):
    """
    Input: event (chính là 1 item từ list chunks)
    {
        "en_chunk": "...",
        "vi_chunk": "...",
        "chunkNumber": 1,
        "totalChunks": 3,
        "source_info": { ... }
    }
    """
    en_chunk = event.get('en_chunk')
    vi_chunk = event.get('vi_chunk')
    chunk_num = event.get('chunkNumber')
    total_chunks = event.get('totalChunks')
    
    # 1. Tạo prompt
    prompt = get_full_prompt(en_chunk, vi_chunk, chunk_num, total_chunks)
    
    # 2. Chuẩn bị payload cho Bedrock (Claude 3)
    # Gói prompt trong "Human:"
    claude_prompt = f"\n\nHuman: {prompt}\n\nAssistant:"
    
    messages = [
        {"role": "user", "content": prompt}
    ]

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": messages
    })
    
    print(f"Đang gọi Bedrock cho chunk {chunk_num}/{total_chunks}...")
    
    try:
        # 3. Gọi Bedrock
        response = bedrock_client.invoke_model(
            body=body,
            modelId=MODEL_ID,
            accept='application/json',
            contentType='application/json'
        )
        
        response_body = json.loads(response.get('body').read())
        
        # 4. Lấy kết quả
        result_text = response_body['content'][0]['text']
        
        print(f"Bedrock hoàn tất chunk {chunk_num}/{total_chunks}.")
        
        # Trả về kết quả
        return {
            "chunkNumber": chunk_num,
            "totalChunks": total_chunks,
            "proofread_result": result_text,
            "source_info": event.get('source_info', {})
        }

    except Exception as e:
        print(f"LỖI khi gọi Bedrock: {e}")
        # Trả về lỗi để Step Function có thể bắt
        raise e