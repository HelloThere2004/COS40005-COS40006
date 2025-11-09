import json
import math

# Giới hạn ký tự cho 1 chunk (giống JS của bạn)
CHAR_LIMIT_EN = 3500

def lambda_handler(event, context):
    """
    Input: event (chính là 1 item từ extracted_contents)
    {
        "original_article_content": "...",
        "translated_article_content": "...",
        "source_info": { ... }
    }
    """
    original = event.get('original_article_content', '')
    translated = event.get('translated_article_content', '')
    source_info = event.get('source_info', {}) # Giữ lại thông tin nguồn

    original_length = len(original)
    translated_length = len(translated)

    if original_length == 0 or translated_length == 0:
        print("Lỗi: Nội dung gốc hoặc dịch bị rỗng.")
        return [] # Trả về mảng rỗng

    # Tính toán tỷ lệ (giống hệt JS)
    ratio = translated_length / original_length
    num_chunks = math.ceil(original_length / CHAR_LIMIT_EN)
    chunks_list = []

    print(f"Đang chia thành {num_chunks} phần...")

    for i in range(num_chunks):
        original_start_index = i * CHAR_LIMIT_EN
        original_end_index = min((i + 1) * CHAR_LIMIT_EN, original_length)
        original_chunk = original[original_start_index:original_end_index]
        
        translated_start_index = math.floor(original_start_index * ratio)
        
        # Nếu là chunk cuối cùng, lấy hết phần còn lại
        if i == num_chunks - 1:
            translated_end_index = translated_length
        else:
            translated_end_index = math.floor(original_end_index * ratio)

        translated_chunk = translated[translated_start_index:translated_end_index]

        chunks_list.append({
            "en_chunk": original_chunk,
            "vi_chunk": translated_chunk,
            "chunkNumber": i + 1,
            "totalChunks": num_chunks,
            "source_info": source_info # Thêm thông tin nguồn vào mỗi chunk
        })

    return chunks_list