import json
import boto3
import os
import math
import urllib.parse

s3_client = boto3.client('s3')

def dynamic_split_blogs(original_text, translated_text, char_limit_en=3500):
    """Thuật toán cắt chuỗi tỷ lệ động (Dynamic Ratio)"""
    original_len = len(original_text)
    translated_len = len(translated_text)
    chunks = []
    
    if original_len == 0 or translated_len == 0: return []

    en_index = 0
    vi_index = 0
    
    while en_index < original_len:
        en_remaining = original_len - en_index
        vi_remaining = translated_len - vi_index
        if en_remaining == 0 or vi_remaining == 0: break
            
        dynamic_ratio = vi_remaining / en_remaining
        en_chunk_len = min(char_limit_en, en_remaining)
        en_end_index = en_index + en_chunk_len
        
        if en_end_index >= original_len:
            vi_end_index = translated_len
        else:
            vi_chunk_len = math.floor(en_chunk_len * dynamic_ratio)
            vi_end_index = min(vi_index + vi_chunk_len, translated_len)
            
        chunks.append({
            "original_text": original_text[en_index:en_end_index],
            "translated_text": translated_text[vi_index:vi_end_index]
        })
        en_index = en_end_index
        vi_index = vi_end_index
        
    return chunks

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    try:
        # 1. Parse Event từ EventBridge (S3 Object Created)
        detail = event.get('detail', {})
        bucket_name = detail.get('bucket', {}).get('name')
        translated_key = detail.get('object', {}).get('key')
        
        if not bucket_name or not translated_key:
            # Fallback nếu test thủ công bằng JSON console
            bucket_name = event.get('bucket')
            translated_key = event.get('key')
            
        # Decode URL (đề phòng tên file có dấu cách hoặc ký tự đặc biệt)
        translated_key = urllib.parse.unquote_plus(translated_key)
        
        # 2. Suy ra đường dẫn file gốc (Logic: translated/abc.md -> original/abc.md)
        file_name = os.path.basename(translated_key)
        original_key = f"original/{file_name}"
        article_id = file_name.replace('.md', '')

        print(f"Processing: {translated_key} & {original_key}")

        # 3. Tải nội dung từ S3
        trans_obj = s3_client.get_object(Bucket=bucket_name, Key=translated_key)
        orig_obj = s3_client.get_object(Bucket=bucket_name, Key=original_key)
        
        translated_text = trans_obj['Body'].read().decode('utf-8')
        original_text = orig_obj['Body'].read().decode('utf-8')

        # 4. Cắt nhỏ (Chunking)
        chunks = dynamic_split_blogs(original_text, translated_text, char_limit_en=3500)
        print(f"Split into {len(chunks)} chunks")
        
        # 5. Output cho Map State
        output_chunks = []
        total_chunks = len(chunks)
        
        for i, chunk in enumerate(chunks):
            output_chunks.append({
                "chunk_index": i + 1,
                "total_chunks": total_chunks,
                "article_id": article_id,
                "original_text": chunk['original_text'],
                "translated_text": chunk['translated_text']
            })

        return output_chunks

    except Exception as e:
        print(f"Error: {str(e)}")
        raise e