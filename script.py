import pandas as pd
import os
import time
import zipfile
import sys
import re  # Added for Regex support

def print_progress(current, total, bar_length=40):
    percent = float(current) / total
    arrow = '█' * int(round(percent * bar_length))
    spaces = '-' * (bar_length - len(arrow))
    sys.stdout.write(f"\rProgress: [{arrow}{spaces}] {int(percent * 100)}%")
    sys.stdout.flush()

def clean_magento_widgets(html_content):
    """
    Removes Magento {{widget ...}} tags from the content.
    """
    if not html_content:
        return ""
    # Pattern matches anything starting with {{widget and ending with }}
    # flags=re.DOTALL ensures it works even if the widget spans multiple lines
    pattern = r'\{\{widget.*?\}\}'
    cleaned_content = re.sub(pattern, '', html_content, flags=re.DOTALL)
    # Optional: Clean up extra whitespace/newlines left behind
    return cleaned_content.strip()

def process_blogs(input_file, max_rows=None):
    try:
        df = pd.read_csv(input_file, dtype=str).fillna('')
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return

    mapped_rows = []
    total = len(df)
    
    print(f"🚀 Processing {total} source rows...")

    for idx, row in df.iterrows():
        print_progress(idx + 1, total)

        is_published = 'TRUE' if str(row.get('status', '')).lower() == 'publication' else 'FALSE'
        
        # --- NEW CLEANING STEP ---
        raw_content = row.get('content', '')
        cleaned_body_html = clean_magento_widgets(raw_content)
        # -------------------------

        mapped_row = {
            'Handle': row.get('url_key', ''),
            'Command': 'MERGE',
            'Title': row.get('title', ''),
            'Author': row.get('author_full_name', ''),
            'Body HTML': cleaned_body_html, # Using the cleaned content here
            'Summary HTML': row.get('short_content', ''),
            'Published At': row.get('publish_date', ''),
            'Published': is_published,
            'Image Src': '',
            'Image Alt Text': row.get('featured_image_alt', ''),
            'Blog: Title': 'Betta Blog',
            'Metafield: title_tag': row.get('meta_title', ''),
            'Metafield: description_tag': row.get('meta_description', '')
        }
        mapped_rows.append(mapped_row)

    print("\n✅ Mapping complete. Exporting files...")

    output_df = pd.DataFrame(mapped_rows)
    output_files = []

    if not max_rows:
        filename = 'shopify_blog_import_all.csv'
        output_df.to_csv(filename, index=False, encoding='utf-8-sig')
        output_files.append(filename)
    else:
        for i in range((len(output_df) + max_rows - 1) // max_rows):
            part_df = output_df.iloc[i * max_rows: (i + 1) * max_rows]
            filename = f'shopify_blog_import_part{i + 1}.csv'
            part_df.to_csv(filename, index=False, encoding='utf-8-sig')
            output_files.append(filename)

    zip_name = f'blog_migration_{int(time.time())}.zip'
    with zipfile.ZipFile(zip_name, 'w') as zipf:
        for f in output_files:
            zipf.write(f)
            os.remove(f)

    print(f"✅ Done! Migration files saved in: {zip_name}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python migrate_blog.py <input_file.csv> [max_rows|unlimited]")
        sys.exit(1)

    input_path = sys.argv[1]

    if len(sys.argv) >= 3:
        arg = sys.argv[2].lower()
        max_rows = None if arg == 'unlimited' else int(arg)
    else:
        max_rows = 900

    process_blogs(input_path, max_rows)