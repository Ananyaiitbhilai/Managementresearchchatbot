import json
import re
import os
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import psutil
from pathlib import Path

# Configure logging similar to XML processor
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('management_filter.log'),
        logging.StreamHandler()
    ]
)

# Regex patterns (same as original)
target_journals = {
    r"\bOrganization\s*Science\b",
    r"\bManagement\s*Science\b",
    r"\bStrategy\s*Science\b",
    r"\bStrategic\s*Management\s*Journal\b",
    r"\bAcademy\s*of\s*Management\s*Journal\b",
    r"\bAdministrative\s*Science\s*Quarterly\b",
    r"\bJournal\s*of\s*Management\b"
}
patterns = [re.compile(journal, re.IGNORECASE) for journal in target_journals]

def is_management_paper(record):
    """Filter function remains unchanged from original"""
    try:
        titles = record['static_data']['summary']['titles']['title']
        for title in titles:
            if title.get('@type') == 'source' and title.get('_text'):
                text = re.sub(r'\s+', '', title['_text'].lower())
                if any(pattern.search(text) for pattern in patterns):
                    return True
    except KeyError:
        pass
    return False

def get_cpu_info():
    """Reuse CPU monitoring from XML processor"""
    cpu = psutil.cpu_percent(interval=1, percpu=True)
    return {
        'total_cores': psutil.cpu_count(),
        'physical_cores': psutil.cpu_count(logical=False),
        'cpu_usage_per_core': cpu,
        'avg_cpu_usage': sum(cpu)/len(cpu),
        'available_memory': f"{psutil.virtual_memory().available/(1024**3):.2f}GB"
    }

def process_json_file(args):
    """Process individual JSON files with error handling"""
    json_path, input_dir, output_dir = args
    start_time = time.time()
    
    try:
        # Create output path preserving directory structure
        relative_path = Path(json_path).relative_to(input_dir)
        output_path = Path(output_dir) / relative_path.with_name(
            f"{relative_path.stem}_management{relative_path.suffix}"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load and process data
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        management_papers = [p for p in data['records'] if is_management_paper(p)]
        
        # Save filtered results
        with open(output_path, 'w') as f:
            json.dump({
                "count": len(management_papers),
                "papers": management_papers
            }, f, indent=2)
        
        return {
            'file': str(json_path),
            'count': len(management_papers),
            'time': time.time() - start_time,
            'success': True
        }
        
    except Exception as e:
        logging.error(f"Failed {json_path}: {str(e)}")
        return {
            'file': str(json_path),
            'count': 0,
            'time': time.time() - start_time,
            'success': False,
            'error': str(e)
        }

def filter_management_papers(input_dir, output_dir, max_workers=None):
    """Main processing function with system monitoring"""
    json_files = list(Path(input_dir).rglob('*.json'))
    if not json_files:
        logging.error(f"No JSON files found in {input_dir}")
        return
    
    # Create output directory (similar to XML processor)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Log initial system status
    cpu_info = get_cpu_info()
    logging.info("\nInitial System Status:")
    logging.info(f"Cores: {cpu_info['total_cores']} Physical: {cpu_info['physical_cores']}")
    logging.info(f"Memory Available: {cpu_info['available_memory']}")
    
    total_papers = 0
    success_count = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        args_list = [(str(f), input_dir, output_dir) for f in json_files]
        
        with tqdm(total=len(json_files), desc="Filtering papers") as pbar:
            for result in executor.map(process_json_file, args_list):
                pbar.update(1)
                if result['success']:
                    success_count += 1
                    total_papers += result['count']
                    current_cpu = get_cpu_info()
                    pbar.set_postfix({
                        'total': total_papers,
                        'current_file': Path(result['file']).name,
                        'cpu%': f"{current_cpu['avg_cpu_usage']:.1f}",
                        'mem': current_cpu['available_memory']
                    })
    
    # Final report
    logging.info(f"\nProcessing Complete:")
    logging.info(f"Files Processed: {len(json_files)}")
    logging.info(f"Successful: {success_count} Failed: {len(json_files)-success_count}")
    logging.info(f"Total Management Papers Found: {total_papers}")

if __name__ == '__main__':
    input_directory = 'json_output'  # Set your input path
    output_directory = 'management_full_papers'  # Output directory
    
    filter_management_papers(
        input_directory,
        output_directory,
        max_workers=os.cpu_count()
    )
