import json
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, List
import time
import psutil
from tqdm import tqdm

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('metadata_processing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_cpu_info():
    """System resource monitoring from previous implementation"""
    cpu = psutil.cpu_percent(interval=1, percpu=True)
    return {
        'total_cores': psutil.cpu_count(),
        'physical_cores': psutil.cpu_count(logical=False),
        'cpu_usage_per_core': cpu,
        'avg_cpu_usage': sum(cpu)/len(cpu),
        'available_memory': f"{psutil.virtual_memory().available/(1024**3):.2f}GB"
    }

def safe_get(obj: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Safely get a value from a dictionary, returning default if key doesn't exist"""
    try:
        return obj.get(key, default)
    except (AttributeError, TypeError):
        return default

def extract_paper_metadata(paper: Dict[str, Any]) -> Dict[str, Any]:
    """Extract structured metadata from a paper record"""
    static_data = safe_get(paper, 'static_data', {})
    dynamic_data = safe_get(paper, 'dynamic_data', {})
    
    # Get basic metadata
    metadata = {
        'wos_uid': safe_get(paper, 'UID', {}).get('_text', ''),
        'doi': '',
        'database_edition': ''
    }
    
    # Extract DOI and database edition
    if 'cluster_related' in dynamic_data:
        identifiers = safe_get(dynamic_data['cluster_related'], 'identifiers', {}).get('identifier', [])
        for id_obj in identifiers:
            if id_obj.get('@type') == 'doi':
                metadata['doi'] = id_obj.get('@value', '')
    
    if 'EWUID' in safe_get(static_data, 'summary', {}):
        edition = safe_get(static_data['summary']['EWUID'], 'edition', {})
        if isinstance(edition, dict):
            metadata['database_edition'] = edition.get('@value', '')
        elif isinstance(edition, list):
            metadata['database_edition'] = [ed.get('@value', '') for ed in edition]

    # Extract content
    content = {
        'title': '',
        'journal': '',
        'abstract': '',
        'keywords': {
            'author': [],
            'system': []
        }
    }
    
    # Get title and journal
    titles = safe_get(static_data, 'summary', {}).get('titles', {}).get('title', [])
    for title in titles:
        if title.get('@type') == 'item':
            content['title'] = title.get('_text', '')
        elif title.get('@type') == 'source':
            content['journal'] = title.get('_text', '')
    
    # Get abstract
    abstracts = safe_get(static_data, 'fullrecord_metadata', {}).get('abstracts', {}).get('abstract', {})
    if abstracts:
        abstract_text = safe_get(abstracts, 'abstract_text', {}).get('p', [])
        if isinstance(abstract_text, list):
            content['abstract'] = ' '.join([p.get('_text', '') for p in abstract_text])
        elif isinstance(abstract_text, dict):
            content['abstract'] = abstract_text.get('_text', '')
    
    # Get keywords
    keywords = safe_get(static_data, 'fullrecord_metadata', {}).get('keywords', {}).get('keyword', [])
    if keywords:
        content['keywords']['author'] = [kw.get('_text', '') for kw in keywords]
    
    keywords_plus = safe_get(static_data, 'item', {}).get('keywords_plus', {}).get('keyword', [])
    if keywords_plus:
        content['keywords']['system'] = [kw.get('_text', '') for kw in keywords_plus]

    # Extract authors
    authors = []
    names = safe_get(static_data, 'summary', {}).get('names', {}).get('name', [])
    addresses = safe_get(static_data, 'fullrecord_metadata', {}).get('addresses', {}).get('address_name', [])
    
    for author in names:
        author_data = {
            'name': safe_get(author, 'display_name', {}).get('_text', ''),
            'orcid': author.get('@orcid_id_tr', ''),
            'email': safe_get(author, 'email_addr', {}).get('_text', ''),
            'affiliations': [],
            'ids': []
        }
        
        # Get affiliations
        addr_no = author.get('@addr_no')
        if addr_no:
            for address in addresses:
                if safe_get(address, 'address_spec', {}).get('@addr_no') == addr_no:
                    addr_spec = address['address_spec']
                    affiliation = {
                        'institution': '',
                        'department': '',
                        'country': safe_get(addr_spec, 'country', {}).get('_text', ''),
                        'city': safe_get(addr_spec, 'city', {}).get('_text', ''),
                        'state': safe_get(addr_spec, 'state', {}).get('_text', '')
                    }
                    
                    # Get institution name
                    orgs = safe_get(addr_spec, 'organizations', {}).get('organization', [])
                    if isinstance(orgs, list):
                        for org in orgs:
                            if isinstance(org, dict) and org.get('@pref') == 'Y':
                                affiliation['institution'] = org.get('_text', '')
                                break
                    
                    # Get department
                    subunits = safe_get(addr_spec, 'suborganizations', {}).get('suborganization', [])
                    if isinstance(subunits, list) and len(subunits) > 0:
                        affiliation['department'] = subunits[0].get('_text', '')
                    elif isinstance(subunits, dict):
                        affiliation['department'] = subunits.get('_text', '')
                        
                    author_data['affiliations'].append(affiliation)
        
        # Get researcher IDs
        if '@r_id' in author:
            author_data['ids'].append(author['@r_id'])
            
        authors.append(author_data)

    # Extract publication details
    pub_info = safe_get(static_data, 'summary', {}).get('pub_info', {})
    publication = {
        'year': pub_info.get('@pubyear', ''),
        'volume_issue': f"{pub_info.get('@vol', '')}({pub_info.get('@issue', '')})",
        'pages': f"{pub_info.get('page', {}).get('@begin', '')}-{pub_info.get('page', {}).get('@end', '')}"
    }

    # Extract categories
    categories = []
    category_info = safe_get(static_data, 'fullrecord_metadata', {}).get('category_info', {})
    subjects = safe_get(category_info, 'subjects', {}).get('subject', [])
    if isinstance(subjects, list):
        categories = [subj.get('_text', '') for subj in subjects if subj.get('@ascatype') == 'traditional']

    # Extract references
    references = []
    refs = safe_get(static_data, 'fullrecord_metadata', {}).get('references', {}).get('reference', [])
    for ref in refs:
        reference = {
            'authors': safe_get(ref, 'citedAuthor', {}).get('_text', ''),
            'title': safe_get(ref, 'citedTitle', {}).get('_text', ''),
            'year': safe_get(ref, 'year', {}).get('_text', ''),
            'source': safe_get(ref, 'citedWork', {}).get('_text', ''),
            'doi': safe_get(ref, 'doi', {}).get('_text', '')
        }
        references.append(reference)

    return {
        'metadata': metadata,
        'content': content,
        'authors': authors,
        'publication': publication,
        'categories': categories,
        'references': references
    }

def process_single_file(args):
    """Process individual JSON files with error handling"""
    input_path, input_dir, output_dir = args
    start_time = time.time()
    
    try:
        # Create output path preserving directory structure
        relative_path = Path(input_path).relative_to(input_dir)
        output_path = Path(output_dir) / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Original processing logic
        logger.info(f"Processing {input_path}")
        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        processed_data = [extract_paper_metadata(paper) for paper in safe_get(raw_data, "papers", default=[])]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(processed_data, f, indent=2, ensure_ascii=False)
        
        return {
            'file': str(input_path),
            'processed': len(processed_data),
            'time': time.time() - start_time,
            'success': True
        }
        
    except Exception as e:
        logger.error(f"Failed {input_path}: {str(e)}")
        return {
            'file': str(input_path),
            'processed': 0,
            'time': time.time() - start_time,
            'success': False,
            'error': str(e)
        }

def process_json_directory(input_dir: str, output_dir: str, max_workers: int = None):
    """Process all JSON files in directory with parallel execution"""
    json_files = list(Path(input_dir).rglob('*.json'))
    if not json_files:
        logger.error(f"No JSON files found in {input_dir}")
        return
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Log system status
    cpu_info = get_cpu_info()
    logger.info("\nSystem Resources:")
    logger.info(f"Cores: {cpu_info['total_cores']} Physical: {cpu_info['physical_cores']}")
    logger.info(f"Memory Available: {cpu_info['available_memory']}")
    
    total_processed = 0
    success_count = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        args_list = [(str(f), input_dir, output_dir) for f in json_files]
        
        with tqdm(total=len(json_files), desc="Processing files") as pbar:
            for result in executor.map(process_single_file, args_list):
                pbar.update(1)
                if result['success']:
                    success_count += 1
                    total_processed += result['processed']
                    current_cpu = get_cpu_info()
                    pbar.set_postfix({
                        'total_papers': total_processed,
                        'current_file': Path(result['file']).name,
                        'cpu%': f"{current_cpu['avg_cpu_usage']:.1f}",
                        'mem': current_cpu['available_memory']
                    })
    
    # Final report
    logger.info(f"\nProcessing Complete:")
    logger.info(f"Files Processed: {len(json_files)}")
    logger.info(f"Successful: {success_count} Failed: {len(json_files)-success_count}")
    logger.info(f"Total Papers Processed: {total_processed}")

if __name__ == "__main__":
    # Configure paths
    input_directory = "management_full_papers"  # Set your input path
    output_directory = "management_full_papers_selected_attributes"  # Output dir
    
    process_json_directory(
        input_directory,
        output_directory,
        max_workers=os.cpu_count()
    )
