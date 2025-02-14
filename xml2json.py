import xml.etree.ElementTree as ET
import json
from collections import defaultdict
import gzip
import os
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import logging
import time
from pathlib import Path
import psutil

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('xml_to_json_step1.log'),
        logging.StreamHandler()
    ]
)

def elem_to_dict(element):
    """Recursively convert XML element to nested dictionary"""
    d = defaultdict(list)
    if element.text and element.text.strip():
        d['_text'] = element.text.strip()
    d.update({'@'+k: v for k, v in element.attrib.items()})
    
    for child in element:
        child_data = elem_to_dict(child)
        if child.tag in d:
            if isinstance(d[child.tag], list):
                d[child.tag].append(child_data)
            else:
                d[child.tag] = [d[child.tag], child_data]
        else:
            d[child.tag] = child_data
            
    return dict((k, v if not isinstance(v, defaultdict) else dict(v)) 
                for k, v in d.items())

def decompress_xml(gz_file_path, xml_file_path):
    """Decompress .xml.gz file to .xml"""
    try:
        with gzip.open(gz_file_path, 'rb') as gz_file:
            xml_content = gz_file.read()
        with open(xml_file_path, 'wb') as xml_file:
            xml_file.write(xml_content)
    except Exception as e:
        logging.error(f"Error decompressing {gz_file_path}: {str(e)}")
        raise

def process_single_file(args):
    """Process a single XML file and convert to JSON"""
    input_file, output_dir = args
    start_time = time.time()
    xml_path = None
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Handle .xml.gz files
        if input_file.endswith('.xml.gz'):
            xml_path = input_file[:-3]  # Remove .gz extension
            logging.info(f"Decompressing {input_file}")
            decompress_xml(input_file, xml_path)
        else:
            xml_path = input_file
            
        if not os.path.exists(xml_path):
            raise FileNotFoundError(f"XML file not found: {xml_path}")
        
        # Prepare output JSON path
        json_file = os.path.join(
            output_dir,
            os.path.basename(xml_path).replace('.xml', '.json')
        )
        
        # Parse XML and convert to JSON
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Remove namespace for easier processing
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]
        
        records = []
        for rec in root.findall('.//REC'):
            record_data = elem_to_dict(rec)
            records.append(record_data)
        
        # Write JSON output
        with open(json_file, 'w') as f:
            json.dump({
                "records": records,
                "total_records": len(records),
                "source": input_file
            }, f, indent=2)
        
        processing_time = time.time() - start_time
        return {
            'file': input_file,
            'records': len(records),
            'time': processing_time,
            'success': True
        }
            
    except Exception as e:
        logging.error(f"Error processing {input_file}: {str(e)}")
        return {
            'file': input_file,
            'records': 0,
            'time': time.time() - start_time,
            'success': False,
            'error': str(e)
        }
        
    finally:
        # Clean up decompressed XML if it was created from a compressed file
        if xml_path and input_file.endswith('.xml.gz') and os.path.exists(xml_path):
            try:
                os.remove(xml_path)
                logging.debug(f"Cleaned up temporary file: {xml_path}")
            except Exception as e:
                logging.warning(f"Failed to clean up temporary file {xml_path}: {str(e)}")

def get_cpu_info():
    """Get CPU usage and core information"""
    cpu = psutil.cpu_percent(interval=1, percpu=True)
    return {
        'total_cores': psutil.cpu_count(),
        'physical_cores': psutil.cpu_count(logical=False),
        'cpu_usage_per_core': cpu,
        'avg_cpu_usage': sum(cpu) / len(cpu),
        'available_memory': f"{psutil.virtual_memory().available / (1024 * 1024 * 1024):.2f}GB"
    }

def convert_xml_files(input_dir, output_dir, max_workers=None):
    """Convert all XML files in input directory and its subdirectories"""
    # Find all XML and XML.GZ files
    xml_files = []
    for ext in ['*.xml', '*.xml.gz']:
        xml_files.extend(Path(input_dir).rglob(ext))
    xml_files = [str(f) for f in xml_files]
    
    if not xml_files:
        logging.error(f"No XML files found in {input_dir}")
        return
    
    # Log initial system information
    cpu_info = get_cpu_info()
    logging.info(f"\nSystem Information:")
    logging.info(f"Total CPU Cores: {cpu_info['total_cores']}")
    logging.info(f"Physical CPU Cores: {cpu_info['physical_cores']}")
    logging.info(f"Initial CPU Usage per Core: {cpu_info['cpu_usage_per_core']}")
    logging.info(f"Average CPU Usage: {cpu_info['avg_cpu_usage']:.2f}%")
    logging.info(f"Available Memory: {cpu_info['available_memory']}")
    
    logging.info(f"\nFound {len(xml_files)} XML files to process")
    
    # Process files in parallel
    total_records = 0
    successful_files = 0
    failed_files = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Create arguments list for each file
        args_list = [(f, output_dir) for f in xml_files]
        
        # Process files with progress bar
        results = []
        with tqdm(total=len(xml_files), desc="Converting files") as pbar:
            for result in executor.map(process_single_file, args_list):
                pbar.update(1)
                results.append(result)
                
                # Update progress bar description with current file info
                if result['success']:
                    successful_files += 1
                    total_records += result['records']
                    # Get current CPU info
                    current_cpu = get_cpu_info()
                    pbar.set_postfix({
                        'records': total_records,
                        'current_file': os.path.basename(result['file']),
                        'time': f"{result['time']:.2f}s",
                        'avg_cpu': f"{current_cpu['avg_cpu_usage']:.1f}%",
                        'mem_free': current_cpu['available_memory']
                    })
                else:
                    failed_files += 1
    
    # Log final statistics
    logging.info(f"\nConversion completed:")
    logging.info(f"Total files processed: {len(xml_files)}")
    logging.info(f"Successful conversions: {successful_files}")
    logging.info(f"Failed conversions: {failed_files}")
    logging.info(f"Total records converted: {total_records}")
    
    # Log final system state
    final_cpu = get_cpu_info()
    logging.info(f"\nFinal System State:")
    logging.info(f"Final CPU Usage per Core: {final_cpu['cpu_usage_per_core']}")
    logging.info(f"Final Average CPU Usage: {final_cpu['avg_cpu_usage']:.2f}%")
    logging.info(f"Final Available Memory: {final_cpu['available_memory']}")
    
    # Log failed files if any
    if failed_files > 0:
        failed_list = [r['file'] for r in results if not r['success']]
        logging.error("Failed files:")
        for f in failed_list:
            logging.error(f"- {f}")

if __name__ == '__main__':
    input_directory = ''  # Replace with your input directory
    output_directory = 'json_output'         # Replace with your output directory
    
    convert_xml_files(
        input_directory,
        output_directory,
        max_workers=os.cpu_count()  # Use all available CPU cores
    )