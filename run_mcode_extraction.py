#!/usr/bin/env python3
"""
Script file for extracting mCODE data from clinical notes using cTAKES & other heuristics.

Usage:
    python3 run_mcode_extraction.py [--config config.yaml] [--input input_file.txt]
    (Arguments can be changed via config.yaml as well)
"""

# Standard libraries
import argparse
import subprocess
import sys
from pathlib import Path

# Third-party libraries
import yaml

# Local modules
from src.parsers.xmi_parser import parse_xmi_file
from src.outputs.csv_generator import generate_mcode_csv

def load_config(config_path: str = 'config.yaml') -> dict:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Load UMLS key from specified file
    if 'umls_api_key_file' in config['ctakes']:
        key_file = Path(config['ctakes']['umls_api_key_file'])
        if key_file.exists():
            config['ctakes']['umls_api_key'] = key_file.read_text().strip()
        else:
            print(f"Warning: UMLS key file not found: {key_file}", file=sys.stderr)
            print(f"Add your UMLS API key to .umls_key and try again", file=sys.stderr)
            sys.exit(1)
    
    return config


def run_ctakes_pipeline(config: dict, input_path: Path) -> list:
    """
    Run cTAKES pipeline on input file or directory.
    
    Args:
        config: Configuration dictionary
        input_path: Path to input clinical note file or directory
    
    Returns:
        List of paths to generated .xmi files
    """
    ctakes_home = Path(config['ctakes']['installation_path'])
    umls_key = config['ctakes']['umls_api_key']
    pipeline = config['pipeline']['name']
    xmi_output_dir = Path(config['paths']['xmi_output_dir']).resolve()
    
    # Create output directory
    xmi_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Input path can be a single file or a directory
    if input_path.is_dir():
        input_dir = input_path.resolve()
        # Get list of .txt files for later XMI matching
        input_files = sorted(input_dir.glob('*.txt'))
    else:
        input_dir = input_path.parent.resolve()
        input_files = [input_path]
    
    # Construct cTAKES command with absolute paths
    runpiper_script = ctakes_home / 'bin' / 'runPiperFile.sh'
    
    cmd = [
        str(runpiper_script),
        '-p', pipeline,  # Just the pipeline name, no path or extension
        '-i', str(input_dir),
        '-o', str(xmi_output_dir),
        '--xmiOut', str(xmi_output_dir),
        '--key', umls_key
    ]
    
    print(f"Running cTAKES pipeline: {pipeline}")
    print(f"Input: {input_dir}")
    print(f"Output: {xmi_output_dir}")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        print("cTAKES pipeline completed successfully")
        if result.stdout:
            print("=== STDOUT ===")
            print(result.stdout)
        if result.stderr:
            print("=== STDERR ===", file=sys.stderr)
            print(result.stderr, file=sys.stderr)
    except subprocess.CalledProcessError as err:
        print(f"Error running cTAKES: {err.stderr}", file=sys.stderr)
        raise
    
    # Collect the .xmi files
    xmi_files = []
    for i in input_files:
        xmi_file = xmi_output_dir / f"{i.name}.xmi"
        if xmi_file.exists():
            xmi_files.append(xmi_file)
        else:
            print(f"Warning: Expected .xmi output not found: {xmi_file}", file=sys.stderr)
    
    if not xmi_files:
        raise FileNotFoundError(f"No .xmi files generated in {xmi_output_dir}")
    
    return xmi_files


def process_clinical_note(xmi_file: Path, input_filename: str, config: dict):
    """
    Process a single XMI file and extract into .csv file with mCODE entries.
    """
    
    print(f"\n{'='*60}")
    print(f"Processing: {input_filename}")
    print(f"{'='*60}\n")
    
    # Get TypeSystem.xml path from cTAKES installation
    ctakes_home = Path(config['ctakes']['installation_path'])
    typesystem_path = ctakes_home / 'resources/org/apache/ctakes/typesystem/types/TypeSystem.xml'
    
    entities, relations, temporal_data, sentences, text = parse_xmi_file(str(xmi_file), str(typesystem_path))
    
    # Print extraction summary
    print("\nExtraction Summary:")
    print(f"  Diseases: {len(entities['diseases'])}")
    print(f"  Medications: {len(entities['medications'])}")
    print(f"  Procedures: {len(entities['procedures'])}")
    print(f"  Anatomical Sites: {len(entities['anatomical_sites'])}")
    print(f"  LOCATION_OF Relations: {len(relations['location_of'])}")
    print(f"  Time Mentions: {len(temporal_data.get('time_mentions', []))}")
    print(f"  Events: {len(temporal_data.get('events', []))}")
    print(f"  Temporal Relations: {len(temporal_data.get('temporal_relations', []))}")
    print(f"  Sentences: {len(sentences)}")
    
    # Count entities with CUIs and negated entities
    cui_count = sum(
        1 for etype in entities.values()
            for e in etype if e.get('primary_cui')
    )
    negated_count = sum(
        1 for etype in entities.values()
            for e in etype if e.get('negated')
    )
    print(f"  Entities with CUIs: {cui_count}")
    print(f"  Negated entities: {negated_count}")
    
    # Generate mCODE CSV
    csv_output_dir = Path(config['paths']['csv_output_dir'])
    csv_output_dir.mkdir(parents=True, exist_ok=True)
    csv_output_path = csv_output_dir / f"{Path(input_filename).stem}_mcode.csv"
    
    print(f"\nGenerating mCODE CSV: {csv_output_path}")
    generate_mcode_csv(entities, relations, temporal_data, sentences, str(csv_output_path), input_filename, text, config)
    
    print(f"\nProcessing complete!")
    print(f"  XMI output: {xmi_file}")
    print(f"  CSV output: {csv_output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Extract mCODE data from clinical notes using cTAKES'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--input',
        help='Path to input clinical note file or directory (overrides config)'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Determine input path (file or directory)
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            print(f"Error: Input not found: {input_path}", file=sys.stderr)
            sys.exit(1)
    else:
        # Use default from config
        input_path = Path(config['paths']['input_dir'])
        if not input_path.exists():
            print(f"Error: Input directory not found: {input_path}", file=sys.stderr)
            sys.exit(1)
    
    # Run cTAKES pipeline (batch processing if directory)
    try:
        print(f"\n{'='*60}")
        if input_path.is_dir():
            print(f"Running cTAKES batch processing on directory: {input_path}")
        else:
            print(f"Running cTAKES on file: {input_path.name}")
        print(f"{'='*60}\n")
        
        xmi_files = run_ctakes_pipeline(config, input_path)
        
        print(f"\ncTAKES generated {len(xmi_files)} XMI file(s)")
        
        # Process each XMI file
        for xmi_file in xmi_files:
            # Extract original filename from XMI filename (remove .xmi extension)
            input_filename = xmi_file.name[:-4]  # Remove .xmi
            process_clinical_note(xmi_file, input_filename, config)
        
        print(f"\n{'='*60}")
        print(f"Batch processing complete! Processed {len(xmi_files)} file(s)")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()