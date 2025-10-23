# validate.py

import sys
import argparse
from pathlib import Path
from src.orchestrator import ValidationOrchestrator

def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(
        description='OFSAA FCCM Data Validation Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate single file (auto-detect table)
  python validate.py data/input/AccountAddress_20251015.dat
  
  # Validate with specific table
  python validate.py data/input/Account.dat --table DIM_ACCOUNT
  
  # Validate all files in directory
  python validate.py data/input --batch
  
  # Specify custom templates directory
  python validate.py data/input/file.dat --templates config/my_templates
        """
    )
    
    parser.add_argument('input', help='Input file or directory')
    parser.add_argument('--table', '-t', help='Table name (auto-detected if not provided)')
    parser.add_argument('--templates', default='config/templates', 
                       help='Templates directory (default: config/templates)')
    parser.add_argument('--batch', '-b', action='store_true', 
                       help='Batch mode: validate all files in directory')
    
    args = parser.parse_args()
    
    try:
        # Initialize orchestrator
        orchestrator = ValidationOrchestrator(args.templates)
        
        input_path = Path(args.input)
        
        if args.batch or input_path.is_dir():
            # Batch mode
            validate_batch(orchestrator, input_path)
        else:
            # Single file mode
            if not input_path.exists():
                print(f"Error: File not found: {args.input}")
                return 1
            
            result = orchestrator.validate_file(str(input_path), args.table)
            
            if result['success']:
                print(f"\n✓ Validation completed successfully!")
                print(f"\nOutput directory: data/output/{input_path.stem}/")
                
                if result['summary']['data_quality_score'] >= 95:
                    return 0
                else:
                    return 1
            else:
                print(f"\n✗ Validation failed")
                return 1
        
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

def validate_batch(orchestrator, input_dir):
    """Validate all files in directory"""
    
    # Find all data files
    files = []
    for ext in ['*.dat', '*.txt', '*.csv']:
        files.extend(input_dir.glob(ext))
    
    if not files:
        print(f"No data files found in {input_dir}")
        return
    
    print(f"\n{'='*80}")
    print(f"BATCH VALIDATION MODE")
    print(f"Found {len(files)} file(s) to validate")
    print(f"{'='*80}\n")
    
    results = []
    
    for file_path in files:
        try:
            result = orchestrator.validate_file(str(file_path))
            results.append({
                'file': file_path.name,
                'status': 'success' if result['success'] else 'failed',
                'summary': result.get('summary', {})
            })
        except Exception as e:
            print(f"\n✗ Failed to validate {file_path.name}: {str(e)}\n")
            results.append({
                'file': file_path.name,
                'status': 'error',
                'error': str(e)
            })
    
    # Print batch summary
    print(f"\n{'='*80}")
    print("BATCH VALIDATION SUMMARY")
    print(f"{'='*80}")
    
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] in ['failed', 'error']]
    
    print(f"Total Files:   {len(results)}")
    print(f"Successful:    {len(successful)}")
    print(f"Failed:        {len(failed)}")
    print(f"{'='*80}\n")
    
    for result in results:
        if result['status'] == 'success':
            summary = result['summary']
            quality = summary.get('data_quality_score', 0)
            icon = "✓" if quality >= 95 else "⚠"
            print(f"{icon} {result['file']}: Quality {quality}% "
                  f"({summary.get('valid_records', 0)}/{summary.get('total_records', 0)} valid)")
        else:
            print(f"✗ {result['file']}: {result.get('error', 'Validation failed')}")

if __name__ == "__main__":
    sys.exit(main())